import config
import os
import logging

from ckanapi import RemoteCKAN, ValidationError, NotFound


class CKANProcessor(object):
    def __init__(self):
        self.meta = config.DATA_CATALOG_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.api_key = os.environ.get('API_KEY', 'Required parameter is missing')
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.host = RemoteCKAN(self.ckan_host, apikey=self.api_key)

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        if 'dataset' in selector_data:
            for data in selector_data['dataset']:
                # Put the details of the dataset we're going to create into a dict.
                # Using data.get sometimes because some values can remain empty while others should give an error
                keywords = data.get('keyword')
                keywords_string = ""
                if keywords:
                    keywords_string = ', '.join(keywords)
                dict_list = [
                    {"key": "Access Level", "value": data.get('accessLevel')},
                    {"key": "Issued", "value": data.get('issued')},
                    {"key": "Spatial", "value": data.get('spatial')},
                    {"key": "Modified", "value": data.get('modified')},
                    {"key": "Publisher", "value": data.get('publisher').get('name')},
                    {"key": "Keywords", "value": keywords_string},
                    {"key": "Temporal", "value": data.get('temporal')},
                    {"key": "Accrual Periodicity", "value": data.get('accrualPeriodicity')}
                ]
                maintainer = data.get('contactPoint').get('fn')
                data_dict = {
                    "name": data['identifier'],
                    "title": data['title'],
                    "notes": data['rights'],
                    "owner_org": 'dat',
                    "maintainer": maintainer,
                    "extras": dict_list
                }
                # name is used for url and cannot have uppercase or spaces so we have to replace those
                data_dict["name"] = data_dict["name"].replace("/", "_").replace(".", "-").lower()

                # Create list with future resources
                future_resources_list = {}
                for resource in data['distribution']:
                    description = resource.get('description')
                    mediatype = resource.get('mediaType')
                    resource_dict = {
                        "package_id": data_dict["name"],
                        "url": resource['accessURL'],
                        "description": description,
                        "name": resource['title'],
                        "format": resource['format'],
                        "mediaType": mediatype
                    }
                    if resource['title'] not in future_resources_list:
                        future_resources_list[resource['title']] = resource_dict
                    else:
                        logging.error(f"'{resource['title']}' already exists, rename this resource")
                        continue

                current_resources_list = {}
                try:
                    # Create list with current resources
                    cur_package = self.host.action.package_show(id=data_dict["name"])
                    for resource in cur_package['resources']:
                        current_resources_list[resource['name']] = resource
                except NotFound as e:
                    logging.exception(e)

                if current_resources_list:
                    # Create lists to create, update and delete
                    resources_to_create = list(set(future_resources_list).difference(current_resources_list))
                    resources_to_update = list(set(future_resources_list).intersection(current_resources_list))
                    resources_to_delete = list(set(current_resources_list).difference(future_resources_list))

                    logging.info("{} new, {} existing and {} old resources for dataset '{}'".format(
                        len(resources_to_create), len(resources_to_update), len(resources_to_delete), data_dict['name']))

                    # Patch dataset
                    try:
                        logging.info(f"Patching dataset '{data_dict['name']}'")
                        data_dict['id'] = self.host.action.package_show(id=data_dict['name']).get('id')
                        self.host.action.package_patch(**data_dict)
                    except NotFound:
                        logging.info(f"Creating dataset '{data_dict['name']}'")
                        self.host.action.package_create(**data_dict)
                    except Exception as e:
                        logging.error(f'Exception occurred: {e}')

                    # Patch resources
                    for name in resources_to_update:
                        resource = future_resources_list.get(name)
                        resource['id'] = current_resources_list.get(name).get('id')

                        try:
                            logging.info(f"Patching resource '{resource['name']}'")
                            self.host.action.resource_patch(**resource)
                        except NotFound:  # Resource does not exist
                            logging.info(f"Resource '{resource['name']}' does not exist, adding to 'resources_to_create'")
                            resources_to_create.append(resource['name'])

                    # Create resources
                    for name in resources_to_create:
                        resource = future_resources_list.get(name)
                        try:
                            logging.info(f"Creating resource '{resource['name']}'")
                            self.host.action.resource_create(**resource)
                        except ValidationError:  # Resource already exists
                            logging.info(f"Resource '{resource['name']}' already exists, patching resource")
                            resource['id'] = self.host.action.resource_show(id=name).get('id')
                            self.host.action.resource_patch(**resource)

                    # Delete resources
                    for name in resources_to_delete:
                        resource = current_resources_list.get(name)

                        try:
                            logging.info(f"Deleting resource '{resource['name']}'")
                            self.host.action.resource_delete(id=resource['id'])
                        except Exception as e:  # An exception occurred
                            logging.error(f"Exception occurred when deleting resource '{resource['name']}': {e}")
                            pass
        else:
            logging.info("JSON request does not contain a dataset")
