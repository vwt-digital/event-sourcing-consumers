import config
import os
import logging

from ckanapi import RemoteCKAN, ValidationError, NotFound, SearchError


class CKANProcessor(object):
    def __init__(self):
        self.meta = config.DATA_CATALOG_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.api_key = os.environ.get('API_KEY', 'Required parameter is missing')
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.host = RemoteCKAN(self.ckan_host, apikey=self.api_key)

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        if len(selector_data.get('dataset', [])) > 0:
            tag_dict = self.create_tag_dict(selector_data)

            for data in selector_data['dataset']:
                # Put the details of the dataset we're going to create into a dict
                dict_list = [
                    {"key": "Access Level", "value": data.get('accessLevel')},
                    {"key": "Issued", "value": data.get('issued')},
                    {"key": "Spatial", "value": data.get('spatial')},
                    {"key": "Modified", "value": data.get('modified')},
                    {"key": "Publisher", "value": data.get('publisher').get('name')},
                    {"key": "Keywords", "value": ', '.join(data.get('keyword')) if 'keyword' in data else ""},
                    {"key": "Temporal", "value": data.get('temporal')},
                    {"key": "Accrual Periodicity", "value": data.get('accrualPeriodicity')},
                    {"key": "Project ID", "value": data.get('projectId')}
                ]

                data_dict = {
                    "name": data['identifier'],
                    "title": data['title'],
                    "notes": data['rights'],
                    "owner_org": 'dat',
                    "maintainer": data.get('contactPoint').get('fn'),
                    "state": "active",
                    "tags": tag_dict,
                    "extras": dict_list
                }
                # name is used for url and cannot have uppercase or spaces so we have to replace those
                data_dict["name"] = data_dict["name"].replace("/", "_").replace(".", "-").lower()

                # Create list with future resources
                future_resources_list = {}
                for resource in data['distribution']:
                    resource_dict = {
                        "package_id": data_dict["name"],
                        "url": resource['accessURL'],
                        "description": resource.get('description', ''),
                        "name": resource['title'],
                        "format": resource['format'],
                        "mediaType": resource.get('mediaType', '')
                    }
                    if resource['title'] not in future_resources_list:
                        future_resources_list[resource['title']] = resource_dict
                    else:
                        logging.error(f"'{resource['title']}' already exists, rename this resource")
                        continue

                # Create lists to create, update and delete
                current_resources_list = self.patch_dataset(data_dict)

                resources_to_create = list(set(future_resources_list).difference(current_resources_list))
                resources_to_update = list(set(future_resources_list).intersection(current_resources_list))
                resources_to_delete = list(set(current_resources_list).difference(future_resources_list))

                self.process_resources(data_dict=data_dict, to_create=resources_to_create,
                                       to_update=resources_to_update, to_delete=resources_to_delete,
                                       current_list=current_resources_list, future_list=future_resources_list)

        else:
            logging.info("JSON request does not contain a dataset")

    def create_tag_dict(self, catalog):
        tag_dict = []
        for name in ['domain', 'solution']:
            vocabulary = None
            try:  # Check if correct vocabulary tags exist
                vocabulary = self.host.action.vocabulary_show(id=name)
            except NotFound:
                vocabulary = self.host.action.vocabulary_create(name=name)
            except Exception:
                raise

            # Create package's tags list
            if vocabulary and name in catalog:
                if catalog[name] not in self.host.action.tag_list(vocabulary_id=vocabulary['id']):
                    tag = self.host.action.tag_create(name=catalog[name], vocabulary_id=vocabulary['id'])
                    logging.info(f"Created '{name}' tag for '{catalog[name]}'")
                else:
                    tag = self.host.action.tag_show(id=catalog[name], vocabulary_id=vocabulary['id'])

                tag_dict.append(tag)

        return tag_dict

    def patch_dataset(self, data_dict):
        logging.info(f"Patching dataset '{data_dict['name']}'")
        current_resources_list = {}

        try:
            cur_package = self.host.action.package_show(id=data_dict['name'])  # Retrieve package

            for resource in cur_package['resources']:  # Create list with package resources
                current_resources_list[resource['name']] = resource

            data_dict['id'] = cur_package.get('id')  # Set package id for patching
            self.host.action.package_patch(**data_dict)  # Patch package
        except NotFound:
            logging.info(f"Creating dataset '{data_dict['name']}'")
            self.host.action.package_create(**data_dict)  # Create package if not-existing
        except Exception:
            raise

        return current_resources_list

    def process_resources(self, data_dict, to_create, to_update, to_delete, current_list, future_list):
        logging.info("{} new, {} existing and {} old resources for dataset '{}'".format(
            len(to_create), len(to_update), len(to_delete), data_dict['name']))

        # Patch resources
        for name in to_update:
            resource = future_list.get(name)
            resource['id'] = current_list.get(name).get('id')

            try:
                logging.info(f"Patching resource '{resource['name']}'")
                self.host.action.resource_patch(**resource)
            except NotFound:  # Resource does not exist
                logging.info(f"Resource '{resource['name']}' does not exist, adding to 'to_create'")
                to_create.append(resource['name'])
            except SearchError:
                logging.error(f"SearchError occured while patching resource '{resource['name']}'")

        # Create resources
        for name in to_create:
            resource = future_list.get(name)
            try:
                logging.info(f"Creating resource '{resource['name']}'")
                self.host.action.resource_create(**resource)
            except ValidationError:  # Resource already exists
                logging.info(f"Resource '{resource['name']}' already exists, patching resource")
                resource['id'] = self.host.action.resource_show(id=name).get('id')
                self.host.action.resource_patch(**resource)
            except SearchError:
                logging.error(f"SearchError occured while creating resource '{resource['name']}'")

        # Delete resources
        for name in to_delete:
            resource = current_list.get(name)

            try:
                logging.info(f"Deleting resource '{resource['name']}'")
                self.host.action.resource_delete(id=resource['id'])
            except Exception as e:  # An exception occurred
                logging.error(f"Exception occurred while deleting resource '{resource['name']}': {e}")
                pass
