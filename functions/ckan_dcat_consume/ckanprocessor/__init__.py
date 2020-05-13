import config
import os
import logging

from deepdiff import DeepDiff
from ckanapi import RemoteCKAN, ValidationError


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
                data_dict["name"] = data_dict["name"].replace("/", "_")
                data_dict["name"] = data_dict["name"].replace(".", "-")
                data_dict["name"] = data_dict["name"].lower()
                resource_dict_list = []
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
                    resource_dict_list.append(resource_dict)

                exclude_paths = ['mimetype', 'cache_url', 'state', 'hash', 'cache_last_updated', 'created', 'size',
                                 'mimetype_inner', 'last_modified', 'position', 'url_type', 'id', 'resource_type',
                                 'package_id']
                exclude_regex_paths = [r"root\[\d+\]\['%s'\]" % path for path in exclude_paths]

                cur_package = self.host.action.package_show(id=data_dict["name"])
                diff = DeepDiff(cur_package['resources'], resource_dict_list, ignore_order=True,
                                exclude_regex_paths=exclude_regex_paths)

                logging.info("{} new and {} old resources for dataset '{}'".format(
                    len(diff.get('iterable_item_added', [])), len(diff.get('iterable_item_removed', [])),
                    data_dict['name']))

                # Put dataset on ckan
                try:
                    logging.info(f"Creating dataset '{data_dict['name']}'")
                    self.host.action.package_create(**data_dict)
                except ValidationError:  # Dataset already exists
                    logging.info(f"Dataset '{data_dict['name']}' already exists, updating")
                    package = self.host.action.package_show(id=data_dict['name'])
                    data_dict['id'] = package['id']
                    self.host.action.package_patch(**data_dict)
                except Exception as e:
                    logging.error(f'Exception occurred: {e}')

                if diff:
                    # Delete old resources from the dataset
                    if 'iterable_item_removed' in diff:
                        for key in diff['iterable_item_removed']:
                            resource_d = diff['iterable_item_removed'][key]
                            try:
                                logging.info(f"Deleting resource '{resource_d['name']}'")
                                self.host.action.resource_delete(id=resource_d['id'])
                            except Exception as e:
                                logging.error(
                                    f"Exception occurred when deleting resource '{resource_d['name']}': {e}")
                                pass

                    # Put new resources on the dataset
                    if 'iterable_item_added' in diff:
                        for key in diff['iterable_item_added']:
                            resource_d = diff['iterable_item_added'][key]
                            try:
                                logging.info(f"Creating resource '{resource_d['name']}'")
                                self.host.action.resource_create(**resource_d)
                            except ValidationError:  # Resource already exists
                                logging.info(f"Resource '{resource_d['name']}' already exists, updating")
                                self.host.action.resource_update(**resource_d)
                            except Exception as e:
                                logging.error(f'Exception occurred: {e}')
        else:
            logging.info("JSON request does not contain a dataset")
