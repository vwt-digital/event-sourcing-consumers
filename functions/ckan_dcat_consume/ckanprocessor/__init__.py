import config
import os
from ckanapi import RemoteCKAN, ValidationError
import logging


class CKANProcessor(object):
    def __init__(self):
        self.meta = config.DATA_CATALOG_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.api_key = os.environ.get('API_KEY', 'Required parameter is missing')
        self.ckan_host = os.environ.get('CKAN_SITE_URL', 'Required parameter is missing')
        self.host = RemoteCKAN(self.ckan_host, apikey=self.api_key)

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        if('dataset' in selector_data):
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
                    {"key": "Publisher", "value": data.get('publisher')},
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
                try:
                    # Put dataset on ckan
                    self.host.action.package_create(**data_dict)
                    # Put the resources on the dataset
                    for resource_d in resource_dict_list:
                        self.host.action.resource_create(**resource_d)
                except ValidationError:
                    # Except if dataset already exists
                    print(f"Dataset {data_dict['name']} already exists, update")
                    self.host.action.package_update(**data_dict)
                    for resource_d in resource_dict_list:
                        # Try to add resource
                        try:
                            self.host.action.resource_create(**resource_d)
                        except ValidationError:
                            # Resource already exists
                            print(f"Resource {resource_d['name']} already exists, update")
                            self.host.action.resource_update(**resource_d)
                except Exception as e:
                    logging.error(f'Exception occurred:{e}')
        else:
            print("JSON request does not contain a dataset")
