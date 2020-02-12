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
        for data in selector_data['dataset']:
            # Put the details of the dataset we're going to create into a dict.
            # Using data.get sometimes because some values can remain empty while others should give an error
            dict_list = [
                {"access level": data.get('accessLevel')},
                {"Issued": data.get('issued')},
                {"Spatial": data.get('spatial')},
                {"Modified": data.get('modified')}
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
            # name is used for url and cant have uppercase or spaces so we have to replace those
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
                self.host.action.package_create(name=data_dict["name"], owner_org=data_dict["owner_org"], data_dict=data_dict)
                # Put the resources on the dataset
                for resource_d in resource_dict_list:
                    self.host.action.resource_create(data_dict=resource_d)
            except ValidationError:
                # Except if dataset already exists
                print(f"Dataset {data_dict['name']} already exists, update")
                self.host.action.package_update(name=data_dict["name"], owner_org=data_dict["owner_org"], data_dict=data_dict)
                for resource_d in resource_dict_list:
                    # Try to add resource
                    try:
                        self.host.action.resource_create(package_id=resource_d["package_id"], data_dict=resource_d)
                    except ValidationError:
                        # Resource already exists
                        print(f"Resource {resource_d['name']} already exists, update")
                        self.host.action.resource_update(package_id=resource_d["package_id"], data_dict=resource_d)
            except Exception as e:
                logging.error(f'Exception occurred:{e}')
