import config
import os


class CKANProcessor(object):
    def __init__(self):
        self.meta = config.DATA_CATALOG_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        print("")
        print(selector_data)
        print("conformsTo")
        print(selector_data['conformsTo'])
        print("backupDestination")
        print(selector_data['backupDestination'])
        print("dataset")
        print(selector_data['dataset'])
