import config
import os
import logging
from defusedxml import ElementTree as defusedxml_ET
import xml.etree.ElementTree as ET  # nosec

from azure.storage.fileshare import ShareClient, ShareLeaseClient
from azure.core.exceptions import HttpResponseError

from google.cloud import secretmanager

logging.basicConfig(level=logging.INFO)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)


class Link2Processor(object):
    def __init__(self):
        self.destshare = config.AZURE_DESTSHARE
        self.storageaccount = os.environ.get('AZURE_STORAGEACCOUNT', 'Required parameter is missing')
        self.project_id = os.environ.get('PROJECT_ID', 'Required parameter is missing')
        self.storagekey_secret_id = os.environ.get('AZURE_STORAGEKEY_SECRET_ID', 'Required parameter is missing')
        client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/{self.project_id}/secrets/{self.storagekey_secret_id}/versions/latest"
        key_response = client.access_secret_version(request={"name": secret_name})
        self.storagekey = key_response.payload.data.decode("UTF-8")
        self.sourcepath_field = config.SOURCEPATH_FIELD
        self.mapping_json = config.MAPPING

    def make_xml(self, selector_data, file_name):
        root = ET.Element(config.XML_ROOT)
        subelement = ET.SubElement(root, config.XML_ROOT_SUBELEMENT)
        for field in self.mapping_json:
            field_json = self.mapping_json[field]
            field_map = selector_data.get(field_json)
            if field_map == "None" or not field_map:
                ET.SubElement(subelement, field).text = ""
            else:
                ET.SubElement(subelement, field).text = field_map

        return root

    def msg_to_fileshare(self, msg):
        sourcepath_field_msg = msg.get(self.sourcepath_field)
        if not sourcepath_field_msg:
            logging.error(f"The sourcepath field {sourcepath_field_msg} cannot be found in message")

        destfilepath = f"{sourcepath_field_msg}.xml"

        logging.info(f"Putting {destfilepath} on //{self.storageaccount}/{self.destshare}")
        share = ShareClient(account_url=f"https://{self.storageaccount}.file.core.windows.net/",
                            share_name=self.destshare, credential=self.storagekey)
        file_on_share = share.get_file_client(destfilepath)
        try:
            file_on_share.create_file(size=0)
        except HttpResponseError:
            ShareLeaseClient(file_on_share).break_lease()
            file_on_share.create_file(size=0)
        file_lease = file_on_share.acquire_lease(timeout=5)
        sourcefile = self.make_xml(msg, destfilepath)
        sourcefile_string = ET.tostring(sourcefile, encoding='utf8', method='xml')
        safe_xml_tree = defusedxml_ET.fromstring(sourcefile_string)
        safe_xml_tree_string = defusedxml_ET.tostring(safe_xml_tree)
        logging.info(f"Writing to //{self.storageaccount}/{self.destshare}/{destfilepath}")
        file_on_share.upload_file(safe_xml_tree_string, lease=file_lease)
        file_lease.release(timeout=5)

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        if isinstance(selector_data, list):
            for data in selector_data:
                self.msg_to_fileshare(data)
        elif isinstance(selector_data, dict):
            self.msg_to_fileshare(selector_data)
        else:
            logging.error("Message is not a list or a dictionary")
