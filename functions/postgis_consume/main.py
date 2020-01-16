import logging
import json
import base64
import json
from flask import Request

#from dbprocessor import DBProcessor

import config  # noqa: F401

#parser = DBProcessor()

logging.basicConfig(level=logging.INFO)

# First json to postgis, then postgis to database
def json_to_database(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        # parser.process(json.loads(payload))

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


if __name__ == '__main__':
    logging.info("Hallo")
    environ = {}
    biepboep = Request(environ)
    data = base64.b64encode(json.dumps({"name": "John", "age":30, "city":"New York"}).encode('utf-8'))
    biepboep.data = json.dumps({"message": {"data": data.decode('utf-8')}, "subscription" : "hoi"}).encode('utf-8')
    json_to_database(biepboep)
