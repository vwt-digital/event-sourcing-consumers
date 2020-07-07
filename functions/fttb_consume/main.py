import json
import base64
import config
import logging
from google.cloud import firestore_v1

db = firestore_v1.Client()


def handler(request):

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]
        collection = config.firestore[subscription]['collection']
        logging.info(f'Read message from subscription {subscription}')

        if subscription == config.subscription1:
            data = data[config.firestore[subscription]['subject']]
            fs_write_planning(data, collection)
            return 'OK', 204

    except Exception:
        logging.exception('Extracting of data failed')
        return 'Error', 500


def fs_write_planning(data, coll):

    if isinstance(data, list):
        data = data
    else:
        data = [data]

    batch = db.batch()
    i = 0
    for record in data:
        record['id'] = str(record['workflow_id'])
        record['project'] = 'FttB'
        batch.set(db.collection(coll).document(record['id']), record)
        i += 1
        if (i + 1) % 500 == 0:
            batch.commit()
            i = 0
            logging.info(f'Comitted {i} records')
    batch.commit()
    logging.info(f'Comitted {i} records, finished message')
