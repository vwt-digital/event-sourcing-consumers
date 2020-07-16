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
        logging.info(f'Read message from subscription {subscription}')
    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    write_to_fs(data)

    return 'OK', 204


def write_to_fs(data):

    records = data['it-fiber-connect-new-construction']
    batch = db.batch()

    for i, row in enumerate(records):
        primary_key = row[config.PRIMARY_KEYS[0]]
        record = {}
        for key in config.COLUMNS:
            record[key] = row[key]
        batch.set(db.collection(config.FIRESTORE_COLLECTION).document(record[primary_key]), record)
        if (i + 1) % config.BATCH_SIZE == 0:
            batch.commit()
    batch.commit()
