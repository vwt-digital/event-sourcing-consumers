import json
import base64
import config
import logging
import datetime

from google.cloud import firestore_v1

db = firestore_v1.Client()


def handler(request):

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Read message from subscription {subscription}')
        write_to_fs(data)
    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    return 'OK', 204


def write_to_fs(data):

    records = data['it-fiber-connect-new-construction']
    batch = db.batch()

    for i, row in enumerate(records):
        record = {}
        for key in config.COLUMNS:
            record[key] = row[key]
        batch.set(db.collection(config.FIRESTORE_COLLECTION[0]).document(record[config.PRIMARY_KEYS[0]]), record)
        if (i + 1) % config.BATCH_SIZE == 0:
            batch.commit()
            logging.info(f'Write {i} message(s) to the firestore')
    batch.commit()
    db.collection(config.FIRESTORE_COLLECTION[1]).document('update_date_consume').set(dict(
        id='update_date_consume', date=datetime.datetime.now().strftime('%Y-%m-%d')))
    logging.info('Writing message to firestore finished')
