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

    col_id = config.PRIMARY_KEYS
    col = config.COLUMNS
    f_coll = config.FIRESTORE_COLLECTION

    records = data['it-fiber-connect-new-construction']
    batch = db.batch()

    for i, row in enumerate(records):
        row[col_id[0]] = row[col_id[0]][0:-13]
        primary_key = row[col_id[0]]
        for ii, el in enumerate(col_id):
            if ii > 0:
                primary_key += '_' + row[el]
        record = dict(id=primary_key)
        for key in col_id + col:
            record[key] = row[key]
        record['project'] = record.pop(col_id[0])
        batch.set(db.collection(f_coll).document(record['id']), record)
        if (i + 1) % config.BATCH_SIZE == 0:
            batch.commit()
    batch.commit()
