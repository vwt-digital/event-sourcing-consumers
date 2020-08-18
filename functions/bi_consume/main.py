import json
import base64
import config
import logging
import random
import string
import hashlib

from dateutil.parser import parse
from google.cloud import firestore_v1

db = firestore_v1.Client()


def handler(request):

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]
        collection = config.firestore[subscription]['collection']
        keys = config.firestore[subscription]['keys']
        dt = parse(envelope['message']['publishTime'])
        ts = int(dt.timestamp())
        logging.info(f'Read message from subscription {subscription}')
    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    if data.get('data'):
        data = data['data']

    fs_write(collection, keys, ts, data)

    return 'OK', 204


def fs_write(coll, keys, ts, data):

    if isinstance(data, list):
        data = data
    else:
        data = [data]

    for record in data:
        record['updated'] = ts
        unique_key = '|'.join(f'{record[key]}' for key in keys)
        unique_id = hashlib.md5(unique_key.encode()).hexdigest()
        version_id = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))

        collection = db.collection(coll)
        ref = collection.document(unique_id)
        doc = ref.get()

        if not doc.exists:
            logging.info(f'Document with id {unique_id} in collection {coll} does not exist, creating new document')
            record['created'] = ts
            ref.set(record)
            ref.collection(f'_{coll}_versions').document(version_id).create(record)
        else:
            logging.info(f'Document with id {unique_id} in collection {coll} already exists, checking document version')
            doc_dict = doc.to_dict()
            if record.get('updated') == doc_dict.get('updated'):
                logging.info(f'Document version already exists, finishing')
            elif record.get('updated') > doc_dict.get('updated'):
                logging.info(f'Document version does not exist, creating new version {version_id}')
                record['created'] = doc_dict['created']
                ref.set(record)
                ref.collection(f'_{coll}_versions').document(version_id).set(record)
            elif record.get('updated') < doc_dict.get('updated'):
                logging.info(f'Document version is outdated, finishing')
