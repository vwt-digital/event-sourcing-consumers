from google.cloud import datastore
import config
import os
import logging


class DBProcessor(object):
    def __init__(self):
        self.client = datastore.Client()
        self.meta = config.AFAS_DB_PROCESSOR[os.environ.get('DATA_SELECTOR', 'Required parameter is missed')]
        pass

    def process(self, payload):
        if 'id_property' in self.meta and self.meta['id_property'] in payload:
            kind, key = self.identity(payload)
            entity_key = self.client.key(kind, key)
            entity = self.client.get(entity_key)
            if not entity:
                entity = datastore.Entity(key=entity_key)
        elif 'filter_property' in self.meta and self.meta['filter_property'] in payload:
            # get entity_key from filter property
            query = self.client.query(kind=self.meta['entity_name'])
            query.add_filter(self.meta['filter_property'], '=', payload[self.meta['filter_property']])
            query_results = list(query.fetch(limit=1))
            entity = query_results[0] if query_results else None
        else:
            logging.error('Received payload without matching id_property or filter_property')
            entity = None

        if entity is not None:
            self.populate_from_payload(entity, payload)
            self.client.put(entity)

    def identity(self, payload):
        return self.meta['entity_name'], payload[self.meta['id_property']]

    @staticmethod
    def populate_from_payload(entity, payload):
        for name in payload.keys():
            value = payload[name]
            entity[name] = value
