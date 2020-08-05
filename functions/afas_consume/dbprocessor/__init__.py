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
            kind = self.meta['entity_name']
            key = self.value_formatter(payload.get(self.meta['id_property'], None))

            if kind and key:
                payload[self.meta['id_property']] = key  # Make sure edited value is updated in payload

                entity_key = self.client.key(kind, key)
                entity = self.client.get(entity_key)
                if not entity:
                    entity = datastore.Entity(key=entity_key)
            else:
                logging.info('Received payload without matching id_property or filter_property, skipping this entity')
                entity = None
        elif 'filter_property' in self.meta and self.meta['filter_property'] in payload:
            payload[self.meta['filter_property']] = self.value_formatter(
                payload.get(self.meta['filter_property']))  # Make sure edited value is updated in payload

            # get entity_key from filter property
            query = self.client.query(kind=self.meta['entity_name'])
            query.add_filter(
                self.meta['filter_property'], '=', payload[self.meta['filter_property']])
            query_results = list(query.fetch(limit=1))

            if self.meta.get('create_entity', False) and not query_results:
                entity_key = self.client.key(self.meta['entity_name'], payload[self.meta['filter_property']])
                entity = datastore.Entity(key=entity_key)
            else:
                entity = query_results[0] if query_results else None
        else:
            logging.info('Received payload without matching id_property or filter_property, skipping this entity')
            entity = None

        if entity is not None:
            self.populate_from_payload(entity, payload)
            self.client.put(entity)

    def value_formatter(self, value):
        if value and 'value_formatter' in self.meta and 'type' in self.meta['value_formatter']:
            if self.meta['value_formatter']['type'] == 'split' and 'value' in self.meta['value_formatter']:
                splitted_value = value.split(self.meta['value_formatter']['value'])
                value = splitted_value[self.meta['value_formatter'].get('index', 0)]

            if self.meta['value_formatter']['type'] == 'prepend' and 'value' in self.meta['value_formatter']:
                value = self.meta['value_formatter']['value'] + value

            if self.meta['value_formatter']['type'] == 'append' and 'value' in self.meta['value_formatter']:
                value = value + self.meta['value_formatter']['value']

        return value

    @staticmethod
    def populate_from_payload(entity, payload):
        for name in payload.keys():
            value = payload[name]
            entity[name] = value
