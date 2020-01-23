import config
# import consumerConfig as config
import os
import psycopg2
from pyproj import Proj
from pyproj import transform


class DBProcessor(object):
    def __init__(self):
        self.meta = config.DB_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        # Get environment variables given to the function
        self.sql_pass = os.environ.get("_SQL_PASS")
        self.db_user = os.environ.get("_DB_USER")
        self.host = '/cloudsql/{}'.format(os.environ.get("INSTANCE_CONNECTION_NAME"))
        pass

    def process(self, payload):
        try:
            connection = psycopg2.connect(user=self.db_user, password=self.sql_pass, host=self.host)
            cursor = connection.cursor()
            # TODO: select 1 from {entity_table} where {id_property} = {id_prop_value}
            # TODO: if id already in entity_table:
            #    add_values = '''UPDATE {} SET location=postgis.createpoint({}, {}) WHERE {} = {};'''
            # .format(self.meta['entity_name'], key, value, self.meta['id_property'], payload[self.meta['id_property']])
            #    cursor.execute(add_values)
            #    connection.commit()
            # TODO: else:
            #    sql_insert_statement =
            # 'INSERT INTO {}({id_property}, location) VALUES({id_property_value}, postgis.createpoint({},{}}))'
            lonlat = self.coordinatesToPostgis(payload[self.meta['x_coordinate']], payload[self.meta['y_coordinate']])
            add_values = '''UPDATE {} SET {} = {} WHERE {} = {};'''.format(self.meta['entity_name'], self.meta['geometry'], lonlat, self.meta['id_property'], payload[self.meta['id_property']])  # noqa: E501
            cursor.execute(add_values)
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while updating PostgreSQL table", error)
        finally:
            # closing database connection.
            if(connection):
                cursor.close()
                connection.close()

    def coordinatesToPostgis(self, x_coordinate, y_coordinate):
        inProj = Proj('epsg:3857')
        outProj = Proj('epsg:4326')
        lon, lat = transform(inProj, outProj, x_coordinate, y_coordinate)

        # Only points are added now, but what if we want other geometry? Then we should get more info from the JSON

        # Point in EPSG 4326, aka longitude and latitude
        return "ST_SetSRID(ST_MakePoint({},{}),4326)".format(lon, lat)
