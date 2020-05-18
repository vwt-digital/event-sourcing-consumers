import config
import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData
from shapely.geometry import Point
import geoalchemy2


class DBProcessor(object):
    def __init__(self):
        self.meta = config.DB_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        self.attributes = config.DB_ATTRIBUTES
        # Get environment variables given to the function
        self.sql_pass = os.environ.get("_SQL_PASS")
        self.db_user = os.environ.get("_DB_USER")
        self.instance_connection_name = os.environ.get('INSTANCE_CONNECTION_NAME')
        self.host = f"/cloudsql/{self.instance_connection_name}"
        self.db_name = os.environ.get("_DB_NAME")
        self.engine = create_engine('postgresql+psycopg2://', creator=self.getconn)
        self.connection = self.engine.connect()

    def __del__(self):
        # closing database connection.
        self.connection.close()

    def process(self, payload):
        selector_data_value = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]

        if isinstance(selector_data_value, list):
            selector_data_list = selector_data_value
        else:
            selector_data_list = [selector_data_value]

        for selector_data in selector_data_list:
            # If both x and y coordinate in request put request in DB, otherwise do nothing
            if self.meta['longitude'] in selector_data and self.meta['latitude'] in selector_data:
                # Only if x and y are not 0
                if selector_data[self.meta['longitude']] != "0" and selector_data[self.meta['latitude']] != "0" \
                        and selector_data[self.meta['longitude']] and selector_data[self.meta['latitude']]:
                    meta_data = MetaData(bind=self.engine, reflect=True)
                    workflow_projects = meta_data.tables[self.meta['entity_name']]

                    params = {}
                    params[self.meta.get("id_property")] = selector_data[self.meta.get("id_property")]

                    # Add UPSERT params from config
                    for attribute in self.attributes:
                        if(attribute != "geometry" and attribute != "entity_name" and attribute in selector_data):
                            params[attribute] = selector_data[attribute]

                    # Add geometry
                    lonlat = self.coordinatesToPostgis(selector_data[self.meta['longitude']], selector_data[self.meta['latitude']])
                    params[self.meta['geometry']] = lonlat

                    # Do PostgreSQL UPSERT
                    upsert = insert(workflow_projects).values([params])
                    upsert = upsert.on_conflict_do_update(
                        index_elements=[self.meta['id_property']],
                        set_=params
                    )
                    self.connection.execute(upsert)
                else:
                    print("x coordinate or y coordinate is 0, no upload")

    def coordinatesToPostgis(self, longitude, latitude):
        # Only points are added now, but what if we want other geometry? Then we should get more info from the JSON
        point = geoalchemy2.shape.from_shape(Point(float(longitude), float(latitude)), srid=4326)

        # Point in EPSG 4326, aka longitude and latitude
        # Returns the same as when you do: "ST_Transform(ST_SetSRID(ST_MakePoint({longitude},{latitude}),3857),4326)"
        return point

    def getconn(self):
        c = psycopg2.connect(user=self.db_user, password=self.sql_pass, host=self.host, dbname=self.db_name)
        return c
