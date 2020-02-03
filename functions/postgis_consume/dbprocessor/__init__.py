import config
import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import MetaData
from shapely.geometry import Point
import geoalchemy2
from pyproj import Proj, transform


class DBProcessor(object):
    def __init__(self):
        self.meta = config.DB_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        # Get environment variables given to the function
        self.sql_pass = os.environ.get("_SQL_PASS")
        self.db_user = os.environ.get("_DB_USER")
        self.host = f"/cloudsql/{os.environ.get('INSTANCE_CONNECTION_NAME')}"
        self.db_name = os.environ.get("_DB_NAME")
        pass

    def process(self, payload):
        selector_data = payload[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        # If both x and y coordinate in request put request in DB, otherwise do nothing
        if self.meta['x_coordinate'] in selector_data and self.meta['y_coordinate'] in selector_data:
            # Only if x and y are not 0
            if selector_data[self.meta['x_coordinate']] != 0 and selector_data[self.meta['y_coordinate']] != 0:
                try:
                    engine = create_engine('postgresql+psycopg2://', creator=self.getconn)
                    connection = engine.connect()

                    lonlat = self.coordinatesToPostgis(selector_data[self.meta['x_coordinate']], selector_data[self.meta['y_coordinate']])

                    meta_data = MetaData(bind=engine, reflect=True)
                    workflow_projects = meta_data.tables[self.meta['entity_name']]

                    # Do PostgreSQL UPSERT
                    upsert = insert(workflow_projects).values([
                        {self.meta.get("id_property"): selector_data[self.meta.get("id_property")], self.meta.get("geometry"): lonlat}
                    ]
                    )
                    geom_params = {}
                    geom_params[self.meta['geometry']] = lonlat
                    upsert = upsert.on_conflict_do_update(
                        index_elements=[self.meta['id_property']],
                        set_=geom_params
                    )
                    connection.execute(upsert)
                finally:
                    # closing database connection.
                    connection.close()

    def coordinatesToPostgis(self, x_coordinate, y_coordinate):
        # Only points are added now, but what if we want other geometry? Then we should get more info from the JSON

        # Transform from projected (coordinate) system to latitude and longitude system
        inProj = Proj("epsg:3857")
        outProj = Proj("epsg:4326")
        lon, lat = transform(inProj, outProj, float(x_coordinate), float(y_coordinate))

        geometry_dict = {}
        geometry_dict["geom"] = f"POINT({lon},{lat})"
        geometry_dict["srid"] = 4326

        point = geoalchemy2.shape.from_shape(Point(lon, lat), srid=4326)

        # Point in EPSG 4326, aka longitude and latitude
        # Returns the same as when you do: "ST_Transform(ST_SetSRID(ST_MakePoint({x_coordinate},{y_coordinate}),3857),4326)"
        return point

    def getconn(self):
        c = psycopg2.connect(user=self.db_user, password=self.sql_pass, host=self.host, dbname=self.db_name)
        return c
