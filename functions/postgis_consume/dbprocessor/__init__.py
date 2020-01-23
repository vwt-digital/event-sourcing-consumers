import config
# import consumerConfig as config
import os
import psycopg2
import osgeo.ogr as ogr
import osgeo.osr as osr


class DBProcessor(object):
    def __init__(self):
        self.meta = config.DB_PROPERTIES[os.environ.get('DATA_SELECTOR', 'Required parameter is missing')]
        # Get environment variables given to the function
        self.sql_pass = os.environ.get("_SQL_PASS")
        self.db_name = os.environ.get("_DB_NAME")
        self.db_user = os.environ.get("_DB_USER")
        self.host = '/cloudsql/' + self.db_name
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
        # Spatial Reference System
        inputEPSG = 3857
        outputEPSG = 4326

        # create a geometry from coordinates
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(x_coordinate, y_coordinate)

        # create coordinate transformation
        inSpatialRef = osr.SpatialReference()
        inSpatialRef.ImportFromEPSG(inputEPSG)

        outSpatialRef = osr.SpatialReference()
        outSpatialRef.ImportFromEPSG(outputEPSG)

        coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

        # transform point
        point.Transform(coordTransform)

        # TODO: Only points are added now, but what if we want other geometry? Should get more info from the JSON

        # Point in EPSG 4326, aka longitude and latitude
        return "ST_SetSRID(ST_MakePoint({},{}),4326)".format(point.GetX(), point.GetY())
