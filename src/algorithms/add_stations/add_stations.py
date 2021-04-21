import psycopg2
import pandas as pd

from psycopg2._psycopg import IntegrityError

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='localhost')

cursor = conn.cursor()

conn2 = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='134.209.101.123')

cursor2 = conn2.cursor()

def add_stations():
    stations = pd.read_excel("Stations.xlsx")
    j = 0
    for i, row in stations.iterrows():
        if row["State"] == 0:
            cursor.execute(f'insert into stations (name, country, latitude, longitude) values '
                           f'(\'{row["Station"]}\', \' \', {row["Lat"]}, {row["Lon"]})')
            cursor2.execute(f'insert into stations (name, country, latitude, longitude) values '
                           f'(\'{row["Station"]}\', \' \', {row["Lat"]}, {row["Lon"]})')
            conn.commit()
            conn2.commit()
            # cursor.execute("delete from stations where name = \'{}\'".format(row["Station"]))
            # cursor2.execute("delete from stations where name = \'{}\'".format(row["Station"]))
            #
            # conn.commit()
            # conn2.commit()

            print(j)
            j+=1

#   add_stations()