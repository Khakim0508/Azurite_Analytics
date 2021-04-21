import psycopg2
import pandas as pd
from math import radians, cos, sin, asin, sqrt

from psycopg2._psycopg import IntegrityError

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='localhost')

cursor = conn.cursor()

conn2 = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='134.209.101.123')

cursor2 = conn2.cursor()

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    # Radius of earth in kilometers is 6371
    km = 6371* c
    return km


def add_route(s1, s2):
    cursor.execute(
        "SELECT LATITUDE, LONGITUDE FROM STATIONS WHERE id = {}".format(s1))
    row = cursor.fetchone()
    lat1 = row[0]
    lon1 = row[1]
    cursor.execute(
        "SELECT LATITUDE, LONGITUDE FROM STATIONS WHERE id = {}".format(s2))
    row = cursor.fetchone()
    lat2 = row[0]
    lon2 = row[1]
    distance = haversine(lat1, lon1, lat2, lon2)
    print('insert into routes (a, b, distance)'
          ' values({}, {}, {});'.format(s1, s2, distance))
    cursor.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(s1, s2, distance))
    cursor.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(s2, s1, distance))
    conn.commit()

    cursor2.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(s1, s2, distance))
    cursor2.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(s2, s1, distance))
    conn2.commit()


def insert_routes(stat1, stat3, list_stat):
    try:

        cursor.execute(
            "SELECT id FROM STATIONS WHERE name = \'{}\'".format(stat1))
        id1 = cursor.fetchone()[0]

        cursor.execute(
            "SELECT id FROM STATIONS WHERE name = \'{}\'".format(stat3))
        id3 = cursor.fetchone()[0]

        cursor.execute("delete from routes where a = {} and b = {}".format(id1, id3))
        cursor.execute("delete from routes where a = {} and b = {}".format(id3, id1))

        cursor2.execute("delete from routes where a = {} and b = {}".format(id1, id3))
        cursor2.execute("delete from routes where a = {} and b = {}".format(id3, id1))

        for stat in list_stat:
            cursor.execute(
                "SELECT id FROM STATIONS WHERE name = \'{}\'".format(stat))
            id2 = cursor.fetchone()[0]
            add_route(id1, id2)
            id1 = id2

        add_route(id1, id3)


    except IntegrityError:
        print("something went wrong")
        cursor.execute("ROLLBACK")
        cursor2.execute("ROLLBACK")


list_stat = []
stations = pd.read_excel("Stations.xlsx")
for i, row in stations.iterrows():
    if row["State"] == 0:
        list_stat.append(row["Station"])
print(len(list_stat))
#insert_routes("Шарташ", "Елимай (обп) (эксп.)", list_stat)

def delete_routes(id1, id3):
    try:
        cursor.execute("delete from routes where a = {} and b = {}".format(id1, id3))
        print("delete from routes where a = {} and b = {}".format(id1, id3))
        cursor.execute("delete from routes where a = {} and b = {}".format(id3, id1))
        conn.commit()

        cursor2.execute("delete from routes where a = {} and b = {}".format(id1, id3))
        print("delete from routes where a = {} and b = {}".format(id1, id3))
        cursor2.execute("delete from routes where a = {} and b = {}".format(id3, id1))
        conn2.commit()



    except IntegrityError:
        print("something went wrong")
        cursor.execute("ROLLBACK")

