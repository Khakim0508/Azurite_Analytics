import psycopg2
from math import radians, cos, sin, asin, sqrt

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='localhost')

cursor = conn.cursor()


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


def connect_two_stations(cursor, conn, node1, node2):
    cursor.execute(
        "SELECT ID, LATITUDE, LONGITUDE FROM STATIONS WHERE NAME = \'{}\'".format(node1))
    row = cursor.fetchone()
    stan1 = row[0]
    lat1 = row[1]
    lon1 = row[2]
    cursor.execute(
        "SELECT ID, LATITUDE, LONGITUDE FROM STATIONS WHERE NAME = \'{}\'".format(node2))
    row = cursor.fetchone()
    stan2 = row[0]
    lat2 = row[1]
    lon2 = row[2]
    distance = haversine(lat1, lon1, lat2, lon2)
    print('insert into routes (a, b, distance)'
          ' values({}, {}, {});'.format(stan1, stan2, distance))
    print('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(stan2, stan1, distance))
    cursor.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(stan1, stan2, distance))
    cursor.execute('insert into routes (a, b, distance)'
                   ' values({}, {}, {});'.format(stan2, stan1, distance))
    conn.commit()

connect_two_stations(cursor, conn, "Павлодар", "Павлодар-южный")