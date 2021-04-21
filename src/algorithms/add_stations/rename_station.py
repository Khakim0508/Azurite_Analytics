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

def rename_station(old_name, new_name):
    print(123)