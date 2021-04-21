import psycopg2
import pandas as pd
import datetime

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='localhost')
cursor = conn.cursor()

dt = datetime.datetime.now()

cursor.execute(f"delete"
               f"from report"
               f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00'"
               f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00';")
cursor.execute(f"delete"
               f"from dislocation"
               f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00'"
               f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00';")

