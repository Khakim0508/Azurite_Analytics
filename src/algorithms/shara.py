import pandas as pd
import psycopg2

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                        password='root', host='localhost')
cursor = conn.cursor()
cursor.execute("select name, country from stations")
sh = cursor.fetchall()

data = pd.DataFrame(data=sh, columns=["Stations", "Country"])
data.to_excel("Stations.xlsx")