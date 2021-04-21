import pandas as pd
import psycopg2

def view_map():
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='134.209.101.123', port=5432)
    cursor = conn.cursor()

    columns = ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
               'Destination_Longitude']
    df = pd.DataFrame(columns=columns)
    cursor.execute("select a, b from routes")
    routes = cursor.fetchall()
    j = 0
    for i in routes:
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(i[0]))
        point1 = cursor.fetchone()
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(i[1]))
        point2 = cursor.fetchone()
        print(j)
        j += 1

        df = df.append({columns[0]: point1[0] + ", " + point1[1],
                                      columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                      columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                      columns[5]: float(point2[3])}, ignore_index=True)

        df.to_excel("map.xlsx")

view_map()