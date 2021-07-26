import pandas as pd
import psycopg2

df = pd.read_excel("Dislocation.xlsx")
print(df)

conn = psycopg2.connect(dbname='flow_map2', user='postgres',
                        password='root', host='localhost')

cursor = conn.cursor()


def commit_to_db(conn, cursor, table_name, df):
    cols = ", ".join([str(i) for i in df.keys()])

    for i, row in df.iterrows():
        try:
            sql = "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            print(sql)
            cursor.execute(sql, tuple(row))

            # the connection is not autocommitted by default, so we must commit to save our changes
            conn.commit()
        except ZeroDivisionError:
            print("Commit to DB fail")


commit_to_db(conn, cursor, "dislocation", df)
