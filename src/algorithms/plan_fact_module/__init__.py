import pandas as pd
import psycopg2


def commit_to_db(df):
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='134.209.101.123')
    cursor = conn.cursor()

    cols = ", ".join([str(i) for i in df.keys()])
    for i, row in df.iterrows():
        try:
            sql = "INSERT INTO " + "PLAN_CARRIAGE" + " (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cursor.execute(sql, tuple(row))

            conn.commit()
        except psycopg2.IntegrityError:
            print("psycopg2.IntegrityError")
            conn.rollback()
        except Exception:
            print("Exception")
            conn.rollback()
    conn.close()

df = pd.read_excel("NEV.xlsx")
commit_to_db(df)
print("all done")
df = pd.read_excel("TAL.xlsx")
commit_to_db(df)
print("all done")