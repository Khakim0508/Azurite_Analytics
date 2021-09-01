import pandas as pd
import psycopg2


def commit_to_db(df):
    df = df[['ShippingDate', 'CarAmount', 'FromStationName', 'ToStationName', 'CargoEtsngName']]
    conn = psycopg2.connect(dbname='flow_map2', user='postgres',
                            password='root', host='localhost')#134.209.101.123
    cursor = conn.cursor()

    cols = ", ".join([str(i) for i in df.keys()])
    for i, row in df.iterrows():
        try:
            sql = "INSERT INTO " + "PLAN_CARRIAGE" + " (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            print(sql)
            cursor.execute(sql, tuple(row))

            conn.commit()
        except psycopg2.IntegrityError:
            print("psycopg2.IntegrityError")
            conn.rollback()

    conn.close()


df = pd.read_excel("KBL.xlsx")
df['CarAmount'] = df['CarAmount'].fillna(0)
#commit_to_db(df)
print(1)
df = pd.read_excel("TAL.xlsx")
df['CarAmount'] = df['CarAmount'].fillna(0)
#commit_to_db(df)
print(1)
df = pd.read_excel("BMZ.xlsx")
df['CarAmount'] = df['CarAmount'].fillna(0)
#commit_to_db(df)
print(1)
df = pd.read_excel("KAL.xlsx")
df['CarAmount'] = df['CarAmount'].fillna(0)
#commit_to_db(df)
print(1)
df = pd.read_excel("NEV2.xlsx")
df['CarAmount'] = df['CarAmount'].fillna(0)
commit_to_db(df)
print(1)
print("all done")