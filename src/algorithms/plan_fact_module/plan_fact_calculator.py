import pandas as pd
import psycopg2
import os



file_name_template = "Dislocation_2021-02-{}_08-00-00_15.xls"
def commit_to_db(df):
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()

    cols = ", ".join([str(i) for i in df.keys()])
    for i, row in df.iterrows():
        try:
            sql = "INSERT INTO " + "FACT_CARRIAGE" + " (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cursor.execute(sql, tuple(row))

            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()
        except Exception:
            conn.rollback()
    conn.close()


for i in range(19, 29):
    df = pd.DataFrame
    if i < 10:
        df = pd.read_excel(file_name_template.format("0" + str(i)))
    else:
        df = pd.read_excel(file_name_template.format(str(i)))
    df = df.loc[0:, ['Номер вагона', 'Станция отправления', 'Станция назначения',
                     'Дата и время отправления', 'Груз']]
    df.columns = ['CarNumber', 'FromStationName', 'ToStationName', 'ShippingDate', 'CargoEtsngName']
    df['ShippingDate'] = pd.to_datetime(df['ShippingDate']).dt.date
    commit_to_db(df)
    print(str(i) + " successnyi success")


print("successnyi success")