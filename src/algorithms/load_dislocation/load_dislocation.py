import os
import pandas as pd
import psycopg2
import datetime

cols = {"Номер вагона": "CarNumber",
        "Станция отправления": 'FromStationName',
        "Станция назначения": 'ToStationName',
        "Станция текущей дислокации": 'LastStationName',
        "Груз": 'CargoEtsngName',
        "Расстояние осталось (от текущей станции)": 'RestDistance',
        "Дата и время отправления": "ShippingDate",
        "Дата и время последней операции": "LastOperationDate",
        "Остаточный пробег": "RestRun",
        "Операция (полное наименование)": "LastOperationName",
        "Простой на станции дислокации": "IdleOnTheLastStation",
        "Собственник (по данным ЭТРАН, ГВЦ)": "OwnerName",
        "Состояние вагона": "CarStateName",
        "Группа": "GroupName",
        "Вес груза, тонны": "CargoWeight",
        "код ст. отправления (6 знаков)": "FromStationCode",
        "код ст. назначения (6 знаков)": "ToStationCode",
        "Станция текущей дислокации, код": "LastStationCode",
        "Груз, код": "CargoEtsngCode",
        "Индекс поезда": "TrainIndex",
        "НРП (вагон в нерабочем парке)": "RepairCurrent_isNRP",
        "Неисправность текущая": "CarFaultinessName",
        "Грузоподъемность, тн": "CarCapacity",
        "Тип вагона": "CarTypeShortName",
        "Номер поезда": "InvNumber",
        "Дата след. ДР" : "PlannedRepairDate"
    }


pd.set_option("display.max_rows", None, "display.max_columns", None)

def commit_df(filename, day_number, month_number):
    dt = datetime.datetime(2021, month_number, day_number, 17, 0)
    pre = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(pre, filename)
    data = pd.read_excel(path)

    data.rename(columns=cols, inplace=True)

    data = data.loc[0:, ["CarNumber", "FromStationCode", "FromStationName", "ShippingDate",
                         "ToStationCode", "ToStationName", "LastOperationDate",
                         "LastStationName", "LastOperationName", "LastStationCode", "RestDistance",
                         "CargoEtsngName", "CargoEtsngCode", "TrainIndex", "IdleOnTheLastStation",
                         "GroupName", "CargoWeight", "RepairCurrent_isNRP", "CarFaultinessName",
                         "OwnerName", "CarCapacity", "CarTypeShortName", "InvNumber",
                         "RestRun", "CarStateName", "PlannedRepairDate"]]

    data["FromStationCode"] = data["FromStationCode"].fillna(0)
    data["FromStationCode"] = data["FromStationCode"].astype(int)

    data["InvNumber"] = data["InvNumber"].fillna(0)
    data["InvNumber"] = data["InvNumber"].astype(int)

    data["ToStationCode"] = data["ToStationCode"].fillna(0)
    data["ToStationCode"] = data["ToStationCode"].astype(int)

    data["LastStationCode"] = data["LastStationCode"].fillna(0)
    data["LastStationCode"] = data["LastStationCode"].astype(int)

    data["CargoEtsngName"] = data["CargoEtsngName"].fillna('')
    data["ShippingDate"] = data["ShippingDate"].replace({pd.NaT: None})
    data["PlannedRepairDate"] = data["PlannedRepairDate"].replace({pd.NaT: None})
    data["LastOperationDate"] = data["LastOperationDate"].replace({pd.NaT: None})
    data['RestRun'] = data['RestRun'].fillna(0)
    data["update_datetime"] = dt

    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()

    commit_to_db(conn, cursor, "dislocation", data)



def commit_to_db(conn, cursor, table_name, df):
    cols = ", ".join([str(i) for i in df.keys()])

    for i, row in df.iterrows():
        try:
            sql = "INSERT INTO " + table_name + " (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
            cursor.execute(sql, tuple(row))

            # the connection is not autocommitted by default, so we must commit to save our changes
            conn.commit()
        except ZeroDivisionError:
            print("Commit to DB fail")


def commit_month_dislocation(month_number):
    file_name_template = "Dislocation_2021-0" + str(month_number) + "-{}_08-00-00_15.xls"
    for i in range(1, 32):
        try:
            if i < 10:
                commit_df(file_name_template.format("0" + str(i)), i, month_number)
            else:
                commit_df(file_name_template.format(str(i)), i, month_number)
        except Exception:
            print(i, end=" ")
            print("число не было обработано")


commit_month_dislocation(4)
print("successnyi success")
