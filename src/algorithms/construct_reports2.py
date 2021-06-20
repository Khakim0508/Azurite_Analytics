import math
import pandas as pd
import datetime
import psycopg2
import os
from algorithms.graph_algorithms import bfs
from algorithms.graph_algorithms import construct_graph
from algorithms.graph_algorithms import dijikstra_graph
from sqlalchemy import create_engine

pd.options.mode.chained_assignment = None


def add_route(route, start_id, end_id, number_of_carriages, cursor, loaded_df,
              empty_df, real_end_id, start, real_end, cargo, columns, distance):
    loaded = ''
    empty = ''
    number_of_carriages1 = number_of_carriages2 = 0

    number_of_carriages = int(number_of_carriages)
    # В метод попадает число вагонов представленных в виде числа с плавающей точкой

    cargo = cargo.strip()
    # В переменную cargo может попасть 12 пробелов и алгоритм будет считать что это loaded_carriage

    label = start + " - " + real_end + ": " + str(number_of_carriages) + ", " + cargo + ", "

    if cargo != '':
        loaded = label + str(math.ceil(distance / 200)) + ' дн.;'
        number_of_carriages1 = number_of_carriages
    else:
        empty = label + str(math.ceil(distance / 330)) + ' дн.;'
        number_of_carriages2 = number_of_carriages

    result = [loaded_df, empty_df]

    path = []
    curr = real_end_id if start_id == end_id else end_id
    while curr != start_id:
        path.append(curr)
        curr = route[curr]
    path.append(curr)

    path = path[::-1]

    cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(start_id))
    point1 = cursor.fetchone()

    if start_id == real_end_id:
        cursor.execute(f"select b from routes where a = {start_id}")
        if cursor.rowcount == 0:
            return result
        end_id = cursor.fetchone()[0]
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(end_id))
        point2 = cursor.fetchone()
        point2, point1 = point1, point2

        result[0] = result[0].append({columns[0]: point1[0] + ", " + point1[1],
                                      columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                      columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                      columns[5]: float(point2[3]), columns[6]: number_of_carriages1,
                                      columns[7]: loaded, columns[8]: ''}, ignore_index=True)

        result[1] = result[1].append({columns[0]: point1[0] + ", " + point1[1],
                                      columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                      columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                      columns[5]: float(point2[3]), columns[6]: number_of_carriages2,
                                      columns[7]: '', columns[8]: empty}, ignore_index=True)

        return result

    if start_id == end_id:
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(path[1]))
        point2 = cursor.fetchone()

        result[0] = result[0].append({columns[0]: point1[0] + ", " + point1[1],
                                      columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                      columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                      columns[5]: float(point2[3]), columns[6]: number_of_carriages1,
                                      columns[7]: loaded, columns[8]: ''}, ignore_index=True)

        result[1] = result[1].append({columns[0]: point1[0] + ", " + point1[1],
                                      columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                      columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                      columns[5]: float(point2[3]), columns[6]: number_of_carriages2,
                                      columns[7]: '', columns[8]: empty}, ignore_index=True)

        return result

    for i in range(len(path) - 1):
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(path[i + 1]))
        point2 = cursor.fetchone()

        if path[i + 1] == end_id:
            result[0] = result[0].append({columns[0]: point1[0] + ", " + point1[1],
                                          columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                          columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                          columns[5]: float(point2[3]), columns[6]: number_of_carriages1,
                                          columns[7]: loaded, columns[8]: ''}, ignore_index=True)

            result[1] = result[1].append({columns[0]: point1[0] + ", " + point1[1],
                                          columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                          columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                          columns[5]: float(point2[3]), columns[6]: number_of_carriages2,
                                          columns[7]: '', columns[8]: empty}, ignore_index=True)


        else:
            result[0] = result[0].append({columns[0]: point1[0] + ", " + point1[1],
                                          columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                          columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                          columns[5]: float(point2[3]), columns[6]: 0,
                                          columns[7]: '', columns[8]: ''}, ignore_index=True)

            result[1] = result[1].append({columns[0]: point1[0] + ", " + point1[1],
                                          columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                          columns[3]: point2[0] + ", " + point2[1], columns[4]: float(point2[2]),
                                          columns[5]: float(point2[3]), columns[6]: 0,
                                          columns[7]: '', columns[8]: ''}, ignore_index=True)

        point1 = point2

    return result


def construct_sample_report(data, cursor):
    data = data.fillna('')
    detailed = data.groupby(['FromStationName', 'ToStationName', 'LastStationName',
                             'CargoEtsngName'], sort=False)['RestDistance'] \
        .describe()[['count', 'mean']].reset_index()
    detailed = detailed.fillna('')

    graph = construct_graph(cursor)

    columns = ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
               'Destination_Longitude',
               'number_of_carriages', 'loaded_carriage', 'empty_carriage']

    loaded_df = pd.DataFrame.from_records([(0, 0, 0, 0, 0, 0, 0, 0, 0)], columns=columns)
    empty_df = pd.DataFrame.from_records([(0, 0, 0, 0, 0, 0, 0, 0, 0)], columns=columns)

    loaded_df = loaded_df[0:0]
    empty_df = empty_df[0:0]

    result = [loaded_df, empty_df]

    iteration = 0
    num_of_routes = len(detailed)
    route = {}
    old_start_id = 0
    counter = 1
    miss_carriages = 0
    for ind, row in detailed.iterrows():
        try:
            start = row[0]
            real_end = row[1]
            end = row[2]
            cargo = row[3]
            number_of_carriages = row[4]
            distance = row[5]

            cursor.execute("select id from stations where name = \'{}\'".format(start))
            counter1 = cursor.rowcount
            cursor.execute("select id from stations where name = \'{}\'".format(end))
            counter2 = cursor.rowcount
            cursor.execute("select id from stations where name = \'{}\'".format(real_end))
            counter3 = cursor.rowcount
            if counter1 != 0 and counter2 != 0 and counter3 != 0:
                cursor.execute(
                    "select id from stations where name = \'{}\'".format(start))
                start_id = cursor.fetchone()[0]

                cursor.execute("select id from stations where name = \'{}\'".format(end))
                end_id = cursor.fetchone()[0]

                cursor.execute("select id from stations where name = \'{}\'".format(real_end))
                real_end_id = cursor.fetchone()[0]

                cursor.execute("SELECT b FROM routes r where r.a = " + str(start_id) + ";")
                tmp1 = cursor.rowcount
                cursor.execute("SELECT b FROM routes r where r.a = " + str(end_id) + ";")
                tmp2 = cursor.rowcount

                if tmp1 != 0 and tmp2 != 0:
                    if bfs(graph, start_id, end_id):
                        if start_id != old_start_id:
                            route = dijikstra_graph(graph, start_id, end_id)
                            result = add_route(route, start_id, end_id, number_of_carriages,
                                               cursor, result[0], result[1],
                                               real_end_id, start, real_end, cargo, columns, distance)
                            old_start_id = start_id
                            counter += 1
                        else:
                            result = add_route(route, old_start_id, end_id, number_of_carriages,
                                               cursor, result[0], result[1],
                                               real_end_id, start, real_end, cargo, columns, distance)

                    else:
                        raise Exception

                else:
                    raise Exception

                iteration += 1


            else:
                raise Exception

        except Exception:
            iteration += 1
            print("{} из {} маршрутов не был обработан".format(iteration, num_of_routes))
            print("Маршрут {} - {}: {} : {} не был обработан".format(start, end, real_end, number_of_carriages))
            conn = psycopg2.connect(dbname='flow_map', user='postgres',
                                    password='root', host='localhost')
            cursor = conn.cursor()
            miss_carriages += number_of_carriages

            continue

    print(miss_carriages)

    return result


def construct_report_by_route(data, cursor, route, state, dt, result):
    if len(data) == 0:
        return result
    tmp = construct_sample_report(data, cursor)

    loaded = tmp[0]

    loaded = loaded.fillna('').groupby(
        ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
         'Destination_Longitude']).agg(
        {'number_of_carriages': 'sum', 'loaded_carriage': ''.join, 'empty_carriage': ''.join})

    loaded["Color"] = 0
    loaded["Width"] = 5
    loaded["carriage_state"] = 'Груженый'
    loaded["cargo"] = 'Все'

    empty = tmp[1]
    empty = empty.fillna('').groupby(
        ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
         'Destination_Longitude']).agg(
        {'number_of_carriages': 'sum', 'loaded_carriage': ''.join, 'empty_carriage': ''.join})

    empty["Color"] = 0
    empty["Width"] = 5
    empty["carriage_state"] = 'Порожний'
    empty["cargo"] = 'Все'

    all = pd.concat([loaded, empty], axis=0)

    all = all.fillna('').groupby(
        ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
         'Destination_Longitude']).agg(
        {'number_of_carriages': 'sum', 'loaded_carriage': ''.join, 'empty_carriage': ''.join})

    all["Color"] = 0
    all["Width"] = 5
    all["carriage_state"] = 'Все'
    all["cargo"] = 'Все'

    if (int(sum(loaded['number_of_carriages'])) != 0):
        for cargo in data["CargoEtsngName"].unique():
            try:
                if len(str(cargo).strip()) != 0:
                    df = data.loc[data['CargoEtsngName'] == cargo]

                    tmp = construct_sample_report(df, cursor)
                    tmp_loaded = tmp[0]

                    tmp_loaded = tmp_loaded.fillna('').groupby(
                        ['Origin', 'Origin_Latitude', 'Origin_Longitude', 'Destination', 'Destination_Latitude',
                         'Destination_Longitude']).agg(
                        {'number_of_carriages': 'sum', 'loaded_carriage': ''.join, 'empty_carriage': ''.join})

                    tmp_loaded["Color"] = 0
                    tmp_loaded["Width"] = 5
                    tmp_loaded["carriage_state"] = 'Груженый'
                    tmp_loaded["cargo"] = cargo

                    loaded = pd.concat([loaded, tmp_loaded], axis=0)

            except Exception:
                conn = psycopg2.connect(dbname='flow_map', user='postgres',
                                        password='root', host='localhost')
                cursor = conn.cursor()
                print("Something went wrong with cargo ")
                continue

    for row in range(len(loaded)):
        if loaded["number_of_carriages"][row] != 0:
            loaded["Color"][row] = 100
            loaded["Width"][row] = 20

    for row in range(len(empty)):
        if empty["number_of_carriages"][row] != 0:
            empty["Color"][row] = 100
            empty["Width"][row] = 20
        if all["number_of_carriages"][row] != 0:
            if all["loaded_carriage"][row].strip() == "":
                all["Color"][row] = 100
            else:
                all["Color"][row] = 300

            all["Width"][row] = 20

    tmp = pd.concat([loaded, empty], axis=0)

    loaded = pd.concat([tmp, all], axis=0)

    loaded["route"] = route
    loaded["update_datetime"] = dt

    without_route = loaded[loaded["number_of_carriages"] != 0]

    without_route["with_route"] = "Без маршрута"
    loaded["with_route"] = "С маршрутом"

    loaded = pd.concat([without_route, loaded], axis=0)

    if result is None:
        result = loaded
    else:
        result = pd.concat([result, loaded], axis=0)

    return result


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


def construct_report(conn, cursor):
    # server = '3.10.162.120,1433'
    # database = 'AZR'
    # username = 'AnalyticsUser'
    # password = 'WNOylkgb6F2ZudrCs3tU'
    #
    # engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=FreeTds')
    #
    # sh = engine.execute(
    #     "SELECT * FROM Local.CarLocation where fromstationname IS NOT NULL AND tostationname IS NOT NULL AND laststationname IS NOT NULL AND cargoweight IS NOT NULL")
    # dt = datetime.datetime.now()
    # hours_added = datetime.timedelta(hours=17)
    # mins = datetime.timedelta(minutes=dt.minute, seconds=dt.second)
    # dt = dt + hours_added - mins
    dt = datetime.datetime(2021, 6, 20, 14, 0)
    dt = dt.strftime("%Y-%m-%d %H:%M")
    #data = pd.DataFrame(data=sh, columns=sh.keys())

    pre = os.path.dirname(os.path.realpath(__file__))
    path = os.path.join(pre, "Dislocation.xls")
    data = pd.read_excel(path)
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

            }
    data.rename(columns=cols, inplace=True)

    data = data.loc[0:, ["CarNumber", 'FromStationName', 'ToStationName', 'LastStationName',
                     'CargoEtsngName', 'RestDistance', 'ShippingDate', "LastOperationDate",
                         "LastOperationName", "IdleOnTheLastStation", "OwnerName", "CarStateName",
                         "GroupName"]]
    data["CargoEtsngName"] = data["CargoEtsngName"].fillna('')
    data["ShippingDate"] = data["ShippingDate"].replace({pd.NaT: None})
    data["LastOperationDate"] = data["LastOperationDate"].replace({pd.NaT: None})

    data["update_datetime"] = dt
    #data['RestRun'] = data['RestRun'].fillna(0)
    commit_to_db(conn, cursor, "dislocation", data)

    result = None

    all_stations = ['Усть-Таловка', 'Неверовская', 'Балхаш I', 'Бозшаколь', 'Актогай']


    stations = [
        ['Усть-Таловка', 'Неверовская'],
        ['Балхаш I'],
        ['Бозшаколь'],
        ['Актогай'],
        ['Балхаш I', 'Достык'],
        ['Бозшаколь', 'Достык']
    ]

    routes = [
        'ВЦМ',
        'БМЗ',
        'КБЛ',
        'КАЛ',
        'Балхаш I - Достык',
        'Бозшаколь - Достык'
    ]

    result = construct_report_by_route(data, cursor, 'Общая карта', 0, dt, result)

    for i in range(len(stations)):
        df = data

        if i < 4:
            df = data.loc[data['FromStationName'].isin(stations[i]) | data['ToStationName'].isin(stations[i])]

        else:
            df = data.loc[data['FromStationName'].isin(stations[i]) & data['ToStationName'].isin(stations[i])]

        df = df.loc[0:, ['FromStationName', 'ToStationName', 'LastStationName',
                         'CargoEtsngName', 'RestDistance']]

        result = construct_report_by_route(df, cursor, routes[i], 1, dt, result)

        print()
        print("Маршрут " + routes[i] + " был обработан")
        print()

    result = result.reset_index()
    commit_to_db(conn, cursor, "report", result)


def delete_trash(cursor, conn):
    dt = datetime.datetime.now()
    cursor.execute(f"delete "
                   f"from report "
                   f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00' "
                   f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00';")
    cursor.execute(f"delete "
                   f"from dislocation "
                   f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00' "
                   f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00';")

    conn.commit()