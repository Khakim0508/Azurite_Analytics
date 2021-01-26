import math

import pandas as pd

from src.algorithms.graph_algorithms import bfs
from src.algorithms.graph_algorithms import construct_graph
from src.algorithms.graph_algorithms import dijikstra_graph


def add_route(route, start_id, end_id, number_of_carrieges, cursor, loaded_df,
              empty_df, real_end_id, start, real_end, cargo, columns, distance):
    loaded = ''
    empty = ''
    number_of_carriages1 = number_of_carriages2 = 0

    number_of_carrieges = int(number_of_carrieges)
    # В метод попадает число вагонов представленных в виде числа с плавающей точкой

    cargo = cargo.strip()
    # В переменную груза может попасть 12 пробелов и алгоритм будет считать что это груженые вагоны

    label = start + " - " + real_end + ": " + str(number_of_carrieges) + ", " + cargo + ", "

    if cargo != '':
        loaded = label + str(math.ceil(distance / 200)) + ' дн.;'
        number_of_carriages1 = number_of_carrieges
    else:
        empty = label + str(math.ceil(distance / 330)) + ' дн.;'
        number_of_carriages2 = number_of_carrieges

    result = [loaded_df, empty_df]

    tmp = real_end_id if start_id == end_id else end_id

    path = []
    curr = tmp
    while curr != start_id:
        path.append(curr)
        curr = route[curr]
    path.append(curr)

    path = path[::-1]

    cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(start_id))
    point1 = cursor.fetchone()

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
    detailed = data.groupby(['Станция отправления', 'Станция назначения', 'Станция текущей дислокации',
                             'Груз'], sort=False)['Расстояние осталось (от текущей станции)'] \
        .describe()[['count', 'mean']].reset_index()
    detailed = detailed.fillna('')

    graph = construct_graph(cursor)

    detailed.to_excel("output_files/Detailed.xlsx")
    columns = ['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude', 'Dest_Longitude',
               'Кол-во вагонов', 'ГРУЖ', 'ПОРОЖ']

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
    miss_carrieges = 0
    for ind, row in detailed.iterrows():
        try:
            start = row[0]
            real_end = row[1]
            end = row[2]
            cargo = row[3]
            number_of_carrieges = row[4]
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
                            result = add_route(route, start_id, end_id, number_of_carrieges,
                                               cursor, result[0], result[1],
                                               real_end_id, start, real_end, cargo, columns, distance)
                            old_start_id = start_id
                            print("А маршрут то поменялся " + str(counter) + " раз")
                            counter += 1
                        else:
                            result = add_route(route, old_start_id, end_id, number_of_carrieges,
                                               cursor, result[0], result[1],
                                               real_end_id, start, real_end, cargo, columns, distance)

                    else:
                        raise Exception

                else:
                    raise Exception

                iteration += 1
                print("{} из {} маршрутов было обработано".format(iteration, num_of_routes))

            else:
                raise Exception

        except Exception:
            iteration += 1
            print("{} из {} маршрутов не был обработан".format(iteration, num_of_routes))
            print("Маршрут {} - {}: {} : {} не был обработан".format(start, end, real_end, number_of_carrieges))
            miss_carrieges += number_of_carrieges

            continue

    print(miss_carrieges)

    return result


def construct_report_by_route(data, cursor, file_name):
    tmp = construct_sample_report(data, cursor)
    loaded = tmp[0]

    loaded = loaded.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                        'Dest_Longitude']).agg(
        {'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})

    loaded["Color"] = 0
    loaded["Width"] = 5
    loaded["Состояние"] = 'Груженый'
    loaded["Груз"] = 'Все'

    empty = tmp[1]

    empty = empty.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                      'Dest_Longitude']).agg(
        {'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})
    #
    empty["Color"] = 0
    empty["Width"] = 5
    empty["Состояние"] = 'Порожний'
    empty["Груз"] = 'Все'

    all = pd.concat([loaded, empty])

    all = all.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                  'Dest_Longitude']).agg({'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})

    all["Color"] = 0
    all["Width"] = 5
    all["Состояние"] = 'Все'
    all["Груз"] = 'Все'

    if (int(sum(loaded['Кол-во вагонов'])) != 0):
        for cargo in data["Груз"].unique():
            try:
                if len(str(cargo).strip()) != 0:
                    df = data.loc[data['Груз'] == cargo]

                    tmp = construct_sample_report(df, cursor)
                    tmp_loaded = tmp[0]

                    tmp_loaded["Color"] = 0
                    tmp_loaded["Width"] = 5
                    tmp_loaded["Состояние"] = 'Груженый'
                    tmp_loaded["Груз"] = str(cargo).strip()

                    loaded = pd.concat([loaded.reset_index(), tmp_loaded], ignore_index=True)


            except KeyError:
                print("Something went wrong with cargo ")
                continue
            except Exception:
                print("Something went wrong with cargo ")
                continue

    for row in range(len(loaded)):
        if loaded["Кол-во вагонов"][row] != 0:
            loaded["Color"][row] = 100
            loaded["Width"][row] = 20

    for row in range(len(empty)):
        if empty["Кол-во вагонов"][row] != 0:
            empty["Color"][row] = 100
            empty["Width"][row] = 20
        if all["Кол-во вагонов"][row] != 0:
            all["Color"][row] = 100
            all["Width"][row] = 20

    if (int(sum(loaded['Кол-во вагонов'])) != 0):
        tmp = pd.concat([loaded, empty.reset_index()])
        loaded = pd.concat([tmp, all.reset_index()])

    else:
        tmp = pd.concat([loaded.reset_index(), empty.reset_index()])
        loaded = pd.concat([tmp.reset_index(), all.reset_index()])

    loaded.reset_index().to_excel("output_files/" + file_name + ".xlsx")


def construct_report(file_name, cursor):
    data = pd.read_excel(file_name)
    stations = [
        ['Актогай', 'Балхаш I'],
        ['Балхаш I', 'Достык', 'Достык (эксп.)'],
        ['Бозшаколь', 'Достык', 'Достык (эксп.)'],
        ['Бозшаколь', 'Балхаш I'],
        ['Бозшаколь', 'Ахангаран'],
        ['Бозшаколь', 'Ежевая']
    ]

    routes = [
        'Актогай - Балхаш',
        'Балхаш - Достык',
        'Бозшаколь - Достык',
        'Бозшаколь - Балхаш',
        'Бозшаколь - Ахангаран',
        'Бозшаколь - Ежевая'
    ]

    #construct_report_by_route(data, cursor, 'Общая карта')

    len(stations)

    for i in range(6):
        df = data.loc[data['Станция отправления'].isin(stations[i]) & data['Станция назначения'].isin(stations[i])]
        df = df.loc[0:, ['Станция отправления', 'Станция назначения', 'Станция текущей дислокации',
                         'Груз', 'Расстояние осталось (от текущей станции)']]

        construct_report_by_route(df, cursor, routes[i])
        print()
        print("Маршрут " + routes[i] + " был обработан")
        print()
