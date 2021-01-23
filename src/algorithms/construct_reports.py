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
                             'Груз'], sort=False)['Расстояние осталось (от текущей станции)']\
                            .describe()[['count', 'mean']].reset_index()

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

            iteration += 1
            print("{} из {} маршрутов было обработано".format(iteration, num_of_routes))
        except Exception:
            iteration += 1
            print("{} из {} маршрутов не был обработан".format(iteration, num_of_routes))

            continue

    return result


def construct_report_by_route(data, cursor, file_name):
    tmp = construct_sample_report(data, cursor)

    res = tmp[0]

    res = res.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                  'Dest_Longitude']).agg({'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})
    #
    res["Color"] = 0
    res["Width"] = 5
    res["Состояние"] = 'ГРУЖ'

    res2 = tmp[1]

    res2 = res2.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                    'Dest_Longitude']).agg({'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})
    #
    res2["Color"] = 0
    res2["Width"] = 5
    res2["Состояние"] = 'ПОРОЖ'

    res3 = pd.concat([res, res2]).reset_index()


    res3 = res3.fillna('').groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination', 'Dest_Latitude',
                                    'Dest_Longitude']).agg({'Кол-во вагонов': 'sum', 'ГРУЖ': ''.join, 'ПОРОЖ': ''.join})
    #
    res3["Color"] = 0
    res3["Width"] = 5
    res3["Состояние"] = 'ГРУЖ/ПОРОЖ'

    for row in range(len(res)):
        if res["Кол-во вагонов"][row] != 0:
            res["Color"][row] = 100
            res["Width"][row] = 20
        if res2["Кол-во вагонов"][row] != 0:
            res2["Color"][row] = 50
            res2["Width"][row] = 10
        if res3["Кол-во вагонов"][row] != 0:
            res3["Color"][row] = 100
            res3["Width"][row] = 20

    tmp = pd.concat([res, res2])
    res = pd.concat([tmp, res3])

    res.reset_index().to_excel("output_files/" + file_name +".xlsx")


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

    construct_report_by_route(data, cursor, 'Общая карта')

    for i in range(len(stations)):
        df = data.loc[data['Станция отправления'].isin(stations[i]) & data['Станция назначения'].isin(stations[i])]
        df = df.loc[0:, ['Станция отправления', 'Станция назначения', 'Станция текущей дислокации',
                         'Груз', 'Расстояние осталось (от текущей станции)']]


        construct_report_by_route(df, cursor, routes[i])


