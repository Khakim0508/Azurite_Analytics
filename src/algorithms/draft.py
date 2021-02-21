import pandas as pd
import datetime

from src.algorithms.graph_algorithms import bfs
from src.algorithms.graph_algorithms import construct_graph
from src.algorithms.graph_algorithms import dijikstra_graph


def add_route(route, start_id, end_id, cursor, report, real_end_id, columns, det_id, disloc,
              start, end, real_end):
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
        cursor.execute("select route_id from routes where a = {} and b = {}".format(path[0], path[1]))
        route_id = cursor.fetchone()[0]
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(path[1]))
        point2 = cursor.fetchone()


        report = report.append({columns[0]: point1[0],
                                    columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                    columns[3]: point2[0], columns[4]: float(point2[2]),
                                    columns[5]: float(point2[3]),
                                    columns[6]: route_id}, ignore_index=True)
        disloc["span_id"].loc[(disloc['Станция отправления'] == start) &
                              (disloc['Станция назначения'] == real_end) &
                              (disloc['Станция текущей дислокации'] == end)] = route_id

        return [report, disloc]

    for i in range(len(path) - 1):
        cursor.execute("select name, country, latitude, longitude from stations where id = {}".format(path[i + 1]))
        point2 = cursor.fetchone()

        if path[i + 1] == end_id:
            cursor.execute("select route_id from routes where a = {} and b = {}".format(path[i], path[i + 1]))
            route_id = cursor.fetchone()[0]

            report = report.append({columns[0]: point1[0],
                                        columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                        columns[3]: point2[0], columns[4]: float(point2[2]),
                                        columns[5]: float(point2[3]),
                                        columns[6]: route_id}, ignore_index=True)

            disloc["span_id"].loc[(disloc['Станция отправления'] == start) &
                                  (disloc['Станция назначения'] == real_end) &
                                  (disloc['Станция текущей дислокации'] == end)] = route_id

        else:
            cursor.execute("select route_id from routes where a = {} and b = {}".format(path[i], path[i + 1]))
            route_id = cursor.fetchone()[0]

            report = report.append({columns[0]: point1[0],
                                        columns[1]: float(point1[2]), columns[2]: float(point1[3]),
                                        columns[3]: point2[0], columns[4]: float(point2[2]),
                                        columns[5]: float(point2[3]),
                                        columns[6]: route_id}, ignore_index=True)

        point1 = point2

    return [report, disloc]


def construct_sample_report(dislocation, cursor):
    dislocation = dislocation.fillna('')
    detailed = dislocation.groupby(['Станция отправления', 'Станция назначения', 'Станция текущей дислокации'],
                                   sort=False)['Расстояние осталось (от текущей станции)'] \
        .describe()[['count']].reset_index()
    detailed = detailed.fillna('')

    graph = construct_graph(cursor)

    detailed.to_excel("new_report/Detailed.xlsx")
    columns = ['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination',
               'Dest_Latitude', 'Dest_Longitude', 'route_id']

    report = pd.DataFrame(columns=columns)
    dislocation['span_id'] = -1

    result = [report, dislocation]

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
            carriages = row[3]

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
                            result = add_route(route, start_id, end_id,
                                               cursor, result[0], real_end_id, columns, ind, result[1],
                                               start, end, real_end)
                            old_start_id = start_id
                            print("А маршрут то поменялся " + str(counter) + " раз")
                            counter += 1
                        else:
                            result = add_route(route, start_id, end_id,
                                               cursor, result[0], real_end_id, columns, ind, result[1],
                                               start, end, real_end)
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
            print("Маршрут {} - {}: {}  не был обработан".format(start, end, real_end, carriages))
            miss_carriages += carriages

            continue

    print("Number of missed carriages: " + str(miss_carriages))

    return result


def construct_report_by_route(dislocation, cursor, file_name, dt):
    tmp = construct_sample_report(dislocation, cursor)

    report = tmp[0]
    dislocation = tmp[1]
    report = report.groupby(['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination',
                            'Dest_Latitude', 'Dest_Longitude', 'route_id']).size()\
                            .reset_index().rename(columns={0: 'count'})

    report = report[['Origin', 'Orig_Latitude', 'Orig_Longitude', 'Destination',
                     'Dest_Latitude', 'Dest_Longitude', 'route_id']]

    report["Date"] = dt
    dislocation["Date"] = dt

    writer = pd.ExcelWriter("new_report/Dislocation.xlsx",
                            engine='xlsxwriter',
                            datetime_format='d/m/yyyy hh:mm',
                            date_format='d/m/yyyy')

    # Convert the dataframe to an XlsxWriter Excel object.
    dislocation.to_excel(writer, sheet_name='Дислокация')

    workbook = writer.book
    worksheet = writer.sheets['Дислокация']

    worksheet.set_column('B:AN', 20)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    report.reset_index().to_excel("new_report/" + file_name + ".xlsx", sheet_name='Карта')


def construct_report(file_name, cursor):
    data = pd.read_excel(file_name)
    dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    construct_report_by_route(data, cursor, 'Общая карта', dt)
