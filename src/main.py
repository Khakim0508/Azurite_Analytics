import psycopg2
from src.algorithms.construct_reports import construct_report

if __name__ == "__main__":
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()


    file_name = "dislocation/Dislocation_2021-02-02_08-00-01_15.xls"

    construct_report(file_name, cursor)
