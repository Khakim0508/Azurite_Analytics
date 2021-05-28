import psycopg2

from algorithms.construct_reports2 import construct_report

if __name__ == "__main__":
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()

    construct_report(conn, cursor)
