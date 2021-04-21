import psycopg2
import schedule
import time
import datetime
from algorithms.construct_reports import construct_report
from algorithms.construct_reports import delete_trash
def job():
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    construct_report(conn, cursor)


def job1():
    conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
    cursor = conn.cursor()
    delete_trash(cursor, conn)

if __name__ == "__main__":
    schedule.every().day.at("01:45").do(job)
    schedule.every().day.at("04:45").do(job)
    schedule.every().day.at("07:45").do(job)
    schedule.every().day.at("10:45").do(job)
    schedule.every().day.at("15:45").do(job1)


    while 1:
        schedule.run_pending()
        time.sleep(1)

