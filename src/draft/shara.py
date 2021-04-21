import pandas as pd
import psycopg2
import datetime

def delete_trash():
    dt = datetime.datetime.now()

    print(f"delete "
                   f"from report "
                   f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00' "
                   f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00';")
    print(f"delete "
                   f"from dislocation "
                   f"where update_datetime >= timestamp '{dt.year}-{dt.month}-{dt.day} 00:00:00' "
                   f"and update_datetime < timestamp '{dt.year}-{dt.month}-{dt.day} 16:30:00'; ")

delete_trash()