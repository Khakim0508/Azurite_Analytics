import pyodbc
import pandas as pd
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = '3.10.162.120,1433'
database = 'AZR'
username = 'AnalyticsUser'
password = 'WNOylkgb6F2ZudrCs3tU'
cnxn = pyodbc.connect('DRIVER={FreeTDS};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'CarLocation'"
               " ORDER BY ORDINAL_POSITION")
sh = cursor.fetchall()

cursor.execute("SELECT * from Local.CarLocation where "
               "CargoEtsngName <> ''")
det = cursor.fetchone()

for i in range(len(sh)):
    print(sh[i][0] + ": " + str(det[i]) + " "
          + str(type(det[i])))

