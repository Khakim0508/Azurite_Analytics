import pandas as pd
import psycopg2

conn = psycopg2.connect(dbname='flow_map', user='postgres',
                            password='root', host='localhost')
cursor = conn.cursor()

df = pd.read_excel("Dislocation_2021-02-02_08-00-01_15.xls", converters={'Дата и время отправления': pd.to_datetime})
# df["span_id"] = 0
# df["span_id"].loc[(df['Станция отправления'] == "Бозшаколь") & (df['Станция назначения'] == "Достык")] = 1
# df["Дата и время отправления"] = str(df["Дата и время отправления"])

writer = pd.ExcelWriter("check.xlsx",
                        engine='xlsxwriter',
                        datetime_format='d/m/yyyy hh:mm',
                        date_format='d/m/yyyy')

# Convert the dataframe to an XlsxWriter Excel object.
df.to_excel(writer, sheet_name='Sheep1')

workbook = writer.book
worksheet = writer.sheets['Sheep1']

worksheet.set_column('B:AD', 20)

# Close the Pandas Excel writer and output the Excel file.
writer.save()