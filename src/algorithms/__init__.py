from sqlalchemy import create_engine


server = '3.10.162.120,1433'
database = 'AZR'
username = 'AnalyticsUser'
password = 'WNOylkgb6F2ZudrCs3tU'

engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=FreeTds')
sh = engine.execute(
         "SELECT * FROM Local.CarLocation where fromstationname IS NOT NULL AND tostationname IS NOT NULL AND laststationname IS NOT NULL AND cargoweight IS NOT NULL")
