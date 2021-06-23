import os
import pandas as pd

cols = {"Номер вагона": "CarNumber",
        "Станция отправления": 'FromStationName',
        "Станция назначения": 'ToStationName',
        "Станция текущей дислокации": 'LastStationName',
        "Груз": 'CargoEtsngName',
        "Расстояние осталось (от текущей станции)": 'RestDistance',
        "Дата и время отправления": "ShippingDate",
        "Дата и время последней операции": "LastOperationDate",
        "Остаточный пробег": "RestRun",
        "Операция (полное наименование)": "LastOperationName",
        "Простой на станции дислокации": "IdleOnTheLastStation",
        "Собственник (по данным ЭТРАН, ГВЦ)": "OwnerName",
        "Состояние вагона": "CarStateName",
        "Группа": "GroupName",
        "Вес груза, тонны": "CargoWeight",
        "код ст. отправления (6 знаков)": "FromStationCode",
        "код ст. назначения (6 знаков)": "ToStationCode",
        "Станция текущей дислокации, код": "LastStationCode",
        "Груз, код": "CargoEtsngCode",
        "Индекс поезда": "TrainIndex",
        "НРП (вагон в нерабочем парке)": "RepairCurrent_isNRP",
        "Неисправность текущая": "CarFaultinessName",
        "Грузоподъемность, тн": "CarCapacity",
        "Тип вагона": "CarTypeShortName",
        "Номер поезда": "InvNumber",
    }


pd.set_option("display.max_rows", None, "display.max_columns", None)

pre = os.path.dirname(os.path.realpath(__file__))
path = os.path.join(pre, "Dislocation.xls")
data = pd.read_excel(path)


data.rename(columns=cols, inplace=True)


print(data.head())