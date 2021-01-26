import pandas as pd

df = pd.read_excel("пятница 04.12.xls")

detailed = df.groupby(['Станция отправления', 'Станция назначения', 'Станция текущей дислокации',
                             'Груз'], sort=False)['Расстояние осталось (от текущей станции)']\
                            .describe()[['count', 'mean']].reset_index()

print(int(sum(detailed["count"])))