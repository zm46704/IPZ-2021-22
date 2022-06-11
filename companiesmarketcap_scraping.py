import requests as req
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc

server = 'ipzazuresql.database.windows.net'
database = 'ipz2db'
username = 'osuch'
password = 'IPZ_2022_sem_letni'
conn = pyodbc.connect(
'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = conn.cursor()

car_makers = pd.read_sql('SELECT * FROM Manufacturers', conn)

# wyznaczenie akltualnej daty
today = datetime.now().date()

# funkcja usuwajaca tagi html
def striphtml(data):
p = re.compile(r'<.*?>')
return p.sub('', data)

ids = list()
rank = list()
date = list()
marketcap = list()
share_price = list()
change_day = list()
change_year = list()

lists = [rank, marketcap, share_price, change_day, change_year]
for i in range(len(car_makers)):
# tworze link do podstrony o danym automakerze na serwisie companiemarketcap.com
# nazwy odpowiednich podstron znajdują się a pliku car_makers_update2.xlsx
link = "https://companiesmarketcap.com/" + str(car_makers["companiesmarketcap"][i]) + "/marketcap/"
webpage = req.get(link)
soup = BeautifulSoup(webpage.content, "html.parser")  # "lxml", "html5lib"
# pobieram wiersze z tej strony gdzie sa dane: marketcap, share price, Changen, rank
values = soup.find_all(attrs={'class': 'line1'})
headers = soup.find_all(attrs={'class': 'line2'})
# wyswietlam te dane (domyslnie dodawane bedzie do dataframe)
# print(car_makers["manufacturer"][i])
ids.append(i + 1)
date.append(today)

del values[2]
del values[5]
del headers[2]
del headers[5]

for i in range(len(values)):
    val = striphtml(str(values[i]))
    # head = striphtml(str(headers[i]))
    # print(head, ":\t", val)
    lists[i].append(val)
# print()

# tworzenie dataframu z list
data = {'manu_id': ids, 'date': date, 'rank_': rank, 'marketcap': marketcap, 'share_price': share_price,
    'change_day': change_day, 'change_year': change_year}
df = pd.DataFrame(data)

# formatowanie kolumn
df['rank_'] = df['rank_'].apply(lambda val: val[1:])
df['marketcap'] = df['marketcap'].apply(lambda val: val[1:])
df['share_price'] = df['share_price'].apply(lambda val: val[1:])
df['change_day'] = df['change_day'].apply(lambda val: val[:-1])
df['change_year'] = df['change_year'].apply(lambda val: val[:-1])
df.loc[df['change_year'] == "N/", "change_year"] = ''

# print(df)

for index, row in df.iterrows():
try:
    marketcap = row.marketcap
    if marketcap[-1] == 'B':
        marketcap = float(marketcap[0:-2]) * 10 ** 9
    elif marketcap[-1] == 'T':
        marketcap = float(marketcap[0:-2]) * 10 ** 12

    cursor.execute(
        "INSERT INTO CompaniesMarketCapData (manu_id, date_, rank_, marketcap, share_price, change_day, change_year) values(?,?,?,?,?,?,?)",
        row.manu_id, row.date, row.rank_, marketcap, row.share_price, row.change_day, row.change_year)
except ValueError:
    print(row)
    continue
conn.commit()
cursor.close()