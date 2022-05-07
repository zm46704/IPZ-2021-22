import requests as req
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

#wyznaczenie akltualnej daty
today = datetime.now().date()


#funkcja usuwajaca tagi html
def striphtml(data):
    p = re.compile(r'<.*?>')
    return p.sub('', data)

# plik z info o producentach aut
car_makers = pd.read_excel('../car_makers_update2.xlsx')

ids = list()
rank = list()
date = list()
marketcap = list()
share_price = list()
change_day = list()
change_year = list()

lists = [rank, marketcap, share_price, change_day, change_year]
for i in range(len(car_makers)):
    #tworze link do podstrony o danym automakerze na serwisie companiemarketcap.com
    #nazwy odpowiednich podstron znajdują się a pliku car_makers_update2.xlsx
    link = "https://companiesmarketcap.com/" + str(car_makers["companiesmarketcap"][i]) + "/marketcap/"
    webpage = req.get(link)
    soup = BeautifulSoup(webpage.content, "html.parser")  # "lxml", "html5lib"
    #pobieram wiersze z tej strony gdzie sa dane: marketcap, share price, Changen, rank
    values = soup.find_all(attrs={'class': 'line1'})
    headers = soup.find_all(attrs={'class': 'line2'})
    #wyswietlam te dane (domyslnie dodawane bedzie do dataframe)
    #print(car_makers["manufacturer"][i])
    ids.append(i+1)
    date.append(today)

    del values[2]
    del values[5]
    del headers[2]
    del headers[5]

    for i in range(len(values)):
        val = striphtml(str(values[i]))
        #head = striphtml(str(headers[i]))
        #print(head, ":\t", val)
        lists[i].append(val)
    #print()

# tworzenie dataframu z list
today = datetime.now().date()
data = {'manu_id': ids, 'date': date, 'rank': rank, 'marketcap ($)': marketcap, 'share_price ($)': share_price, 'change_day (%)': change_day, 'change_year (%)': change_year }
df = pd.DataFrame(data)
#print(df)

#formatowanie kolumn
df['rank'] = df['rank'].apply(lambda val: val[1:])
df['marketcap ($)'] = df['marketcap ($)'].apply(lambda val: val[1:])
df['share_price ($)'] = df['share_price ($)'].apply(lambda val: val[1:])
df['change_day (%)'] = df['change_day (%)'].apply(lambda val: val[:-1])
df['change_year (%)'] = df['change_year (%)'].apply(lambda val: val[:-1])
df.loc[df['change_year (%)'] == "N/", "change_year (%)"] = ''

print(df)

df.to_csv('automakers_webscraping_24_04.csv')

