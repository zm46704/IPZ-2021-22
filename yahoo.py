import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc

day_number = datetime.today().weekday()

if day_number == 6 or day_number == 0:
    exit(0)

# Połączenie z bazą danych
server = 'ipzazuresql.database.windows.net'
database = 'ipz2db'
username = 'osuch'
password = 'IPZ_2022_sem_letni'
conn = pyodbc.connect(
'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = conn.cursor()

# Pobranie danych o prodycentach
car_makers = pd.read_sql('SELECT * FROM Manufacturers', conn)
#print(car_makers)

# pobieranie danych przez API
for i, sm_name in enumerate(car_makers["sm_name"]):
    #print(i + 1)
    sm_data = yf.download(sm_name,
                          period='1d',  # z obecnego dnia
                          progress=False,
                          interval='1d')  # sumarczyne dla całego dnia
    # dodawanie kolumny z id producenta
    id = np.ones((len(sm_data), 1), dtype=int) * (i + 1)
    sm_data.insert(0, "manu_id", id)
    # print(sm_data)
    if i == 0:
        data = sm_data
    else:
        data = data.append(sm_data)

# print(data)

for index, row in data.iterrows():
    try:
        date = str(index)[0:10]
        cursor.execute(
            "INSERT INTO Yfinance (manu_id, date_, open_, high_, low_, close_, adj_close, volume) values(?,?,?,?,?,?,?,?)",
            int(row.manu_id), date, row.Open, row.High, row.Low, row.Close, row['Adj Close'], int(row.Volume))
        i += 1
    except ValueError:
        print(row)
        continue
    conn.commit()
cursor.close()