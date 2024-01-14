import sqlite3
from bs4 import BeautifulSoup
import requests
import time
import numpy as np

# スクレイピング
weather_info_tmp = []
for i in range(1,13):
    url = f'https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php?prec_no=46&block_no=47670&year=2023&month={i}&day=1&view=p1'
    response = requests.get(url)
    response.encoding = 'utf-8'
    text = response.text
    soup = BeautifulSoup(text, 'html.parser')
    find = soup.find_all('td', class_='data_0_0')
    tmp = []
    for f in find:
        text = f.get_text()
        tmp.append(text)
    weather_info_tmp.append(tmp)
    time.sleep(0.5)

# データの整形
weather_info = []
for i in range(len(weather_info_tmp)):
    numpy_data = np.array(weather_info_tmp[i])
    reshaped_data = numpy_data.reshape(-1, 20)
    cleaned_data = np.where((reshaped_data == "--") | (reshaped_data == "-- )"), np.nan, reshaped_data)
    for j in range(len(cleaned_data)):
        data = np.append(cleaned_data[j], [i + 1])
        data = np.append(data, [j + 1])
        tup = tuple(data)
        weather_info.append(tup)

# dbに保存
path = "./db/"
db_name = "課題.sqlite"
con = sqlite3.connect(path + db_name)
cur = con.cursor()

create_table_weather = """
CREATE TABLE weather(
    現地気圧 REAL,
    海面気圧 REAL,
    降水合計 REAL,
    一時間降水 REAL,
    十分降水 REAL,
    平均気温 REAL,
    最高気温 REAL,
    最低気温 REAL,
    平均湿度 REAL,
    最小湿度 REAL,
    平均風速 REAL,
    最大風速 REAL,
    風向 TEXT,
    最大瞬間風速 REAL,
    最大瞬間風向 TEXT,
    日照時間 REAL,
    降雪 REAL,
    積雪 REAL,
    昼 TEXT,
    夜 TEXT,
    月 REAL,
    日 REAL
    )
"""
drop_table_weather = "DROP TABLE IF EXISTS weather"
cur.execute(drop_table_weather)
cur.execute(create_table_weather)

sql_insert_many = "INSERT INTO weather VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
cur.executemany(sql_insert_many, weather_info)
con.commit()
con.close()