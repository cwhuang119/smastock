import sqlite3
import requests
import pandas as pd
from io import StringIO
import datetime

def execute_manay(db_path,sql_cmd,insert_values):
    with sqlite3.connect(db_path) as con:
        cursor = con.cursor()
        cursor.executemany(sql_cmd,insert_values)
        con.commit()
import time
from datetime import timedelta, date
def get_day_range(start_date,end_date):

    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    day_range = []
    for single_date in daterange(start_date, end_date):
        day_range.append(single_date)
    return day_range


def crawl_price(date):
    
    # 將 date 變成字串 舉例：'20180525' 
    datestr = date.strftime('%Y%m%d')
    
    # 從網站上依照 datestr 將指定日期的股價抓下來
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALLBUT0999')
    
    # 將抓下來的資料（r.text），其中的等號給刪除
    content = r.text.replace('=', '')
    
    # 將 column 數量小於等於 10 的行數都刪除
    lines = content.split('\n')
    lines = list(filter(lambda l:len(l.split('",')) > 10, lines))
    
    # 將每一行再合成同一行，並用肉眼看不到的換行符號'\n'分開
    content = "\n".join(lines)
    
    # 假如沒下載到，則回傳None（代表抓不到資料）
    if content == '':
        return None
    
    # 將content變成檔案：StringIO，並且用pd.read_csv將表格讀取進來
    df = pd.read_csv(StringIO(content))
    
    # 將表格中的元素都換成字串，並把其中的逗號刪除
    df = df.astype(str)
    df = df.apply(lambda s: s.str.replace(',', ''))
    
    # 將爬取的日期存入 dataframe
    df['date'] = pd.to_datetime(date)
    
    # 將「證券代號」的欄位改名成「stock_id」
    df = df.rename(columns={'證券代號':'stock_id'})
    
    # 將 「stock_id」與「date」設定成index 
    df = df.set_index(['stock_id', 'date'])
    
    # 將所有的表格元素都轉換成數字，error='coerce'的意思是說，假如無法轉成數字，則用 NaN 取代
    df = df.apply(lambda s:pd.to_numeric(s, errors='coerce'))
    
    # 刪除不必要的欄位
    df = df[df.columns[df.isnull().all() == False]]
    
    return df

def create_insert_values(df):
    insert_values = []
    for row in df.iterrows():
        insert_value = [row[0][0],str(row[0][1])]
        for col_name in ['成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '最後揭示買價',
           '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']:
            insert_value.append(row[1][col_name])
        insert_values.append(str(tuple(insert_value)).replace('nan','null'))
    return insert_values

date_range = get_day_range(date(2019,12,11),date(2020,8,8))
for d in date_range:
    df = crawl_price(d)
    if df is not None:
        
        insert_values = create_insert_values(df)
        post_data = {'insert_values':insert_values,'sql_cmd':sql_cmd}
        r = requests.post('http://114.35.97.130:41118/price',json=post_data)
        if r.status_code!=200:
            print('failed at',d)
        time.sleep(2)
    else :
        time.sleep(4)
    print(d)