import requests
import pandas as pd
from io import StringIO
import datetime
from datetime import date,timedelta

def crawl_monthly_revenue(year,month):
    url = f'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{str(year-1911)}_{str(month)}_0.html'
    r = requests.get(url)

    # 讓pandas可以讀取中文（測試看看，假如不行讀取中文，就改成 'utf-8'）
    r.encoding = 'big5'
    # 把剛剛下載下來的網頁的 html 文字檔，利用 StringIO() 包裝成一個檔案給 pandas 讀取
    dfs = pd.read_html(StringIO(r.text))
    # 將dfs中，row長度介於5~11的table合併（這些才是我們需要的table，其他table不需要）
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])

    # 設定column名稱
    df.columns = df.columns.get_level_values(1)

    # 將 df 中的當月營收用 .to_numeric 變成數字，再把其中不能變成數字的部分以 NaN 取代（errors='coerce'）
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')

    # 再把當月營收中，出現 NaN 的 row 用 .dropna 整行刪除
    df = df[~df['當月營收'].isnull()]

    # 刪除「公司代號」中出現「合計」的行數，其中「～」是否定的意思
    df = df[df['公司代號'] != '合計']

    # 將「公司代號」與「公司名稱」共同列為 df 的 indexes
    df = df.set_index(['公司代號', '公司名稱'])
    return df
def get_time_range(start,end):
    year_start,month_start = [int(x) for x in start.strftime("%Y-%m").split('-')]
    year_end,month_end = [int(x) for x in end.strftime("%Y-%m").split('-')]
    time_range = []
    for i in [x for x in range(year_start,year_end+1)]:
        for j in range(1,13):
            time_range.append([i,j])
    start_index = time_range.index([year_start,month_start])
    end_index = time_range.index([year_end,month_end])+1
    time_range = time_range[start_index:end_index]
    return time_range

def convert_df(df):
    insert_values = []
    for row in df.iterrows():
        insert_value = [row[0][0],timestamp]
        for col_name in ['上月比較增減(%)', '上月營收','前期比較增減(%)', '去年同月增減(%)', '去年當月營收','去年累計營收', '當月營收', 
            '當月累計營收']:
            value = row[1][col_name]
            insert_value.append(value)
        insert_values.append(str(tuple(insert_value)).replace('nan','null'))
    return insert_values

for t in get_time_range(date(2019,5,1),date(2020,8,1)):
    year = t[0]
    month = t[1]
    timestamp = str(datetime.datetime(year,month,10))
    df = crawl_monthly_revenue(year,month)
    insert_values = create_insert_values(df)
    sql_cmd = "INSERT INTO monthly_revenue VALUES"
    post_data = {'sql_cmd':sql_cmd,'insert_values':insert_values}
    requests.post('http://localhost:8181/monthly_revenue',json=post_data)