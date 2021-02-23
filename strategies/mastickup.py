# -*- coding: utf-8 -*-
"""
Spyder Editor
计算所有股票MA
This is a temporary script file.
"""
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import json
# MONGODB CONNECT
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

#TUSHARE GET STOCK_DAILY BY CODE
def get_daily_code(stockcode):
    df = pro.daily(ts_code=stockcode)
    return df

#TUSHARE GET STOCK_DAILY BY tradedate
def get_daily_tradedate(tradedate):
    df = pro.daily(trade_date=tradedate)
    return df

#计算MA函数
def cal_ma(df,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise
def cal_ma_ta(df,n):
    temp_serise = talib.MA(df['close'],timeperiod=n)
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise
#MA
def get_ma_n(stockcode):
    mycollection=mydb["stocks_daily"]
    rs_stockcode = mycollection.find({'ts_code':stockcode})
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    if (df_stockcode.empty or len(df_stockcode)<30):
        print (stockcode)
    else:
        df_stockcode['MA5'] = cal_ma_ta(df_stockcode,5)
        df_stockcode['MA10'] = cal_ma_ta(df_stockcode,10)
        df_stockcode['MA20'] = cal_ma_ta(df_stockcode,20)
        df_stockcode['MA30'] = cal_ma_ta(df_stockcode,30)
    return df_stockcode


#粘合算法,返回粘合系数列表
def cal_stick(df,param1,param2,n):
    stick_list = []
    i = 0
    while i<n:
        w = math.fabs((df[param1][i]-df[param2][i])/df[param2][i])
        stick_list.append(w)
        i +=1
    #stick_series = pd.Series(stick_list)    
    return stick_list
    
#UP算法,返回true/false
def cal_up(df,param1,param2,n,step):
    up_list = []
    i = 0
    while i<n:
        w = df[param1][i]-df[param2][i+step]
        up_list.append(w)
        i +=1
    #stick_series = pd.Series(stick_list)    
    return up_list

#MA粘合策略+5、10线上升
def get_ma_stick_up(df):
    w = 0.01
    up = 0
    result_list = []
    stockcodelist = df['ts_code'].tolist()
    for i in stockcodelist:
        df_ma = get_ma_n(i)
        #停牌或无20MA
        if (df_ma.empty or len(df_ma)<30):
            print (i)
        else:
            stick_w_5_10 = cal_stick(df_ma,'MA5','MA10',5)
            stick_w_10_20 = cal_stick(df_ma,'MA10','MA20',5)
            #stick_w_20_30 = cal_stick(df_ma,'MA20','MA30',5)
            up_w_5_10 = cal_up(df_ma,'MA5','MA10',2,0)
            if (max(stick_w_5_10)<=w):
                if (max(stick_w_10_20)<=w):
                    if (up_w_5_10[0]>up and up_w_5_10[1]<up):
                        result_list.append(i)
                        print ('OK: ',i,' ',up_w_5_10)
    df_daily = pro.daily(trade_date=get_lasttradedate(0))
    if (df_daily.empty):
        df_daily = pro.daily(trade_date=get_lasttradedate(1))
    df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    if (df_daily_basic.empty):
        df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(1))
    df_result = df_daily_basic[df_daily_basic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    rundate = datetime.datetime.now().strftime('%Y%m%d')
    mycollection=mydb['mastickup'+rundate]
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)
    return result 
'''          
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

df = get_stockbasket('SSE','主板')
print (get_ma_stick_up(df))
  '''      




