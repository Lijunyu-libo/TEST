# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020

@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time

# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取历史交易日
df = pro.trade_cal(exchange='SSE', is_open='1',start_date=get_day_time(5), end_date=get_day_time(0))
lasttradedate = df['cal_date'].tail(1).iloc[0]
df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
df_dailybasic = pro.daily_basic(ts_code='', trade_date=lasttradedate)
df = pd.merge(df_stockbasic, df_dailybasic, how='left', on='ts_code')
df.to_csv('stocks_dailybasic_lastday.csv')

'''
#获取N日换手率等数据
df_tushare = pd.DataFrame()
for i in df['cal_date']:
    df_tradedate = pro.daily(trade_date=i)
    df_tushare = df_tushare.append(df_tradedate,ignore_index=True)
df_tushare.to_csv(today+'.csv')    
'''
#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["stocks_dailybasic_lastday"]
mycollection.remove()
path_df=open('stocks_dailybasic_lastday.csv','r',encoding='UTF-8') 
df_csv = pd.read_csv(path_df)
records = json.loads(df_csv.T.to_json()).values()
#print (records)
mycollection.insert(records)
