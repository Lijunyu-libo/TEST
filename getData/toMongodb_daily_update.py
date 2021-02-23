# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取数据库中最后一条数据交易日，补充从最后一个交易日后-至前一交易日日线数据
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time

# MONGODB CONNECT
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["dailytest"]
#获取数据库中最后一个交易日
rs = mycollection.find().sort('_id', -1).limit(1)  # 倒序以后，只返回1条数据
#print (rs)
LAST_TRADE_DATE = ''
for i in rs:
    LAST_TRADE_DATE = str(i['trade_date'])
    print (LAST_TRADE_DATE)
# TUSHARE CONNECT
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

#计算指定日期的前N天的时间戳
def get_preday_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#计算指定日期的后N天的时间戳
def get_afterday_time(day,n):
    the_date = datetime.datetime.strptime(day, "%Y%m%d")
    after_date = the_date + datetime.timedelta(days=n)
    after_date_str = after_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return after_date_str
# DANGTIAN
TODAY = time.strftime('%Y%m%d',)
# QIANYITIAN 
PRE_TODAY = get_preday_time(1)#获取历史交易日,get_preday_time(1) 不包含当前交易日

df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_afterday_time(LAST_TRADE_DATE,1), end_date=PRE_TODAY)
print (df['cal_date'])

#获取日线数据
df_tushare = pd.DataFrame()
for i in df['cal_date']:
    df_tradedate = pro.daily(trade_date=i)
    df_tushare = df_tushare.append(df_tradedate,ignore_index=True)
df_tushare.to_csv(PRE_TODAY+'_daily.csv')    

#入库
path_df=open(PRE_TODAY+'_daily.csv') 
df_csv = pd.read_csv(path_df)
records = json.loads(df_csv.T.to_json()).values()
#print (records)
mycollection.insert(records)