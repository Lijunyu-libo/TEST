# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取 000001 399001 399005 399006 指数日线数据
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time
import talib

# MONGODB CONNECT
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()
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
df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
print (lasttradeday)


#入库
from pymongo import MongoClient
#print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

