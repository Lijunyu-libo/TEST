# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 10:06:55 2020
模拟下单函数
@author: iFunk
"""

import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
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

#策略下单 autotrade
SLIPRATE = 0.015
def autotrade(resultdf):
    if (resultdf.empty):
        print ('no tradememo saved')
    else:
        mycol = mydb['tradememo']
        create_date = get_day_time(0)
        resultdf['create_date'] = create_date
        resultdf['sliprate'] = SLIPRATE
        resultdf['price_buy'] = resultdf['trigger_close']*(1+SLIPRATE)
        mycol.insert_many(resultdf.to_dict('records'))
        print ('tradememo saved: '+str(len(resultdf)))

#人工下单 manualtrade    
def manualtrade(resultdict,reason,pricebuy):
    mycol = mydb['tradememo']
    create_date = get_day_time(0)
    resultdict['create_date'] = create_date
    resultdict['reason'] = reason
    resultdict['price_buy'] = pricebuy
    mycol.insert_one(resultdict)
    print ('tradememo saved: '+str(len(resultdict)))

#自动删除所有模拟交易 慎用  
def removeall():
    mycol = mydb['tradememo']
    mycol.remove()
    print ('tradememo removed')
   
#人工撤单

#人工平仓
    
#自动全部平仓