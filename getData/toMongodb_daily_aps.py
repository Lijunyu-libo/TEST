# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020

@author: 李博
"""
import tushare as ts
import pandas as pd
#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()
from pymongo import MongoClient

client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]

#daily
def get_stocks_daily_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        #print (stockname)
        mycol = mydb['daily_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq')
        if (df is None):
            print (stockname+' data empty')
        else:
            mycol.insert_many(df.to_dict('records'))
            print ('daily_'+stockname+': '+str(len(df)))
#weekly
def get_stocks_weekly_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        mycol = mydb['weekly_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq',freq='W')
        if (df is None):
            print (stockname+' data empty')
        else:
            mycol.insert_many(df.to_dict('records'))
            print ('weekly_'+stockname+': '+str(len(df)))
            
sched.add_job(get_stocks_daily_qfq,'cron',day_of_week='mon-fri', hour=23, minute=1)
sched.add_job(get_stocks_weekly_qfq,'cron',day_of_week='sat', hour=23, minute=1)
sched.start()