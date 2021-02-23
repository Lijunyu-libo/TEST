# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020

@author: 李博
"""
import tushare as ts
import pandas as pd
df = pd.DataFrame()
import time
today=time.strftime('%Y%m%d',)
#from datetime import date
#定时器初始化
#from apscheduler.schedulers.blocking import BlockingScheduler
#sched = BlockingScheduler()
#tushare初始化
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#获取某时间段单只股票
def getShareOne():
    df = pro.daily(ts_code='600848.SH',start_date=today,end_date=today)
    print (df['open'])
    df.to_csv(today+'.csv')

def getAllOneday():
    df=pro.daily(trade_date = today)
    df.to_csv(today+'.csv')
#getAllOneday()
getShareOne()
#sched.add_job(getShareOne,'date',run_date=datetime(2020,6,29,16,30,5))
#sched.add_job(getShareOne,'cron',day_of_week='1-5',hour=16,minute=10)
#sched.start()