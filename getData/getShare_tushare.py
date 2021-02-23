# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020

@author: 李博
"""
import tushare as ts
#import datetime
#from datetime import date
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
def getShareOne():
    data = ts.get_hist_data('600848',start='2020-06-01',end='2020-06-29')
    print (data['open'])
#getShareOne()
#sched.add_job(getShareOne,'date',run_date=datetime(2020,6,29,16,30,5))
sched.add_job(getShareOne,'cron',day_of_week='1-5',hour=9,minute=55)
sched.start()