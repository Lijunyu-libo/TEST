# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取月线数据并入库
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
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取历史交易日
#df = pro.income(ts_code='600000.SH', start_date='20190101', end_date='20200731', fields='ts_code,end_date,f_ann_date,report_type,comp_type,basic_eps,diluted_eps')
#period='20191231',
#获取利润表数据
df_income= pro.income_vip(fields='ts_code,f_ann_date,end_date,basic_eps,revenue,total_cogs,sell_exp,admin_exp,fin_exp,total_profit,n_income')
df2 = df_income.drop_duplicates(subset = ['ts_code','f_ann_date','end_date'])
print (df2)
df2.to_csv('stocks_income.csv')       

#入库
from pymongo import MongoClient
print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#执行入库
toMongodb('stocks_income','stocks_income')

