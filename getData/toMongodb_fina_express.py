# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取业绩快报并入库
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time

# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取财务指标数据

df_indicator = pro.fina_indicator_vip(ann_date='20201202')
#print (df_indicator)

#period='20191231',
#获取财务快报数据

df_express= pro.express_vip(ann_date='20201202')
#print (df_express)
#df_express.to_csv('stocks_express.csv')       

'''
#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
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
'''
