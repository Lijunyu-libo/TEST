# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取某季度股票财报预告
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
TODAY = time.strftime('%Y%m%d',)

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
 

#入库
from pymongo import MongoClient
print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

def get_forecast(period):
    #获取某季度业绩预告
    df_forecast = pro.forecast_vip(period=period)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_stockbasic['ts_name'] = df_stockbasic['name']
    df1 = pd.merge(df_stockbasic, df_forecast, how='right', on=['ts_code'])    
    #df1.to_csv('moneyflow_lastday.csv')  
    mycollection=mydb['forecast_'+period]
    mycollection.remove()
    #path_df=open(filename+'.csv','r',encoding='UTF-8') 
    #df_csv = pd.read_csv(path_df)
    records = json.loads(df1.T.to_json()).values()
    mycollection.insert(records)
    print ('forecast',period,df1['ann_date'][0],str(len(df1)))
    return df1

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

def get_forecast_2020():
    anndates =  ['20200331','20200630','20200930','20201231']
    for i in anndates:
        get_forecast(i)
#get_forecast_2020()        

#toMongodb('moneyflow_lastday','moneyflow_lastday')
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/forecast/')
async def get_indexs(request:Request):
    anndates =  ['20200331','20200630','20200930','20201231']
    for i in anndates:
        get_forecast(i)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })