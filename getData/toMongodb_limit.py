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

def get_limit_last():
    #获取历史交易日
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    #获取某日涨停股票，并指定字段输出
    df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    if (df_limit_lastday.empty):
        df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
        lasttradeday = df['cal_date'].tail(1).iloc[0]
        df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_limit = pd.merge(df_stockbasic, df_limit_lastday, how='right', on=['ts_code','name'])
    df_limit['ts_name'] = df_limit['name']
    #df1.to_csv('limit_lastday.csv')
    mycollection=mydb['limit_last']
    mycollection.remove()
    records = json.loads(df_limit.T.to_json()).values()
    mycollection.insert(records)
    print (df_limit['trade_date'][0],'daily_limit_last: '+str(len(df_limit)))

def get_limit(n):
    #获取时间段统计信息
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_limit = pro.limit_list(start_date=get_day_time(n), end_date=get_day_time(0), limit_type='U')
    df2 = pd.merge(df_stockbasic, df_limit, how='right', on=['ts_code','name'])
    df2['ts_name'] = df2['name']
    #df2.to_csv('limit.csv')
    mycollection=mydb['limit_'+str(n)]
    mycollection.remove()
    records = json.loads(df2.T.to_json()).values()
    mycollection.insert(records)

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#toMongodb('limit_lastday','limit_lastday')
#toMongodb('limit','limit')
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/limit/')
async def get_limits(request:Request):
    get_limit(5)
    get_limit(10)
    get_limit(20)
    get_limit(30)
    get_limit_last()
    #toMongodb('concept','concept')
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })