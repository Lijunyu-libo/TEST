# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取某日股票资金流入流出，并指定字段输出
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
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
TODAY = time.strftime('%Y%m%d',)

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str



#获取时间段统计信息
#获取单个股票数据
#df = pro.moneyflow(ts_code='002149.SZ', start_date='20190115', end_date='20190315')
#df2 = pd.merge(df_stockbasic, df_limit, how='right', on=['ts_code','name'])
#df2.to_csv('limit.csv')
 

#入库
from pymongo import MongoClient
#print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(500), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

def get_dailybasic_last():
    #获取历史交易日
    lasttradeday = get_lasttradedate(0)
    #获取某日股票资金流入流出，并指定字段输出
    df_dailybasic_lastday = pro.daily_basic(ts_code='', trade_date=lasttradeday)
    df_daily_lastday = pro.daily(trade_date=lasttradeday)
    if (df_dailybasic_lastday.empty or df_daily_lastday.empty):
        lasttradeday = get_lasttradedate(1)        
        df_dailybasic_lastday = pro.daily_basic(ts_code='', trade_date=lasttradeday)
        df_daily_lastday = pro.daily(trade_date=lasttradeday)
    df_temp = pd.merge(df_daily_lastday, df_dailybasic_lastday, how='left', on=['ts_code','trade_date','close'])
    #df1.to_csv('moneyflow_lastday.csv')  
    mycollection=mydb['dailybasic_last']
    mycollection.remove()
    #path_df=open(filename+'.csv','r',encoding='UTF-8') 
    #df_csv = pd.read_csv(path_df)
    records = json.loads(df_temp.T.to_json()).values()
    mycollection.insert(records)

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#toMongodb('moneyflow_lastday','moneyflow_lastday')
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/dailybasic/last/')
async def get_indexs(request:Request):
    get_dailybasic_last()
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })
#get_dailybasic_last()