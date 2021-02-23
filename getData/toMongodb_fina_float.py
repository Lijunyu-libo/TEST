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

#计算指定日期的后N天的时间戳
def get_day_after(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date + datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#TRADELIST
def get_tradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_after(n), end_date=get_day_after(n+days))
    lasttradeday_list = df['cal_date'].tolist()
    #lasttradeday_list.reverse()
    return lasttradeday_list 
#print (get_tradedatelist(0,10))
#入库
from pymongo import MongoClient
print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

def get_float_period(period):
    #获取某日限售股解禁数据
    df_float = pro.share_float(ann_date=period)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_stockbasic['ts_name'] = df_stockbasic['name']
    df1 = pd.merge(df_stockbasic, df_float, how='right', on=['ts_code'])    
    #df1.to_csv('moneyflow_lastday.csv')  
    mycollection=mydb['float_'+period]
    mycollection.remove()
    #path_df=open(filename+'.csv','r',encoding='UTF-8') 
    #df_csv = pd.read_csv(path_df)
    records = json.loads(df1.T.to_json()).values()
    mycollection.insert(records)

def get_float_last():
    #获取某日限售股解禁数据
    mycollection=mydb['float_last']
    mycollection.remove()
    tradedatelist = get_tradedatelist(0,10)
    print (tradedatelist)
    for tradedate in tradedatelist:
        df_float = pro.share_float(float_date=tradedate)
        if (df_float.empty):
            pass
        else:
            #df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
            #df_stockbasic['ts_name'] = df_stockbasic['name']
            #df1 = pd.merge(df_stockbasic, df_float, how='right', on=['ts_code'])    
            #df1.to_csv('moneyflow_lastday.csv')  
        
            #path_df=open(filename+'.csv','r',encoding='UTF-8') 
            #df_csv = pd.read_csv(path_df)
            records = json.loads(df_float.T.to_json()).values()
            mycollection.insert(records)
            print ('float',tradedate,str(len(df_float)))
#get_float_last()
#全年解禁个股数据
def get_float_year(year):
    #获取某日限售股解禁数据
    #lastday = get_day_time(1)
    df_float = pro.share_float(start_date=year+'0101',end_date = year+'1231')
    #df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    #df_stockbasic['ts_name'] = df_stockbasic['name']
    #df1 = pd.merge(df_stockbasic, df_float, how='right', on=['ts_code'])    
    #df1.to_csv('moneyflow_lastday.csv')  
    mycollection=mydb['float_year']
    mycollection.remove()
    #path_df=open(filename+'.csv','r',encoding='UTF-8') 
    #df_csv = pd.read_csv(path_df)
    records = json.loads(df_float.T.to_json()).values()
    mycollection.insert(records)
    print ('float',year,str(len(df_float)))
    return df_float
#get_float_year('2020')

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#toMongodb('moneyflow_lastday','moneyflow_lastday')
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/float/')
async def get_indexs(request:Request):
    anndates =  [get_day_time(1),get_day_time(2),get_day_time(3),get_day_time(4),get_day_time(5)]
    #季度解禁数据
    for i in anndates:
        get_float_period(i)
    #某日解禁数据
    get_float_last()
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })