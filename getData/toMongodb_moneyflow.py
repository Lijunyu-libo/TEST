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



#获取时间段统计信息
#获取单个股票数据
#df = pro.moneyflow(ts_code='002149.SZ', start_date='20190115', end_date='20190315')
#df2 = pd.merge(df_stockbasic, df_limit, how='right', on=['ts_code','name'])
#df2.to_csv('limit.csv')
 

#入库
from pymongo import MongoClient
print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

#更新N交易日资金流向数据
def get_moneyflow(n,days):
    mycollection=mydb['moneyflow']
    mycollection.remove()
    #获取历史交易日
    tradeday_list = get_lasttradedatelist(n,days)
    for tradeday in tradeday_list:
        #获取某日股票资金流入流出，并指定字段输出
        df_moneyflow_tradeday = pro.moneyflow(trade_date=tradeday)
        mycollection.insert_many(df_moneyflow_tradeday.to_dict('records'))
        print ('moneyflow_'+tradeday+': '+str(len(df_moneyflow_tradeday)))
#get_moneyflow(0,300)
#更新最近一个交易日资金流向数据
def get_moneyflow_last():
    #获取历史交易日
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    #获取某日股票资金流入流出，并指定字段输出
    df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    if (df_moneyflow_lastday.empty):
        df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
        lasttradeday = df['cal_date'].tail(1).iloc[0]
        df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_moneyflow = pd.merge(df_stockbasic, df_moneyflow_lastday, how='right', on=['ts_code'])
    df_moneyflow['stockname'] = df_moneyflow['name']
    #df1.to_csv('moneyflow_lastday.csv')  
    mycollection=mydb['moneyflow_lastday']
    mycollection.remove()
    #path_df=open(filename+'.csv','r',encoding='UTF-8') 
    #df_csv = pd.read_csv(path_df)
    records = json.loads(df_moneyflow.T.to_json()).values()
    mycollection.insert(records)
    print (df_moneyflow['trade_date'][0],'daily_moneyflow_last: '+str(len(df_moneyflow)))
    

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#获取个股资金流向数据
def get_stock_moneyflow(stockcode):
    df_one = pro.moneyflow(ts_code=stockcode)
    mycollection=mydb['moneyflow_'+stockcode]
    mycollection.remove()
    mycollection.insert_many(df_one.to_dict('records'))
    print ('stocks_daily_qfq: '+stockcode)

def get_eachstock_moneyflow():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockcode in stocknamelist:
        df_one = pro.moneyflow(ts_code=stockcode)
        if (df_one is None):
            print (stockcode+' data empty')
        else:
            mycollection=mydb['moneyflow_'+stockcode]
            mycollection.remove()
            mycollection.insert_many(df_one.to_dict('records'))
            print ('moneyflow_'+stockcode+': '+str(len(df_one)))
    
#toMongodb('moneyflow_lastday','moneyflow_lastday')
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/moneyflow/')
async def get_indexs(request:Request):
    get_moneyflow_last()
    #toMongodb('concept','concept')
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })
#get_eachstock_moneyflow()