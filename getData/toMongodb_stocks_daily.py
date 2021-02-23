# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取个股日线数据并入库
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
# MONGODB CONNECT
from pymongo import MongoClient
#print (os.getcwd())
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#TUSHARE
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(500), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

#QFQ
def get_fqratio_tradedatelist(tradedatelist):
    result = pd.DataFrame()
    for i in tradedatelist:        
        df_tradedate = pro.adj_factor(ts_code='', trade_date=i)
        result = result.append(df_tradedate,ignore_index=True)
        print ('fqratio',i)
    mycollection=mydb['stocks_fqr']
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)
    return result
        
#GET DAILY BY TRADEDATELIST
def get_daily_qfq_tradedatelist(tradedatelist):
    #fqr
    mycollection=mydb["stocks_fqr"]
    rs_fqr = mycollection.find()
    list_fqr = list(rs_fqr)
    list_fqr.reverse()
    df_fqr = pd.DataFrame(list_fqr)
    df_fqr_stockcode = df_fqr.sort_values(by="ts_code",ascending=True)
    #print (df_fqr_stockcode[['ts_code','trade_date']])
    #daily
    mycollection=mydb["stocks_daily"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    df_stockcode = pd.DataFrame(list_stockcode)
    df_ma_gb_stockcode = df_stockcode.groupby('ts_code')
    mycollection=mydb['stocks_daily_qfq']
    mycollection.remove()
    #计算前复权价格
    df_all=pd.DataFrame()
    for name,group in df_ma_gb_stockcode:
        df_one=pd.DataFrame()
        #个股复权因子
        #df_fqr_name = df_fqr_stockcode[df_fqr_stockcode['ts_code']==name]
        #df_fqr_name = df_fqr_name.sort_values(by="trade_date",ascending=False)
        #df_fqr_name.reset_index(drop=True, inplace=True)
        #print (df_fqr_name)  
        #某股未复权行情
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        #合并数据并计算前复权行情
        #df_one = pd.merge(df_fqr_name, df_group, how='right', on=['trade_date','ts_code'])
        df_one = pd.merge(df_fqr_stockcode, df_group, how='right', on=['trade_date','ts_code'])
        df_one['close_qfq'] = df_one['close']*df_one['adj_factor']/df_one ['adj_factor'][0]
        #df_all=df_all.append(df_one,ignore_index=True)
        print ('stocks_daily_qfq: '+name)
        #records = json.loads(df_one.T.to_json()).values()
        mycollection.insert_many(df_one.to_dict('records'))  
        #mycollection.insert(records)
    return df_all

#GET DAILY BY TRADEDATELIST
def get_weekly_qfq_tradedatelist(tradedatelist):
    #fqr
    mycollection=mydb["stocks_fqr"]
    rs_fqr = mycollection.find()
    list_fqr = list(rs_fqr)
    list_fqr.reverse()
    df_fqr = pd.DataFrame(list_fqr)
    df_fqr_stockcode = df_fqr.sort_values(by="ts_code",ascending=True)
    #print (df_fqr_stockcode[['ts_code','trade_date']])
    #daily
    mycollection=mydb["stocks_weekly"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    df_stockcode = pd.DataFrame(list_stockcode)
    df_ma_gb_stockcode = df_stockcode.groupby('ts_code')
    mycollection=mydb['stocks_weekly_qfq']
    mycollection.remove()
    #计算前复权价格
    df_all=pd.DataFrame()
    for name,group in df_ma_gb_stockcode:
        df_one=pd.DataFrame()
        #个股复权因子
        #df_fqr_name = df_fqr_stockcode[df_fqr_stockcode['ts_code']==name]
        #df_fqr_name = df_fqr_name.sort_values(by="trade_date",ascending=False)
        #df_fqr_name.reset_index(drop=True, inplace=True)
        #print (df_fqr_name)  
        #某股未复权行情
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        #合并数据并计算前复权行情
        #df_one = pd.merge(df_fqr_name, df_group, how='right', on=['trade_date','ts_code'])
        df_one = pd.merge(df_fqr_stockcode, df_group, how='right', on=['trade_date','ts_code'])
        df_one['close_qfq'] = df_one['close']*df_one['adj_factor']/df_one ['adj_factor'][0]
        #df_all=df_all.append(df_one,ignore_index=True)
        print ('stocks_weekly_qfq: '+name)
        #records = json.loads(df_one.T.to_json()).values()
        mycollection.insert_many(df_one.to_dict('records'))  
        #mycollection.insert(records)
    return df_all
    
#GET DAILY BY TRADEDATELIST
def get_daily_tradedatelist(tradedatelist):
    result = pd.DataFrame()
    for i in tradedatelist:        
        df_tradedate = pro.daily(trade_date=i)
        result = result.append(df_tradedate,ignore_index=True)
        print ('stocks_daily',i)
    mycollection=mydb['stocks_daily']
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)
    
#GET DAILY BY TRADEDATELIST
def get_weekly_tradedatelist(tradedatelist):
    result = pd.DataFrame()
    for i in tradedatelist:        
        df_weekly_tradedate = pro.weekly(trade_date=i)
        if (df_weekly_tradedate.empty):
            continue
        else:
            result = result.append(df_weekly_tradedate,ignore_index=True)
            print ('stocks_weekly',i)
    mycollection=mydb['stocks_weekly']
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)

#封装自动更新函数
def get_daily_qfq_asp():
    tradedatelist = get_lasttradedatelist(0,300)
    get_daily_tradedatelist(tradedatelist)
    get_fqratio_tradedatelist(tradedatelist)
    get_daily_qfq_tradedatelist(tradedatelist)

def get_weekly_qfq_asp():
    tradedatelist = get_lasttradedatelist(0,300)
    get_weekly_tradedatelist(tradedatelist)
    get_fqratio_tradedatelist(tradedatelist)
    get_weekly_qfq_tradedatelist(tradedatelist)

tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/stocks/fqr/')
async def get_stocks_fqr(request:Request):
    tradedatelist = get_lasttradedatelist(0,200)
    get_fqratio_tradedatelist(tradedatelist)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })
    
@router.get('/update/stocks/daily/')
async def get_stocks_daily(request:Request):
    tradedatelist = get_lasttradedatelist(0,200)
    #get_daily_tradedatelist(tradedatelist)
    get_daily_qfq_tradedatelist(tradedatelist)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })

tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/stocks/weekly/')
async def get_stocks_weekly(request:Request):
    tradedatelist = get_lasttradedatelist(0,200)
    #get_weekly_tradedatelist(tradedatelist)
    get_weekly_qfq_tradedatelist(tradedatelist)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })
#get_weekly_qfq_asp()
#tradedatelist = get_lasttradedatelist(0,200)
#get_daily_tradedatelist(tradedatelist)
#get_weekly_tradedatelist(tradedatelist)
#get_fqratio_tradedatelist(tradedatelist)
#get_daily_qfq_tradedatelist(tradedatelist)
#get_weekly_qfq_tradedatelist(tradedatelist)
#df2 = ts.pro_bar(ts_code='000002.SZ', adj='qfq', start_date=get_lasttradedate(20), end_date=get_lasttradedate(0))