# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取申万行业分类数据并更新入库
@author: 李博
"""
import pandas as pd
import json
import datetime
import time
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
# MONGODB CONNECT
#入库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str


#stocks=getstocks('801740.SI')
#print (stocks)

#获取当前分类的成份股详情
    '''
def getstocksdetail(indexcode,stockbasiccollection):
    stocks_items=[]
    stocks = pro.index_member(index_code=indexcode)
    stockslist = stocks['con_code'].tolist()
    for i in stockslist:
        print (i)
        stock_code = i
        query = { "ts_code": stock_code}
        stock_basic = stockbasiccollection.find(query)[0]
        stockname = stock_basic['name']
        area = stock_basic['area']
        industry = stock_basic['industry']
        #拼装json
        data = {"stockcode":stock_code,"stockname":stockname,"area":area,"industry":industry}
        print (data)
        stocks_items.append(data)       
    return stocks_items
#stocks=getstocksdetail('801740.SI')
#print (stocks)
'''

#获取申万一到三级行业列表
def get_sw_list(src,level):
    df = pro.index_classify(level=level, src=src)
    records = json.loads(df.T.to_json()).values()
    mycollection=mydb[src+level]
    mycollection.remove()
    mycollection.insert_many(records)
    print (src,level,str(len(df)))
    return df

#获取基本信息
df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
df_stockbasic['stockname'] = df_stockbasic['name']
#获取每日行情
df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
df_daily = pro.daily(trade_date=lasttradeday)
#print (df_daily)
if (df_daily.empty):
    print ('df empty')
    df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
    lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
    df_daily = pro.daily(trade_date=lasttradeday)
    #print ('get_df_dailyc',len(df_daily),lasttradeday)
df_daily['amount'] = df_daily['amount']/10000
#获取每日指标
df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
df_daily_basic = pro.daily_basic(ts_code='', trade_date=lasttradeday, fields='ts_code,turnover_rate,volume_ratio,pe_ttm,pb,circ_mv')
if (df_daily_basic.empty):
    df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
    lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
    df_daily_basic = pro.daily_basic(ts_code='', trade_date=lasttradeday, fields='ts_code,turnover_rate,volume_ratio,pe_ttm,pb,circ_mv')
    #print ('get_df_daily_basic',len(df_daily_basic),lasttradeday)
df_daily_basic['circ_mv'] = df_daily_basic['circ_mv']/10000
#获取当前分类的成份股
def get_sw_stocks(src,level):
    df = pro.index_classify(level=level, src=src)
    for index in df['index_code']:
        i = 0
        while i < 3:
            try:
                df_swstocks = pro.index_member(index_code=index)
                break
            except:
                print ('PAUSE WAITING 60S TO RECONNECTION')
                time.sleep(60)
                i += 1          
        #df_swstocks = pro.index_member(index_code=index)
        df_swstocks['ts_code'] = df_swstocks['con_code']
        df_temp = pd.merge(df_swstocks, df_stockbasic, how='left', on='ts_code')
        df = pd.merge(df_temp, df_daily_basic, how='left', on='ts_code')
        df2 = pd.merge(df, df_daily, how='left', on='ts_code')
        #df.to_csv('./data/sw/'+i+'_'+src+level+'.csv')
        mycollection=mydb['stocks_'+index]
        mycollection.remove()
        records = json.loads(df2.T.to_json()).values()
        mycollection.insert_many(records)
        print (src+level+index)

    
#标准写入函数
def csvtomongodb(filename,collection):
   #client = MongoClient('mongodb://112.12.60.2:27017')
   client = MongoClient('mongodb://127.0.0.1:27017')
   mydb=client["ptest"]
   mycollection=mydb[collection]
   mycollection.remove()
   path_df=open('./data/sw/'+filename+'.csv','r',encoding='UTF-8')
   df_csv = pd.read_csv(path_df)
   records = json.loads(df_csv.T.to_json()).values()
   #print (records)
   mycollection.insert(records) 

#入库
def get_sw_stocks_data():
    get_sw_list('sw','l1')
    get_sw_list('sw','l2')
    get_sw_list('sw','l3')
    get_sw_stocks('sw','l1')
    get_sw_stocks('sw','l2')
    get_sw_stocks('sw','l3')
    
#申万行业分类数据入库
def get_sw_level_data():
    get_sw_list('sw','l1')
    get_sw_list('sw','l2')
    get_sw_list('sw','l3')
 
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/sw/')
async def get_indexs(request:Request):
    get_sw_list('sw','l1')
    get_sw_list('sw','l2')
    get_sw_list('sw','l3')
    get_sw_stocks('sw','l1')
    get_sw_stocks('sw','l2')
    get_sw_stocks('sw','l3')
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })