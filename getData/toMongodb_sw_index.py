# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取申万行业指数日线行情并更新入库
@author: 李博
"""
import pandas as pd
import json
import datetime
import time
from getData import indextools as tools
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
    mycollection.insert(records) 


#获取当前分类的指数行情
def get_sw_indexs_daily(src,level):
    df = pro.index_classify(level=level, src=src)
    for i in df['index_code']:
        df_swindex = pro.sw_daily(ts_code=i)
        df_swindex['index_name'] = df_swindex['name']
        df_swindex['high_low'] = df_swindex['high']-df_swindex['low']
        df_swindex['pct_high_low'] = ((df_swindex['high']-df_swindex['low'])/df_swindex['close']*100).round(decimals=2)
        df_swindex['amount_w'] = (df_swindex['amount']/1).round(decimals=2)
        df_swindex['amount_y'] = (df_swindex['amount']/10000).round(decimals=2)
        df_swindex['pre_amount_w'] = df_swindex['amount_w'].shift(-1)
        df_swindex['amount_change'] = (df_swindex['amount_w']-df_swindex['pre_amount_w']).round(decimals=2)
        df_swindex['pct_amount_change'] = (df_swindex['amount_change']/df_swindex['pre_amount_w']*100).round(decimals=2)
        #计算前3日成交金额
        df_swindex['amount_w_1'] = df_swindex['amount_w'].shift(-1)
        df_swindex['amount_w_2'] = df_swindex['amount_w'].shift(-2)
        df_swindex['amount_w_3'] = df_swindex['amount_w'].shift(-3)
        #计算前3日成交金额差额
        df_swindex['amount_change_1'] = df_swindex['amount_w']-df_swindex['amount_w_1']
        df_swindex['amount_change_2'] = df_swindex['amount_w_1']-df_swindex['amount_w_2']
        df_swindex['amount_change_3'] = df_swindex['amount_w_2']-df_swindex['amount_w_3']
        #计算前3日成交金额差额比
        df_swindex['pct_amount_change_1'] = (df_swindex['amount_change_1']/df_swindex['amount_w_1']*100).round(decimals=2)
        df_swindex['pct_amount_change_2'] = (df_swindex['amount_change_2']/df_swindex['amount_w_2']*100).round(decimals=2)
        df_swindex['pct_amount_change_3'] = (df_swindex['amount_change_3']/df_swindex['amount_w_3']*100).round(decimals=2)
        #判断前3日成交量涨跌
        df_swindex['amount_up'] = df_swindex.apply(tools.get_3days_up,axis=1)
        df_swindex['MA5'] = tools.get_ma(df_swindex,'MA5',5)
        df_swindex['MA10'] = tools.get_ma(df_swindex,'MA10',10)
        df_swindex['MA20'] = tools.get_ma(df_swindex,'MA20',20)
        df_swindex['MA30'] = tools.get_ma(df_swindex,'MA30',30)
        df_swindex['RSI6'] = tools.get_rsi(df_swindex,6)
        df_swindex['RSI12'] = tools.get_rsi(df_swindex,12)
        df_swindex['RSI24'] = tools.get_rsi(df_swindex,24)
        df_swindex['MTM'] = tools.get_mom(df_swindex,6)
        df_swindex['CCI'] = tools.get_cci(df_swindex)
        df_macd = tools.get_macd_mas(df_swindex)
        df_swindex['MACD'] = df_macd['MACD']
        df_swindex['DIF'] = df_macd['DIF']
        df_swindex['DEA'] = df_macd['DEA']
        df_kjd = tools.get_kdj(df_swindex)
        df_swindex['K'] = df_kjd['K']
        df_swindex['D'] = df_kjd['D']
        df_swindex['J'] = df_kjd['J']
        df_boll = tools.get_boll(df_swindex)
        df_swindex['BOLL_UPPER'] = df_boll['boll_upper']
        df_swindex['BOLL_MIDDLE'] = df_boll['boll_middle']
        df_swindex['BOLL_LOWER'] = df_boll['boll_lower']
        #df.to_csv('./data/sw/'+i+'_'+src+level+'.csv')
        mycollection=mydb[i+'_daily']
        mycollection.remove()
        records = json.loads(df_swindex.T.to_json()).values()
        mycollection.insert(records)
        print ('sw_indexs_daily',src,level,i,df_swindex['trade_date'][0],len(df_swindex))

#获取最新交易日的指数行情
def get_sw_indexs_daily_lastday():
    df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
    lasttradedate = df_tradedate['cal_date'].tail(1).iloc[0]
    df_swindex = pro.sw_daily(trade_date=lasttradedate)
    if (df_swindex.empty):        
        df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
        lasttradedate = df_tradedate['cal_date'].tail(1).iloc[0]
        df_swindex = pro.sw_daily(trade_date=lasttradedate)
        print ('sw_indexs_daily_lastday',lasttradedate,len(df_swindex))
    df_swindex['index_name'] = df_swindex['name']
    mycollection=mydb['swindexs_daily_last']
    mycollection.remove()
    records = json.loads(df_swindex.T.to_json()).values()
    mycollection.insert(records)
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
def get_sw_indexs_data():
    get_sw_list('sw','l1')
    get_sw_list('sw','l2')
    get_sw_list('sw','l3')
    get_sw_indexs_daily('sw','l1')
    get_sw_indexs_daily('sw','l2')
    get_sw_indexs_daily('sw','l3')
    get_sw_indexs_daily_lastday()
    
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/swindexs/')
async def get_indexs(request:Request):
    get_sw_list('sw','l1')
    get_sw_list('sw','l2')
    get_sw_list('sw','l3')
    get_sw_indexs_daily('sw','l1')
    get_sw_indexs_daily('sw','l2')
    get_sw_indexs_daily('sw','l3')
    get_sw_indexs_daily_lastday()
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })
    