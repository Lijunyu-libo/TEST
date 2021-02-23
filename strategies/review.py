# -*- coding: utf-8 -*-
"""
Created on Fri Nov 27 09:06:24 2020
#策略回测框架
@author: iFunk
"""
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import inspect
# MONGODB CONNECT
from pymongo import MongoClient
import json
client = MongoClient('mongodb://127.0.0.1:27017')
#client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()

def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#获取集合函数 条件参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (len(df_col))
    return df_col

#读取数据库函数 条件参数 col st et 返回 df
def get_col_st_et_df(col,st,et):
    mycollection=mydb[col]
    query = { "$and": [{"trade_date":{"$gte":st}},{"trade_date":{"$lte":et}}]} 
    rs_daterange_col = mycollection.find(query)
    list_col = list(rs_daterange_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (len(df_col))
    return df_col    
#df_all = get_col_df('daily_qfq_macd_000001.SZ')
def gather(stockcode,startdate,enddate):
    df = get_col_st_et_df('daily_qfq_macd_'+stockcode,startdate,enddate)
    print (stockcode,startdate,enddate,'MAX:',round(df['close'].max(),2),'AVG:',round(df['close'].mean(),2),'MIN:',round(df['close'].min(),2),'CLOSE:',round(df['close'][0],2))
#gather('000002.SZ','20200101','20201126')
#gather('000002.SZ','20200101','20200331')
#gather('000002.SZ','20200401','20200630')
#gather('000002.SZ','20200701','20200930')
#gather('000002.SZ','20201001','20201126')
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(30), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    print (lasttradeday)
    return lasttradeday


#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    lasttradeday_list.reverse()
    print (lasttradeday_list)
    return lasttradeday_list

#get_lasttradedatelist(0,10)

#指定行业数据获取功能   
#获取指定证监会行业分类 股票代码列表
def get_industry_stockcode_list(industry_list):
    list_result = []
    df = get_col_df('stocksbasic_industry_list')
    df_result = df[df['industry_name'].isin(industry_list)]
    for item in df_result['stockslist']:
        #print (item)
        list_result.extend(item)
        #list_result.append(item)
    print (len(list_result))
    return list_result
#industry_list = ['元器件','互联网']
#industry_stockcode_list = get_industry_stockcode_list(industry_list)

#指定市场数据获取功能    
#exchage SSE SZSE
#market 主板 科创板 主板 中小板 创业板
def get_market_list(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    list_result = data['ts_code'].tolist()   
    #print (exchange,market,len(list_result))
    return list_result
#get_market_list('SSE','主板')
#get_market_list('SSE','科创板')
#get_market_list('SZSE','主板')
#get_market_list('SZSE','中小板')
#get_market_list('SZSE','创业板')
#get_market_list('','')
    
#回测DEMO
def calback_demo(exchange,market):
    #定义目标个股列表
    #industry_list = ['元器件','互联网']
    #industry_stockcode_list = get_industry_stockcode_list(industry_list)
    #industry_stockcode_list = get_market_list('SZSE','中小板')
    industry_stockcode_list = get_market_list(exchange,market)
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    for stockcode in industry_stockcode_list:
        result_dict={}
        #定义返回数据日期范围
        n_start = 10
        n = 60
        df_qfq = get_col_df('daily_qfq_macd_'+stockcode)
        #df_qfq = get_col_st_et_df('daily_qfq_macd_'+stockcode,startdate,enddate)
        if (df_qfq is None or len(df_qfq)<n):
            continue
        else:
            vol_n_min = df_qfq['vol'][n_start:n].min()
            close_n_min = df_qfq['close'][n_start:n].min()
            if(df_qfq['vol'][n_start]==vol_n_min and df_qfq['close'][n_start]<close_n_min*1.05):
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][n_start]
                result_dict['trigger_close'] = df_qfq['close'][n_start]
                result_dict['reason'] = FNAME+str(n)
                result_dict['close_diff_ratio'] = round((df_qfq['close'][0]-df_qfq['close'][n_start])/df_qfq['close'][n_start],3)
                result_dict['close_max_ratio'] = round((df_qfq['close'][0:n_start].max()-df_qfq['close'][n_start])/df_qfq['close'][n_start],3)
                #print (stockcode,df_qfq['trade_date'][0],result_dict['close_diff_ratio'],result_dict['close_max_ratio'])
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    close_up_ratio = round(len(result_df[result_df['close_diff_ratio']>0])/len(result_df),2)        
    print (exchange,market,'涨跌统计：',len(result_df),close_up_ratio,'  涨跌幅统计：',round(result_df['close_diff_ratio'].mean(),3),round(result_df['close_diff_ratio'].max(),3),round(result_df['close_diff_ratio'].min(),3))
    #print ('END',FNAME,len(result_df),TIMENOW)
    return result_df      
#calback_demo('SZSE','主板')
#calback_demo('SZSE','中小板')
#calback_demo('SSE','主板') 