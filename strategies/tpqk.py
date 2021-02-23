# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 11:53:34 2020
#突破缺口策略
@author: iFunk
"""
import sys
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import inspect
#import caltools
#import tradememo
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
from tushare.util import formula as fm
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')


def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取条件集合函数 参数 col param 返回df
def get_col_param_df(col,param,value):
    mycollection=mydb[col]
    query = {param:value}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取数据函数 排序
def get_col_sort_df(collection,colum,asc,count):
    mycollection=mydb[collection]
    rs_col = mycollection.find().limit(count).sort([(colum,asc)])
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    #data = data[~ data['name'].str.contains('ST|退')]
    data = data[~ data['name'].str.contains('退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data
#判断缺口数量
def count_qk(df,n):           
    stick_list = []
    i = 0
    while i<n:
        w = df['low'][i]-df['high'][i+1]
        if w>0:
            stick_list.append(w)
        i +=1
    return stick_list



#判断当前交易日为缺口函数
def qkdemo():
    #df_stocks = get_stockbasket('','')
    #for stockcode in df_stocks['ts_code']:
    #    print (stockcode)
    #print (len(df_stocks['ts_code']))
    df_0 = pro.daily(trade_date='20201125')
    df_1 = pro.daily(trade_date='20201124')
    #df_daily_merge = pd.DataFrame()
    df_daily_1 = pd.DataFrame()
    df_daily_1['ts_code'] = df_1['ts_code']
    df_daily_1['high_1'] = df_1['high']
    df_0 = pd.merge(df_0, df_daily_1, how='left', on='ts_code')
    df_0['qk'] = (df_0['low']-df_0['high_1'])/df_0['high_1']
    df_qk = df_0[df_0['qk']>0]
    df_result = pd.DataFrame()
    n = 20
    for stockcode in df_qk['ts_code']:
        df_stockcode = get_col_df('daily_qfq_macd_'+stockcode)
        if len(df_stockcode) >= n:
            list_qk = count_qk(df_stockcode,n)
            if len(list_qk) <= 1:
                if (df_stockcode['high'][0] == df_stockcode['close'][0] and df_stockcode['open'][0] == df_stockcode['low'][0]):
                    print (stockcode)
                    df_result = df_stockcode
                
    return df_result
#df = qkdemo()    
    
def get_data_tpqk():
    #m日前行情缺口股票列表及缺口开盘股价
    tradedatelist = get_lasttradedatelist(1,100)
    tradedatelist.reverse()
    #print (tradedatelist)
    n = 11
    df = pro.daily(trade_date=tradedatelist[n+0])
    df_0 = pro.daily(trade_date=tradedatelist[n+1])
    df_1 = pro.daily(trade_date=tradedatelist[n+2])
    df_daily_1 = pd.DataFrame()
    df_daily_1['ts_code'] = df_1['ts_code']
    df_daily_1['high_1'] = df_1['high']
    df_daily_1['close_1'] = df_1['close']
    df_0 = pd.merge(df_0, df_daily_1, how='left', on='ts_code')
    #获取满足条件个股列表
    df_0['qk'] = (df_0['low']-df_0['high_1'])/df_0['high_1']
    df_0['high_close'] = df_0['high'] - df_0['close']    
    df_qk = df_0[df_0['qk']>0]
     
    df_qk = df_qk[df_qk['high_close']==0]
    #前一天涨幅<7%
    #df_qk = df_qk[df_qk['pct_chg']<7.0]    
    df_qk = df_qk.set_index('ts_code',drop=False, append=False, inplace=False, verify_integrity=False)
    qk_list = df_qk['ts_code'].tolist()
    #监控个股股价
    for stockcode in qk_list:
        #if(df_qk.at[stockcode,'close_1']>df.at[stockcode,'low']):
        print (stockcode)
    
#get_data_tpqk()    
    
    
    
    