# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 17:36:39 2020
KDJ策略
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
import caltools
#import tradememo
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
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

             
#获取stockcode条件集合函数 参数 stockcode
def get_data_stockcode(col,stockcode):
    mycollection=mydb[col]
    query = {'ts_code':stockcode}
    index_rs = mycollection.find(query)
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json
#获取stockcode条件集合函数 参数 col 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#当前交易日日KDJ数据
def get_stocks_daily_kdj():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW) 
    #df_stocks = get_stockbasket('','')
    textlist = ['000001.SZ','000002.SZ']
    for stockcode in textlist:#df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<10):
            continue
        else:
            print (stockcode,df_qfq['trade_date'][0],round(df_qfq['K'][0],3),round(df_qfq['D'][0],3),round(df_qfq['J'][0],3))
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
get_stocks_daily_kdj()

#当前交易日KDJ有效金叉
def get_stocks_daily_kdj_cross():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<10):
            continue
        else:
            if (df_qfq['vol'][1]<df_qfq['vol'][0]):
                if (df_qfq['J'][1]<df_qfq['D'][1] and df_qfq['J'][1]<df_qfq['K'][1]):
                    if (df_qfq['J'][0]>df_qfq['D'][0] and df_qfq['J'][0]>df_qfq['K'][0]):
                        print (stockcode,df_qfq['trade_date'][0])
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                        result_dict['trigger_close'] = df_qfq['close'][0]
                        result_dict['reason'] = FNAME
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df 
#get_stocks_daily_kdj_cross()

#MACD0轴上金叉，KDJ J大于KD 小于80
def get_stocks_daily_kdj_j_up_80_macd_cross_abovezero():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<10):
            continue
        else:
            if (df_qfq['DIF'][0]>0 and df_qfq['DIF'][0]>df_qfq['DEA'][0] and df_qfq['DIF'][1]<df_qfq['DEA'][1]):
                if (df_qfq['J'][0]<80 and df_qfq['J'][0]>df_qfq['D'][0] and df_qfq['J'][0]>df_qfq['K'][0]):
                    print (stockcode,df_qfq['trade_date'][0])
                    result_dict['ts_code'] = stockcode
                    result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                    result_dict['trigger_close'] = df_qfq['close'][0]
                    result_dict['reason'] = FNAME
                    result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df            

#MACD0轴上金叉，KDJ J大于KD 
def get_stocks_daily_kdj_j_up_macd_cross_abovezero():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<10):
            continue
        else:
            if (df_qfq['DIF'][0]>0 and df_qfq['DIF'][1]<0 and df_qfq['DIF'][0]>df_qfq['DEA'][0] and df_qfq['DIF'][1]==df_qfq['DEA'][1]):
                if (df_qfq['J'][0]>df_qfq['D'][0] and df_qfq['J'][0]>df_qfq['K'][0]):
                    print (stockcode,df_qfq['trade_date'][0])
                    result_dict['ts_code'] = stockcode
                    result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                    result_dict['trigger_close'] = df_qfq['close'][0]
                    result_dict['reason'] = FNAME
                    result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df  
#get_stocks_daily_kdj_j_up_macd_cross_abovezero()
