# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 17:35:40 2020
RSI 相对强弱指标策略
@author: iFunk
"""
#BASIC
import datetime
import time
import json
#MATH
import pandas as pd
import numpy as np
import talib
import math
# MONGODB CONNECT
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
#client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
#TS INITAIL
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data
#获取stockcode条件集合函数 参数 stockcode 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

#计算RSI函数
def get_rsi(df,nday):
    temp_serise = talib.RSI(df['close'],timeperiod=nday)
    temp_serise.dropna(inplace=True)
    rsi_serise = temp_serise.reset_index(drop=True)
    return rsi_serise

#最近交易日个股日RSI金叉 and RSI<30策略
def get_stocks_daily_rsi_cross():
    result_list=[]
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<30):
            continue
        else:
            df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
            nonan_df = df_qfq_asc.dropna(axis=0, how='any')
            nonan_df.reset_index(drop=True, inplace=True)
            RSI6 = talib.RSI(nonan_df['close'],timeperiod=6)
            RSI12 = talib.RSI(nonan_df['close'],timeperiod=12)
            RSI24 = talib.RSI(nonan_df['close'],timeperiod=24)
            df = pd.DataFrame()
            df['RSI6'] = RSI6
            df['RSI12'] = RSI12
            df['RSI24'] = RSI24
            df = df.sort_index(ascending=False)
            df.reset_index(drop=True, inplace=True)
            if (df.at[1,'RSI6']<30):
                if(df.at[1,'RSI6']<df.at[1,'RSI12']<df.at[1,'RSI24'] and df.at[0,'RSI6']>df.at[0,'RSI12']>df.at[0,'RSI24']):
                    print (stockcode)
#get_stocks_daily_rsi_cross()