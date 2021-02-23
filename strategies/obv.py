# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 09:41:13 2020
OBV 积累量能策略
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

#最近交易日个股日OBV金叉策略
def get_stocks_daily_obv_cross():
    result_list=[]
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        #df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        df_qfq = ts.pro_bar(ts_code=stockcode, adj='qfq')
        #df_qfq = ts.pro_bar(ts_code='000001.SZ')
        df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
        nonan_df = df_qfq_asc.dropna(axis=0, how='any')
        df_qfq_asc.reset_index(drop=True, inplace=True)
        CLOSE = np.array(nonan_df['close'])
        HIGH = np.array(nonan_df['high'])
        LOW = np.array(nonan_df['low'])
        VOL = np.array(nonan_df['vol'])
        CHANGE = np.diff(CLOSE)
        SIGNS = np.sign(CHANGE)
        VOL_SIGN = VOL[1:]*SIGNS
        VOLS = np.concatenate(([VOL[0]],VOL_SIGN),axis=0)
        #OBV
        OBVS = np.cumsum(VOLS)
        OBVS_R = OBVS[::-1]
        df = pd.DataFrame()
        df['OBV'] = pd.Series(OBVS)
        #MAOBV
        df['MAOBV']=((nonan_df['close']-nonan_df['low'])-(nonan_df['high']-nonan_df['close']))/(nonan_df['high']-nonan_df['low'])*nonan_df['vol']
        #MAOBV = ((CLOSE*2-LOW-HIGH)*VOL)/(HIGH-LOW)
        #MAOBVS = np.cumsum(MAOBV)
        #print (VOL,HIGH,LOW,MAOBV)
        df['MAOBVS'] = df['MAOBV'].cumsum()
        MAOBVS = np.array(df['MAOBVS'])
        MAOBVS_R = MAOBVS[::-1]
        print (stockcode,OBVS_R[0],MAOBVS_R[0],OBVS_R[1],MAOBVS_R[1])
        if (OBVS_R[0]>MAOBVS_R[0] and OBVS_R[1]<MAOBVS_R[1]):
            print ('OK',stockcode)
            result_list.append(stockcode)
    print (result_list)
#get_stocks_daily_obv_cross()