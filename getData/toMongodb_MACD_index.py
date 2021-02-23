# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取 000001 399001 399005 399006 指数日线数据
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time
import talib

# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取历史交易日
#df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(180), end_date=get_day_time(1))
#print (df['cal_date'])

indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']


#通过TALIB.MACD计算MACD函数
def get_macd(df):
    df['MACD'],df['MACDsignal'],df['MACDhist'] = talib.MACD(df['close'],fastperiod=6,slowperiod=12,signalperiod=9)
    df['MACD'].dropna(inplace=True)
    df['MACD'] = df['MACD'].reset_index(drop=True)
    df['MACDsignal'].dropna(inplace=True)
    df['MACDsignal'] = df['MACDsignal'].reset_index(drop=True)
    df['MACDhist'].dropna(inplace=True)
    df['MACDhist'] = df['MACDhist'].reset_index(drop=True)
    return df

#通过TALIB.MACDEXT函数计算MACD
def get_macdext(df):
    df['DIFF'], df['DEA'],df['MACD'] = talib.MACDEXT(df['close'], fastperiod=12, fastmatype=1, slowperiod=26,    slowmatype=1, signalperiod=9, signalmatype=1)
    df['MACD'] = df['MACD'] * 2
    return df

#通过公式计算MACD
def get_EMA(df,N):  
    for i in range(len(df)):  
        if i==0:  
            df.loc[i,'ema']=df.loc[i,'close']  
            #df.loc[i,'ema']=0  
        if i>0:  
            #df.loc[i,'ema']=(2*df.loc[i,'close']+(N-1)*df.loc[i,'close'])/(N+1)
            a = 2/(N+1)
            df.loc[i,'mea'] = a*df.loc[i,'close']+(1-a)*df.loc[i,'close']
    ema=list(df['ema'])  
    return ema  
  
def get_MACDS(df,short=12,long=26,M=9):  
    a=get_EMA(df,short)  
    b=get_EMA(df,long)  
    df['diff']=pd.Series(a)-pd.Series(b)  
    #print(df['diff'])  
    for i in range(len(df)):  
        if i==0:  
            df.loc[i,'dea']=df.loc[i,'diff']  
        if i>0:  
            df.loc[i,'dea']=((M-1)*df.loc[i-1,'dea']+2*df.loc[i,'diff'])/(M+1)  
    df['macd']=2*(df['diff']-df['dea'])  
    #diff = df['diff'].values
    #dea = df['dea'].values
    #macd = df['macd'].values
    return df

#通过TALIB.EMA计算MACD函数
def get_macd_emas(df):
    ema12 = talib.EMA(df['close'],timeperiod=12)
    ema12.dropna(inplace=True)
    df['EMA12']  = ema12.reset_index(drop=True)
    ema26 = talib.EMA(df['close'],timeperiod=26)
    ema26.dropna(inplace=True)
    df['EMA26'] =  ema26.reset_index(drop=True)
    df['DIF'] = df['EMA12']-df['EMA26']
    dea = talib.EMA(df['DIF'],timeperiod=9)
    dea.dropna(inplace=True)
    df['DEA'] =  dea.reset_index(drop=True)
    df['MACD'] = 2*(df['DIF']-df['DEA'])
    return df

#通过talib.MA函数计算MACD函数
def get_macd_mas(df):
    ema12 = talib.MA(df['close'],timeperiod=12)
    ema12.dropna(inplace=True)
    df['EMA12']  = ema12.reset_index(drop=True)
    ema26 = talib.MA(df['close'],timeperiod=26)
    ema26.dropna(inplace=True)
    df['EMA26'] =  ema26.reset_index(drop=True)
    df['DIF'] = df['EMA12']-df['EMA26']
    dea = talib.MA(df['DIF'],timeperiod=9)
    dea.dropna(inplace=True)
    df['DEA'] =  dea.reset_index(drop=True)
    df['MACD'] = 2*(df['DIF']-df['DEA'])
    return df

#通过talib.CCI计算CCI函数
def get_cci(df):
    cci = talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)
    df['CCI'] = cci
    return df
'''
#获取指数 日线
for index in indexlist:
    df = pro.index_daily(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    df_macd = get_macd(df)
    df['MACD'] = df_macd['MACD']
    df['MACDsignal'] = df_macd['MACDsignal']
    df['MACDhist'] = df_macd['MACDhist']  
    #print (df)
    df.to_csv(index+'.csv')

#获取指数 周线
for index in indexlist:
    df = pro.index_weekly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    #print (df)
    df.to_csv(index+'_weekly.csv')   

#获取指数 月线
for index in indexlist:
    df = pro.index_monthly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    #print (df)
    df.to_csv(index+'_monthly.csv')       
#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#执行入库
for index in indexlist:
    toMongodb(index,index)
    
for index in indexlist:
    toMongodb(index+'_weekly',index+'_weekly')

for index in indexlist:
    toMongodb(index+'_monthly',index+'_monthly')
'''
df = pro.daily(ts_code='000001.SZ')
#df = pro.index_daily(ts_code='000001.SH', start_date=get_day_time(730), end_date=get_day_time(0))
#df2 = get_macdext(df)
#df2 = get_MACDS(df)
#df2 = get_macd(df)
#df2['MACD'].dropna(inplace=True)
#df2['MACD'] = df2['MACD'].reset_index(drop=True)
df2 = get_macd_mas(df)
df2.to_csv('macd_index.csv')