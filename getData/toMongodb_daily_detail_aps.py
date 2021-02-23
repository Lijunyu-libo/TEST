# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020

@author: 李博
"""
import tushare as ts
import pandas as pd
import talib

#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()
from pymongo import MongoClient

client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
#计算MA函数
def get_ma(df,maname,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise

#计算RSI函数
def get_rsi(df,nday):
    temp_serise = talib.RSI(df['close'],timeperiod=nday)
    temp_serise.dropna(inplace=True)
    rsi_serise = temp_serise.reset_index(drop=True)
    return rsi_serise

#计算MTM函数
def get_mom(df,nday):
    temp_serise = talib.MOM(df['close'],timeperiod=nday)
    temp_serise.dropna(inplace=True)
    mom_serise = temp_serise.reset_index(drop=True)
    return mom_serise

#计算MACD函数
def get_macd(df):
    df['MACD'],df['MACDsignal'],df['MACDhist'] = talib.MACD(df['close'],fastperiod=6,slowperiod=12,signalperiod=9)
    df['MACD'].dropna(inplace=True)
    df['MACD'] = df['MACD'].reset_index(drop=True)
    df['MACDsignal'].dropna(inplace=True)
    df['MACDsignal'] = df['MACDsignal'].reset_index(drop=True)
    df['MACDhist'].dropna(inplace=True)
    df['MACDhist'] = df['MACDhist'].reset_index(drop=True)
    return df

#计算MACDEXT函数
def get_macdext(df):
    df['DIFF'], df['DEA'],df['MACD'] = talib.MACDEXT(df['close'], fastperiod=12, fastmatype=1, slowperiod=26,    slowmatype=1, signalperiod=9, signalmatype=1)
    df['MACD'] = df['MACD'] * 2
    return df

def get_EMA(df,N):  
    for i in range(len(df)):  
        if i==0:  
            df.loc[i,'ema']=df.loc[i,'close']  
#            df.ix[i,'ema']=0  
        if i>0:  
            df.loc[i,'ema']=(2*df.loc[i,'close']+(N-1)*df.loc[i-1,'ema'])/(N+1)  
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
    #macd = df['get_macd_emasmacd'].values
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

#通过talib 计算BOLL函数
def get_boll(df):
    df['upper'], df['middle'], df['lower'] = talib.BBANDS(
                df.close.values, 
                timeperiod=20,
                # number of non-biased standard deviations from the mean
                nbdevup=2,
                nbdevdn=2,
                # Moving average type: simple moving average here
                matype=0)
    return df
#daily
def get_stocks_daily_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        #print (stockname)
        mycol = mydb['daily_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq')
        if (df.empty or len(df)<=30):
            print (stockname+' data empty')
        else:
            df['MA5'] = get_ma(df,'MA5',5)
            df['MA10'] = get_ma(df,'MA10',10)
            df['MA20'] = get_ma(df,'MA20',20)
            df['MA30'] = get_ma(df,'MA30',30)
            df['RSI6'] = get_rsi(df,6)
            df['RSI12'] = get_rsi(df,12)
            df['RSI24'] = get_rsi(df,24)
            df['MTM'] = get_mom(df,6)
            df_macd = get_macd_mas(df)
            df['MACD'] = df_macd['MACD']
            df['DIF'] = df_macd['DIF']
            df['DEA'] = df_macd['DEA']
            df_boll = get_boll(df)
            df['UPPER'] = df_boll['upper']
            df['MIDDLE'] = df_boll['middle']
            df['LOWER'] = df_boll['lower']
            mycol.insert_many(df.to_dict('records'))
            print ('daily_'+stockname+': '+str(len(df)))
#weekly
def get_stocks_weekly_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        mycol = mydb['weekly_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq',freq='W')
        if (df.empty):
            print (stockname+' data empty')
        else:
            mycol.insert_many(df.to_dict('records'))
            print ('weekly_'+stockname+': '+str(len(df)))
            
sched.add_job(get_stocks_daily_qfq,'cron',day_of_week='mon-fri', hour=23, minute=0)
sched.add_job(get_stocks_weekly_qfq,'cron',day_of_week='sat', hour=23, minute=0)
sched.start()