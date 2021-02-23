# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 18:37:12 2020

@author: iFunk
"""
import pandas as pd
import talib
import math

from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#获取库数据函数 参数
def get_col_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df


#计算MA函数
def get_ma(df,maname,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise

#计算RSI函数
def get_rsi_talib(df,nday):
    close_arr = df['close'].values
    close_arr_reverse=close_arr[::-1]   
    rsi_serise = talib.RSI(close_arr,timeperiod=nday)
    #temp_serise.dropna(inplace=True)
    #rsi_serise = temp_serise.reset_index(drop=True)
    return rsi_serise

def get_rsi(df,nday):
    for i in range(len(df)):    
        if len(df)>nday:            
            change_history_arr = df['change'][i:nday+i].values
            chg_up = 0
            chg_down =0
            for change in change_history_arr:
                #上涨幅度之和
                if (change>0):
                    chg_up +=change
                #下跌幅度之和
                else:
                    chg_down += change
            #rs = (chg_up/chg_down)*100
            #A÷（A＋B）×100
            rsi = (chg_up/(chg_up-chg_down))*100
            df.loc[i,'RSI']=rsi
        else:
            df['RSI'] = None
    return df['RSI']
    
#计算MTM函数
def get_mom(df,nday):
    close_arr = df['close'].values
    #倒置数组 日期升序
    close_arr_reverse=close_arr[::-1]
    temp_arr = talib.MOM(close_arr_reverse,timeperiod=nday)
    #倒置 日期降序
    mom_arr = temp_arr[::-1]
    return mom_arr

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
    df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df['close'], fastperiod=12, fastmatype=1, slowperiod=26,    slowmatype=1, signalperiod=9, signalmatype=1)
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
    df['DIF']=pd.Series(a)-pd.Series(b)  
    #print(df['diff'])  
    for i in range(len(df)):  
        if i==0:  
            df.loc[i,'DEA']=df.loc[i,'DIF']  
        if i>0:  
            df.loc[i,'DEA']=((M-1)*df.loc[i-1,'DEA']+2*df.loc[i,'DIF'])/(M+1)  
    df['MACD']=2*(df['DIF']-df['DEA'])  
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
    df['upper'].dropna(inplace=True)
    df['upper'] = df['upper'].reset_index(drop=True)
    df['middle'].dropna(inplace=True)
    df['middle'] = df['middle'].reset_index(drop=True)
    df['lower'].dropna(inplace=True)
    df['lower'] = df['lower'].reset_index(drop=True)
    return df


def test(stockcode):
    df = get_col_df('daily_qfq_macd_'+stockcode)[0:500]
    df['MTM'] = get_mom(df,12)
    return df
#df = test('000001.SZ')