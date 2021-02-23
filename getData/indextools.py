# -*- coding: utf-8 -*-
"""
Created on Tue Aug 25 09:39:14 2020

@author: iFunk
"""
import pandas as pd
import talib
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

#通过talib.CCI计算CCI函数
def get_cci(df):
    temp_serise = talib.CCI(df['high'], df['low'], df['close'], timeperiod=14)
    temp_serise.dropna(inplace=True)
    cci_serise = temp_serise.reset_index(drop=True)
    return cci_serise

#通过talib计算KJD函数
def get_kdj(df):
    dw = pd.DataFrame()
    # KDJ 值对应的函数是 STOCH
    dw['K'], dw['D'] = talib.STOCH(
            df['high'].values, 
			df['low'].values, 
			df['close'].values,
            fastk_period=9,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=3,
            slowd_matype=0)
    # 求出J值，J = (3*K)-(2*D)
    dw['J'] = list(map(lambda x,y: 3*x-2*y, dw['K'], dw['D']))
    dw['K'].dropna(inplace=True)
    dw['K'] = dw['K'].reset_index(drop=True)
    dw['D'].dropna(inplace=True)
    dw['D'] = dw['D'].reset_index(drop=True)
    dw['J'].dropna(inplace=True)
    dw['J'] = dw['J'].reset_index(drop=True)
    #dw.index = range(len(dw))
    return dw

#通过talib计算布林线函数
def get_boll(df):
    dw = pd.DataFrame()
    dw['boll_upper'], dw['boll_middle'], dw['boll_lower'] = talib.BBANDS(
                df['close'].values, 
                timeperiod=20,
                # number of non-biased standard deviations from the mean
                nbdevup=2,
                nbdevdn=2,
                # Moving average type: simple moving average here
                matype=0)
    dw['boll_upper'].dropna(inplace=True)
    dw['boll_upper'] = dw['boll_upper'].reset_index(drop=True)
    dw['boll_middle'].dropna(inplace=True)
    dw['boll_middle'] = dw['boll_middle'].reset_index(drop=True)
    dw['boll_lower'].dropna(inplace=True)
    dw['boll_lower'] = dw['boll_lower'].reset_index(drop=True)
    return dw
    
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

#判断3日参数涨跌
def get_3days_up(series):
    n1 = series["pct_amount_change_1"]
    n2 = series["pct_amount_change_2"]
    n3 = series["pct_amount_change_3"]
    up_data = 0
    if(n1>0):
        up_data= 1
        if(n1>0 and n2>0 and n3>0):
            up_data = 3
        if(n1>0 and n2>0 and n3<0):
            up_data = 2
    return up_data