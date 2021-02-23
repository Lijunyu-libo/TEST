# -*- coding: utf-8 -*-
"""
Created on Tue Sep  8 18:37:12 2020

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
    df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
    df['MACD'] = df['MACD'] * 2
    df['MACD'].dropna(inplace=True)
    df['MACD'] = df['MACD'].reset_index(drop=True)
    df['DIF'].dropna(inplace=True)
    df['DIF'] = df['DIF'].reset_index(drop=True)
    df['DEA'].dropna(inplace=True)
    df['DEA'] = df['DEA'].reset_index(drop=True)
    #交易日降序排列
    df = df.sort_values(by="trade_date",ascending=False)
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
    ma12 = talib.MA(df['close'],timeperiod=12)
    ma12.dropna(inplace=True)
    df['MA12']  = ma12.reset_index(drop=True)
    ma26 = talib.MA(df['close'],timeperiod=26)
    ma26.dropna(inplace=True)
    df['MA26'] =  ma26.reset_index(drop=True)
    df['DIF'] = df['MA12']-df['MA26']
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

import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()

#通过talib计算KJD函数 不一致
def get_kdj_talib():
    df = ts.pro_bar(ts_code='002410.SZ')
    #按照日期升序df
    df_qfq_asc = df.sort_values(by="trade_date",ascending=True)
    #重新设置索引
    df_qfq_asc.reset_index(drop=True, inplace=True)
    #初始化CDMA df
    dw = pd.DataFrame()
    # KDJ 值对应的函数是 STOCH
    #slowk_matype=1 #MA_Type: 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA, 7=MAMA, 8=T3 (Default=SMA)
    dw['K'], dw['D'] = talib.STOCH(
            df_qfq_asc['high'].values, 
			df_qfq_asc['low'].values, 
			df_qfq_asc['close'].values,
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
#通过函数计算KJD函数
def get_kdj(df):
    #df = ts.pro_bar(ts_code='000001.SZ')
    #按照日期升序df
    df = df.sort_values(by="trade_date",ascending=True)
    #重新设置索引
    df.reset_index(drop=True, inplace=True)
    low_list = df['low'].rolling(9, min_periods=9).min()
    low_list.fillna(value=df['low'].expanding().min(), inplace=True)
    high_list = df['high'].rolling(9, min_periods=9).max()
    high_list.fillna(value=df['high'].expanding().max(), inplace=True)
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['K'] = pd.DataFrame(rsv).ewm(com=2).mean()
    df['D'] = df['K'].ewm(com=2).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    df = df.sort_values(by="trade_date",ascending=False)
    #重新设置索引
    df.reset_index(drop=True, inplace=True)
    return df
#df = get_kdj()