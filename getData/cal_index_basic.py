# -*- coding: utf-8 -*-
"""
Created on Sun Dec 27 09:22:59 2020
#入库基本指标计算
@author: iFunk
"""
import pandas as pd
import numpy as np
import talib
import math
import tushare as ts
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
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
def get_ma(df,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise

#计算MACD函数 使用talib ext
def get_macd_talib_macdext(df_qfq):
    #按照日期升序df
    df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
    #重新设置索引
    df_qfq_asc.reset_index(drop=True, inplace=True)
    #初始化CDMA df
    df = pd.DataFrame()
    df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
    df['MACD'] = df['MACD'] * 2
    #按照index反序
    df = df.sort_index(ascending=False)
    #重新设置索引
    df.reset_index(drop=True, inplace=True)
    #赋值series给df_qfq
    df_qfq['DIF'] = df['DIF']
    df_qfq['DEA'] = df['DEA']
    df_qfq['MACD'] = df['MACD']
    return df_qfq

#计算CCI函数 使用TALIB
def get_cci_talib_cci(df_qfq):
    #按照日期升序df
    df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
    #重新设置索引
    df_qfq_asc.reset_index(drop=True, inplace=True)
    CCI_serise = talib.CCI(df_qfq_asc['high'], df_qfq_asc['low'], df_qfq_asc['close'], timeperiod=14)
    #按照index反序
    CCI_serise = CCI_serise.sort_index(ascending=False)
    #重新设置索引
    CCI_serise.reset_index(drop=True, inplace=True)
    df_qfq['CCI'] = CCI_serise
    return df_qfq

#计算MTM函数 使用TALIB
def get_mtm_talib_mom(df_qfq):
    close_arr = df_qfq['close'].values
    #倒置数组 日期升序
    close_arr_reverse=close_arr[::-1]
    temp_arr = talib.MOM(close_arr_reverse,timeperiod=12)
    #倒置数组 日期降序
    mom_arr = temp_arr[::-1]
    df_qfq['MTM'] = mom_arr
    temp_serise = df_qfq['MTM'].rolling(6).mean()
    temp_serise.dropna(inplace=True)
    mamtm_serise = temp_serise.reset_index(drop=True)
    df_qfq['MAMTM'] = mamtm_serise
    return df_qfq

#计算BOLL函数 使用TALIB
def get_boll_talib_bbands(df):
    df['BOLL_UPPER'], df['BOLL_MIDDLE'], df['BOLL_LOWER'] = talib.BBANDS(
                df.close.values, 
                timeperiod=20,
                # number of non-biased standard deviations from the mean
                nbdevup=2,
                nbdevdn=2,
                # Moving average type: simple moving average here
                matype=0)
    df['BOLL_UPPER'].dropna(inplace=True)
    df['BOLL_UPPER'] = df['BOLL_UPPER'].reset_index(drop=True)
    df['BOLL_MIDDLE'].dropna(inplace=True)
    df['BOLL_MIDDLE'] = df['BOLL_MIDDLE'].reset_index(drop=True)
    df['BOLL_LOWER'].dropna(inplace=True)
    df['BOLL_LOWER'] = df['BOLL_LOWER'].reset_index(drop=True)    
    return df

#通过函数计算KDJ函数
def get_kdj(df):
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
    #按照日期降序df
    df = df.sort_values(by="trade_date",ascending=False)
    #重新设置索引
    df.reset_index(drop=True, inplace=True)
    return df

#通过函数计算OBV函数
def get_obv(df):
    #按照日期升序df
    df_qfq_asc = df.sort_values(by="trade_date",ascending=True)
    #重新设置索引
    df_qfq_asc.reset_index(drop=True, inplace=True)
    df_qfq_asc = df_qfq_asc.dropna(axis=0, how='any')
    CLOSE = np.array(df_qfq_asc['close'])
    #HIGH = np.array(df_qfq_asc['high'])
    #LOW = np.array(df_qfq_asc['low'])
    VOL = np.array(df_qfq_asc['vol'])
    CHANGE = np.diff(CLOSE)
    SIGNS = np.sign(CHANGE)
    VOL_SIGN = VOL[1:]*SIGNS
    VOLS = np.concatenate(([VOL[0]],VOL_SIGN),axis=0)
    #OBV
    OBVS = np.cumsum(VOLS)
    OBVS_R = OBVS[::-1]
    df['OBV'] = pd.Series(OBVS_R)
    #MAOBV
    #df['MAOBV']=((nonan_df['close']-nonan_df['low'])-(nonan_df['high']-nonan_df['close']))/(nonan_df['high']-nonan_df['low'])*nonan_df['vol']
    #df['MAOBVS'] = df['MAOBV'].cumsum()
    #MAOBVS = np.array(df['MAOBVS'])
    #MAOBVS_R = MAOBVS[::-1]
    return df

def get_obv_talib_OBV(df):
    close_arr = df['close'].values
    vol_arr = df['vol'].values
    #倒置数组 日期升序
    close_arr_reverse = close_arr[::-1]
    vol_arr_reverse = vol_arr[::-1]    
    temp_arr = talib.OBV(close_arr_reverse, vol_arr_reverse)
    #倒置数组 日期降序
    obv_arr = temp_arr[::-1]
    df['OBV'] = obv_arr
    temp_serise = df['OBV'].rolling(30).mean()
    temp_serise.dropna(inplace=True)
    maobv_serise = temp_serise.reset_index(drop=True)
    df['MAOBV'] = maobv_serise
    return df

#计算RSI函数
def get_rsi_talib_RSI(df):
    close_arr = df['close'].values
    close_arr_reverse=close_arr[::-1]   
    temp_6_arr = talib.RSI(close_arr_reverse,timeperiod=6)
    rsi_6_arr = temp_6_arr[::-1]
    df['RSI6'] = rsi_6_arr
    temp_12_arr = talib.RSI(close_arr_reverse,timeperiod=12)
    rsi_12_arr = temp_12_arr[::-1]
    df['RSI12'] = rsi_12_arr
    temp_24_arr = talib.RSI(close_arr_reverse,timeperiod=24)
    rsi_24_arr = temp_24_arr[::-1]
    df['RSI24'] = rsi_24_arr
    return df

#计算RSI函数
def get_vrsi_talib_RSI(df):
    vol_arr = df['vol'].values
    vol_arr_reverse=vol_arr[::-1]   
    temp_6_arr = talib.RSI(vol_arr_reverse,timeperiod=6)
    vrsi_6_arr = temp_6_arr[::-1]
    df['VRSI6'] = vrsi_6_arr
    temp_12_arr = talib.RSI(vol_arr_reverse,timeperiod=12)
    vrsi_12_arr = temp_12_arr[::-1]
    df['VRSI12'] = vrsi_12_arr
    temp_24_arr = talib.RSI(vol_arr_reverse,timeperiod=24)
    vrsi_24_arr = temp_24_arr[::-1]
    df['VRSI24'] = vrsi_24_arr
    return df

def test(stockcode):
    df = ts.pro_bar(ts_code=stockcode,adj='qfq')
    #MA
    df['MA5'] = get_ma(df,5)
    df['MA10'] = get_ma(df,10)
    df['MA20'] = get_ma(df,20)
    df['MA30'] = get_ma(df,30)
    #MACD
    df = get_macd_talib_macdext(df)
    #KDJ
    df = get_kdj(df)
    #BOLL
    df = get_boll_talib_bbands(df)
    #CCI
    df = get_cci_talib_cci(df)
    #MTM
    df = get_mtm_talib_mom(df)
    #OBV
    #df = get_obv(df)
    df = get_obv_talib_OBV(df)
    #VRSI
    df = get_vrsi_talib_RSI(df)
    return df
df = test('000001.SZ')    