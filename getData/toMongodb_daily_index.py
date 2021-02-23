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
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
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
def get_pb(indexcode):
    df = pro.index_dailybasic(ts_code=indexcode)
    return df
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

#获取指数 日线
def get_indexs_daily(indexlist):
    for indexcode in indexlist:
        df = pro.index_daily(ts_code=indexcode, start_date=get_day_time(730), end_date=get_day_time(0))
        df['high_low'] = df['high']-df['low']
        df['pct_high_low'] = ((df['high']-df['low'])/df['close']*100).round(decimals=2)
        df['amount_w'] = (df['amount']/10).round(decimals=2)
        df['amount_y'] = (df['amount']/100000).round(decimals=2)
        df['pre_amount_w'] = df['amount_w'].shift(-1)
        df['amount_change'] = (df['amount_w']-df['pre_amount_w']).round(decimals=2)
        df['pct_amount_change'] = (df['amount_change']/df['pre_amount_w']*100).round(decimals=2)
        #计算前3日成交金额
        df['amount_w_1'] = df['amount_w'].shift(-1)
        df['amount_w_2'] = df['amount_w'].shift(-2)
        df['amount_w_3'] = df['amount_w'].shift(-3)
        #计算前3日成交金额差额
        df['amount_change_1'] = df['amount_w']-df['amount_w_1']
        df['amount_change_2'] = df['amount_w_1']-df['amount_w_2']
        df['amount_change_3'] = df['amount_w_2']-df['amount_w_3']
        #计算前3日成交金额差额比
        df['pct_amount_change_1'] = (df['amount_change_1']/df['amount_w_1']*100).round(decimals=2)
        df['pct_amount_change_2'] = (df['amount_change_2']/df['amount_w_2']*100).round(decimals=2)
        df['pct_amount_change_3'] = (df['amount_change_3']/df['amount_w_3']*100).round(decimals=2)
        #判断前3日成交量涨跌
        df['amount_up'] = df.apply(get_3days_up,axis=1)
        df_dailybasic = get_pb(indexcode)
        df['PB'] = df_dailybasic['pb']
        df['MA5'] = get_ma(df,'MA5',5)
        df['MA10'] = get_ma(df,'MA10',10)
        df['MA20'] = get_ma(df,'MA20',20)
        df['MA30'] = get_ma(df,'MA30',30)
        df['RSI6'] = get_rsi(df,6)
        df['RSI12'] = get_rsi(df,12)
        df['RSI24'] = get_rsi(df,24)
        df['MTM'] = get_mom(df,6)
        df['CCI'] = get_cci(df)
        df_macd = get_macd_mas(df)
        df['MACD'] = df_macd['MACD']
        df['DIF'] = df_macd['DIF']
        df['DEA'] = df_macd['DEA']
        df_kjd = get_kdj(df)
        df['K'] = df_kjd['K']
        df['D'] = df_kjd['D']
        df['J'] = df_kjd['J']
        df_boll = get_boll(df)
        df['BOLL_UPPER'] = df_boll['boll_upper']
        df['BOLL_MIDDLE'] = df_boll['boll_middle']
        df['BOLL_LOWER'] = df_boll['boll_lower']
        #print (df)
        df.to_csv('./data/indexs/'+indexcode+'.csv')

#获取指数 周线
def get_indexs_weekly(indexlist):
    for index in indexlist:
        df = pro.index_weekly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
        df['high_low'] = df['high']-df['low']
        df['pct_high_low'] = ((df['high']-df['low'])/df['close']*100).round(decimals=2)
        df['amount_w'] = (df['amount']/10000).round(decimals=2)
        df['amount_y'] = (df['amount']/100000000).round(decimals=2)
        df['pre_amount_y'] = df['amount_y'].shift(-1)
        df['amount_change'] = (df['amount_y']-df['pre_amount_y']).round(decimals=2)
        df['pct_amount_change'] = (df['amount_change']/df['pre_amount_y']*100).round(decimals=2)
        #计算前3日成交金额
        df['amount_w_1'] = df['amount_w'].shift(-1)
        df['amount_w_2'] = df['amount_w'].shift(-2)
        df['amount_w_3'] = df['amount_w'].shift(-3)
        #计算前3日成交金额差额
        df['amount_change_1'] = df['amount_w']-df['amount_w_1']
        df['amount_change_2'] = df['amount_w_1']-df['amount_w_2']
        df['amount_change_3'] = df['amount_w_2']-df['amount_w_3']
        #计算前3日成交金额差额比
        df['pct_amount_change_1'] = (df['amount_change_1']/df['amount_w_1']*100).round(decimals=2)
        df['pct_amount_change_2'] = (df['amount_change_2']/df['amount_w_2']*100).round(decimals=2)
        df['pct_amount_change_3'] = (df['amount_change_3']/df['amount_w_3']*100).round(decimals=2)
        #判断前3日成交量涨跌
        df['amount_up'] = df.apply(get_3days_up,axis=1)
        df['MA5'] = get_ma(df,'MA5',5)
        df['MA10'] = get_ma(df,'MA10',10)
        df['MA20'] = get_ma(df,'MA20',20)
        df['MA30'] = get_ma(df,'MA30',30)
        #print (df)
        df.to_csv('./data/indexs/'+index+'_weekly.csv')   

#获取指数 月线
def get_indexs_monthly(indexlist):
    for index in indexlist:
        df = pro.index_monthly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
        df['high_low'] = df['high']-df['low']
        df['pct_high_low'] = ((df['high']-df['low'])/df['close']*100).round(decimals=2)
        df['amount_w'] = (df['amount']/10000).round(decimals=2)
        df['amount_y'] = (df['amount']/100000000).round(decimals=2)
        df['pre_amount_y'] = df['amount_y'].shift(-1)
        df['amount_change'] = (df['amount_y']-df['pre_amount_y']).round(decimals=2)
        df['pct_amount_change'] = (df['amount_change']/df['pre_amount_y']*100).round(decimals=2)
        #计算前3日成交金额
        df['amount_w_1'] = df['amount_w'].shift(-1)
        df['amount_w_2'] = df['amount_w'].shift(-2)
        df['amount_w_3'] = df['amount_w'].shift(-3)
        #计算前3日成交金额差额
        df['amount_change_1'] = df['amount_w']-df['amount_w_1']
        df['amount_change_2'] = df['amount_w_1']-df['amount_w_2']
        df['amount_change_3'] = df['amount_w_2']-df['amount_w_3']
        #计算前3日成交金额差额比
        df['pct_amount_change_1'] = (df['amount_change_1']/df['amount_w_1']*100).round(decimals=2)
        df['pct_amount_change_2'] = (df['amount_change_2']/df['amount_w_2']*100).round(decimals=2)
        df['pct_amount_change_3'] = (df['amount_change_3']/df['amount_w_3']*100).round(decimals=2)
        #判断前3日成交量涨跌
        df['amount_up'] = df.apply(get_3days_up,axis=1)
        df['MA5'] = get_ma(df,'MA5',5)
        df['MA10'] = get_ma(df,'MA10',10)
        df['MA20'] = get_ma(df,'MA20',20)
        df['MA30'] = get_ma(df,'MA30',30)
        #print (df)
        df.to_csv('./data/indexs/'+index+'_monthly.csv')       
#入库
from pymongo import MongoClient
print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open('./data/indexs/'+filename+'.csv') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)
    print (collectionname,filename)

#执行入库
def get_indexs_data():
    get_indexs_monthly(indexlist)
    get_indexs_weekly(indexlist)
    get_indexs_daily(indexlist)
    for index in indexlist:
        toMongodb(index,index)
        
    for index in indexlist:
        toMongodb(index+'_weekly',index+'_weekly')
    
    for index in indexlist:
        toMongodb(index+'_monthly',index+'_monthly')

#get_indexs_data()

tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/indexs')
async def get_indexs(request:Request):
    indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
    get_indexs_monthly(indexlist)
    get_indexs_weekly(indexlist)
    get_indexs_daily(indexlist)
    for index in indexlist:
        toMongodb(index,index)
    for index in indexlist:
        toMongodb(index+'_weekly',index+'_weekly')
    for index in indexlist:
        toMongodb(index+'_monthly',index+'_monthly')
    return tmp.TemplateResponse('update_indexs.html',
                                {'request':request
                                 })