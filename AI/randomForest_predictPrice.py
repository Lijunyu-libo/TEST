# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
通过随机森林模型 预测未来股价涨跌
@author: 李博
"""
import os
import numpy as np
import pandas as pd
import math
import matplotlib.pyplot as plt
import json
import datetime
import time
import talib
from sklearn.ensemble import RandomForestClassifier #分类决策树模型
from sklearn.metrics import accuracy_score #引入预测准确度评分函数

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

#获取数据
df = pro.daily(ts_code='000001.SZ')
df['close-open'] = (df['close']-df['open'])/df['open']
df['high-low'] = (df['high']-df['low'])/df['low']
df['price_change'] = df['close']-df['pre_close']
df['MA5'] = get_ma(df,'MA5',5)
df['MA10'] = get_ma(df,'MA10',10)
df['MA20'] = get_ma(df,'MA20',20)
df['MA30'] = get_ma(df,'MA30',30)
df['RSI6'] = get_rsi(df,6)
df['RSI12'] = get_rsi(df,12)
df['RSI24'] = get_rsi(df,24)
df['MTM'] = get_mom(df,6)
df2 = get_macd_mas(df)
df3 = df2[0:4900]
df3=df3.set_index('trade_date')
#df3.sort_index(axis=0, ascending=True)
df3.to_csv('macd_index.csv')

#提取特征变量和目标变量
#X = df3[['close','vol','close-open','high-low','price_change','pct_chg','MA5','MA10','MA20','MA30','RSI6','RSI12','RSI24','MTM','MACD']]
X = df3[['close','vol','close-open','high-low','price_change','pct_chg','MA5','MA10','RSI6','MTM','MACD']]
y = np.where(df3['price_change'].shift(-1)>0,1,-1)

#划分训练集和测试集
X_length = X.shape[0]
split = int(X_length*0.1)
X_test,X_train = X[:split],X[split:]
y_test,y_train = y[:split],y[split:]

#模型搭建
model = RandomForestClassifier(max_depth=3,n_estimators=10,min_samples_leaf=10,random_state=1)
model.fit(X_train,y_train)

#模型使用
y_pred = model.predict(X_test)
result = pd.DataFrame()
#result['交易日'] = list(y_test)
result['预测值'] = list(y_pred)
result['实际值'] = list(y_test)
result['概率'] = list(model.predict_proba(X_test))
result.to_csv('result.csv')

#计算收益率
X_test['prediction'] = model.predict(X_test)
X_test['p_change'] = (X_test['close']-X_test['close'].shift(1))/X_test['close'].shift(1)
X_test['origin'] = (1+X_test['p_change']).cumprod()
X_test['strategy'] = (X_test['prediction'].shift(1)*X_test['p_change']+1).cumprod()
X_test.to_csv('result_p.csv')

#特征值分析
score = accuracy_score(y_pred,y_test)
model.score(X_test,y_test)
features = X.columns
importances = model.feature_importances_
result_f = pd.DataFrame()
result_f['特征值'] = features
result_f['特征重要性'] = importances
result_f = result_f.sort_values('特征重要性',ascending=False)
result_f.to_csv('result_f.csv')

'''
#获取指数 日线
for index in indexlist:
    df = pro.index_daily(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    df['MA5'] = get_ma(df,'MA5',5)
    df['MA10'] = get_ma(df,'MA10',10)
    df['MA20'] = get_ma(df,'MA20',20)
    df['MA30'] = get_ma(df,'MA30',30)
    df['RSI6'] = get_rsi(df,6)
    df['RSI12'] = get_rsi(df,12)
    df['RSI24'] = get_rsi(df,24)
    df['MTM'] = get_mom(df,6)
    df_macd = get_macd_emas(df)
    df['MACD'] = df_macd['MACD']
    df['DIF'] = df_macd['DIF']
    df['DEA'] = df_macd['DEA']  
    #print (df)
    df.to_csv(index+'.csv')

#获取指数 周线
for index in indexlist:
    df = pro.index_weekly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    df['MA5'] = get_ma(df,'MA5',5)
    df['MA10'] = get_ma(df,'MA10',10)
    df['MA20'] = get_ma(df,'MA20',20)
    df['MA30'] = get_ma(df,'MA30',30)
    #print (df)
    df.to_csv(index+'_weekly.csv')   

#获取指数 月线
for index in indexlist:
    df = pro.index_monthly(ts_code=index, start_date=get_day_time(730), end_date=get_day_time(0))
    df['MA5'] = get_ma(df,'MA5',5)
    df['MA10'] = get_ma(df,'MA10',10)
    df['MA20'] = get_ma(df,'MA20',20)
    df['MA30'] = get_ma(df,'MA30',30)
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