# -*- coding: utf-8 -*-
"""
Created on Fri Oct 23 11:07:26 2020
塑性一阶模型计算塑性系数
@author: iFunk
"""
import sys
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import inspect
import statsmodels.api as sm
#import caltools
#import tradememo
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
from tushare.util import formula as fm
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

#数据库查询函数 参数 col 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode

#数据库查询函数 条件参数 col,param,paramvalue 返回df
def get_col_param_df(col,param,paramvalue):
    mycollection=mydb[col]
    #查询条件
    query = {param:paramvalue}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    df_col = pd.DataFrame(list_col)
    return df_col

#计算MA函数
def cal_ma(df,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise
def cal_ma_ta(df,n):
    temp_serise = talib.MA(df['close'],timeperiod=n)
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise

#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

'''
#VRBP
df = get_df_stockcode('daily_qfq_macd_600000.SH')
df_db = pro.daily_basic(ts_code='600000.SH',fields='trade_date,free_share,float_share,turnover_rate,volume_ratio,pe,pb')
df['MA10'] = cal_ma_ta(df,10)
df['VRBP'] = (df['MA10']/df['MA10'].shift(-1))-1
C_AVG_0 = df['close'][0:10].mean()
C_AVG_1 = df['close'][1:11].mean()
VRBP = (C_AVG_0-C_AVG_1)/C_AVG_1
print (VRBP)
#SPPI
AMOUNT_1 = df['amount'][1]*1000
VOL_1 = df['vol'][1]*100
C_AVG_1 = df['MA10'][1]
FREE_SHARE = df_db['free_share'][1]*10000
FLOAT_SHARE = df_db['float_share'][1]*10000
SPPI = (AMOUNT_1-VOL_1*C_AVG_1)/(FLOAT_SHARE*C_AVG_1)
df['SPPI'] = (df['amount']*1000-df['vol']*100*df['MA10'])/ (df_db['float_share']*10000*df['MA10'])
print (AMOUNT_1,VOL_1,C_AVG_1,FREE_SHARE,FLOAT_SHARE,SPPI,df['SPPI'][1])


import matplotlib.pyplot as plt

# 先看一下散点图
def scatter_fig():
    x = df['VRBP'][0:200]
    y = df['SPPI'][1:201]
    plt.scatter(x, y)
    #plt.xticks(rotation=45)
    plt.show()
scatter_fig()

# 做线性回归: y = β₁x + β₂;  ols('因变量1 ~ 自变量2',data = '变量数据来源').fit(); fit()表示拟合
#from statsmodels.formula.api import ols
#linear_regression = ols(' VRBP ~ SPPI ',data=df).fit()
#print(linear_regression.summary())

X = df['SPPI'][1:201].values
y = df['VRBP'][0:200].values
X2 = sm.add_constant(X)
est = sm.OLS(y, X2)
est2 = est.fit()
print(est2.summary())
print(est2.params[1])
'''
def SPPI():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()   
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}  
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=200):
            continue
        else:
            df_db = pro.daily_basic(ts_code=stockcode,fields='trade_date,free_share,float_share,turnover_rate,volume_ratio,pe,pb')
            df_qfq['MA10'] = cal_ma_ta(df_qfq,10)
            df_qfq['VRBP'] = (df_qfq['MA10']/df_qfq['MA10'].shift(-1))-1
            df_qfq['SPPI'] = (df_qfq['amount']*1000-df_qfq['vol']*100*df_qfq['MA10'])/ (df_db['float_share']*10000*df_qfq['MA10'])    
            X = df_qfq['SPPI'][1:201].values
            y = df_qfq['VRBP'][0:200].values
            X2 = sm.add_constant(X)
            est = sm.OLS(y, X2)
            est2 = est.fit()
            alpha = est2.params[1]
            result_dict['ts_code'] = stockcode
            result_dict['alpha'] = alpha
            result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df           

DF = SPPI()