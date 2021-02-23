# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
个股每日交易基本信息策略模块
@author: 李博
"""
import pandas as pd
import datetime
import time
from collections import Counter
# MONGODB CONNECT
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
TODAY = time.strftime('%Y%m%d',)

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

def get_dailybasic_last_topparam(param,n):
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    if (df_dailybasic.empty):
        print ('DATA EMPTY')
    else:
        df_dailybasic = df_dailybasic.sort_values(by=param,ascending=False).head(n)
    return df_dailybasic

def get_dailybasic_last_betweenparam(param,paramvaluegte,paramvaluelte):
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    if (df_dailybasic.empty):
        print ('DATA EMPTY')
    else:
        df_result = df_dailybasic[ (df_dailybasic[param] >= paramvaluegte) & (df_dailybasic[param] <= paramvaluelte) ]
    return df_result

#换手率TOP10
def get_dailybasic_last_turnover_top100(df):
    df_dailybasic_last_turnover =  get_dailybasic_last_topparam('turnover_rate',100) # 流通股换手率turnover_rate_f
    result = pd.merge(df,df_dailybasic_last_turnover , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='turnover_rate',ascending=False)
    return result
#量比TOP10
def get_dailybasic_last_volumeratio_top100(df):
    df_dailybasic_last_volumeratio =  get_dailybasic_last_topparam('volume_ratio',100) # 流通股换手率turnover_rate_f
    result = pd.merge(df,df_dailybasic_last_volumeratio , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='volume_ratio',ascending=False)
    return result

#换手率区间
def get_dailybasic_last_turnover_between(df):
    df_dailybasic_last_turnover =  get_dailybasic_last_betweenparam('turnover_rate',5,7) # 流通股换手率turnover_rate_f
    result = pd.merge(df,df_dailybasic_last_turnover , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='turnover_rate',ascending=False)
    return result
#量比TOP10
def get_dailybasic_last_volumeratio_between(df):
    df_dailybasic_last_volumeratio =  get_dailybasic_last_betweenparam('volume_ratio',3,10) # 流通股换手率turnover_rate_f
    result = pd.merge(df,df_dailybasic_last_volumeratio , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='volume_ratio',ascending=False)
    return result