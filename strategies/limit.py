# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
个股涨停策略模块
@author: 李博
"""
import pandas as pd
import datetime
import time
from collections import Counter

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

def get_limit_last_top(param,n):
    #获取历史交易日
    lasttradeday = get_lasttradedate(0)
    #获取最近交易日涨停股票，并指定字段输出
    df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    if (df_limit_lastday.empty):
        lasttradeday = get_lasttradedate(1)
        df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    df_limit_lastday = df_limit_lastday.sort_values(by=param,ascending=False).head(n)
    return df_limit_lastday

def get_limit_last_param(param,paramvalue):
    #获取历史交易日
    lasttradeday = get_lasttradedate(0)
    #获取最近交易日涨停股票，并指定字段输出
    df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    if (df_limit_lastday.empty):
        lasttradeday = get_lasttradedate(1)
        df_limit_lastday = pro.limit_list(trade_date=lasttradeday, limit_type='U')
    df_limit_lastday = df_limit_lastday(df_limit_lastday[param]==paramvalue)
    return df_limit_lastday

def get_limit_last_strth_top10(df):
    df_limit_lastday =  get_limit_last_top('strth',10)
    result = pd.merge(df,df_limit_lastday , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='strth',ascending=False)
    return result

def get_limit_last_fdamount_top10(df):
    df_limit_lastday =  get_limit_last_top('fd_amount',10)
    result = pd.merge(df,df_limit_lastday , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='fd_amount',ascending=False)
    return result

def get_limit_last_fcratio_top10(df):
    df_limit_lastday =  get_limit_last_top('fc_ratio',10)
    result = pd.merge(df,df_limit_lastday , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='fc_ratio',ascending=False)
    return result

def get_limit_last_flratio_top10(df):
    df_limit_lastday =  get_limit_last_top('fl_ratio',10)
    result = pd.merge(df,df_limit_lastday , how='right', on=['ts_code'])
    #result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='fl_ratio',ascending=False)
    return result

def get_limit_lb1(df):   
    ttlist = get_lasttradedatelist(0,60)
    ttlist.reverse()
     #获取30交易日内涨停信息
    df_limit = pro.limit_list(start_date=ttlist[30], end_date=ttlist[1], limit_type='U')
    df_limit_lastday = pro.limit_list(trade_date=ttlist[0], limit_type='U')
    df_limit_set = set(df_limit['ts_code'].tolist())
    df_limit_lastday_set = set(df_limit_lastday['ts_code'].tolist())
    #判断最近交易日股票代码元组中与30个交易日股票交易代码不同的元素
    df_set = df_limit_lastday_set.difference(df_limit_set) 
    #返回最近交易日代码元组中 仅仅包含不同股票代码的行
    df_different = df_limit_lastday[df_limit_lastday.ts_code.isin(df_set)]
    result = pd.merge(df, df_different, how='right', on=['ts_code'])
    return result
    
def get_limit_lb2(df):   
    ttlist = get_lasttradedatelist(0,60)
    ttlist.reverse()
     #获取30交易日内涨停信息
    df_limit = pro.limit_list(start_date=ttlist[30], end_date=ttlist[2], limit_type='U')
    df_limit_2 = pro.limit_list(start_date=ttlist[1], end_date=ttlist[0], limit_type='U')
    df_limit_2_list = df_limit_2['ts_code'].tolist()
    #统计重复出现次数为2的统计字典 key为股票代码 value为出现次数
    counter_dict = Counter(df_limit_2_list)
    #初始化 重复次数为2次 的统计元组，元素为股票代码
    df_limit_2_set = set()
    for key,value in counter_dict:
        if (value==2):
            df_limit_2_set.add(key)
    df_limit_set = set(df_limit['ts_code'].tolist())
    #判断重复次数为2次 的统计元组中与30个交易日涨停股票代码不同的元素
    df_set = df_limit_2_set.difference(df_limit_set) 
    #返回重复次数为2次 的统计元组 仅仅包含不同股票代码的行
    df_different = df_limit_2[df_limit_2.ts_code.isin(df_set)]
    result = pd.merge(df, df_different, how='right', on=['ts_code'])
    return result

def get_limit_lb3(df):   
    ttlist = get_lasttradedatelist(0,60)
    ttlist.reverse()
     #获取30交易日内涨停信息
    df_limit = pro.limit_list(start_date=ttlist[30], end_date=ttlist[3], limit_type='U')
    df_limit_3 = pro.limit_list(start_date=ttlist[2], end_date=ttlist[0], limit_type='U')
    df_limit_3_list = df_limit_3['ts_code'].tolist()
    #统计重复出现次数为3的统计字典 key为股票代码 value为出现次数
    counter_dict = Counter(df_limit_3_list)
    #初始化 重复次数为3次 的统计元组，元素为股票代码
    df_limit_3_set = set()
    for key,value in counter_dict:
        if (value==3):
            df_limit_3_set.add(key)
    df_limit_set = set(df_limit['ts_code'].tolist())
    #判断重复次数为2次 的统计元组中与30个交易日涨停股票代码不同的元素
    df_set = df_limit_3_set.difference(df_limit_set) 
    #返回重复次数为2次 的统计元组 仅仅包含不同股票代码的行
    df_different = df_limit_3[df_limit_3.ts_code.isin(df_set)]
    result = pd.merge(df, df_different, how='right', on=['ts_code'])
    return result

def get_limit_lb4(df):   
    ttlist = get_lasttradedatelist(0,60)
    ttlist.reverse()
     #获取30交易日内涨停信息
    df_limit = pro.limit_list(start_date=ttlist[30], end_date=ttlist[4], limit_type='U')
    df_limit_4 = pro.limit_list(start_date=ttlist[3], end_date=ttlist[0], limit_type='U')
    df_limit_4_list = df_limit_4['ts_code'].tolist()
    #统计重复出现次数为3的统计字典 key为股票代码 value为出现次数
    counter_dict = Counter(df_limit_4_list)
    #初始化 重复次数为3次 的统计元组，元素为股票代码
    df_limit_4_set = set()
    for key,value in counter_dict:
        if (value==4):
            df_limit_4_set.add(key)
    df_limit_set = set(df_limit['ts_code'].tolist())
    #判断重复次数为2次 的统计元组中与30个交易日涨停股票代码不同的元素
    df_set = df_limit_4_set.difference(df_limit_set) 
    #返回重复次数为2次 的统计元组 仅仅包含不同股票代码的行
    df_different = df_limit_4[df_limit_4.ts_code.isin(df_set)]
    result = pd.merge(df, df_different, how='right', on=['ts_code'])
    return result

def get_limit_lb(df,n):   
    ttlist = get_lasttradedatelist(0,60)
    ttlist.reverse()
     #获取30交易日内涨停信息
    df_limit = pro.limit_list(start_date=ttlist[30], end_date=ttlist[n], limit_type='U')
    df_limit_n = pro.limit_list(start_date=ttlist[n-1], end_date=ttlist[0], limit_type='U')
    df_limit_n_list = df_limit_n['ts_code'].tolist()
    #统计重复出现次数为3的统计字典 key为股票代码 value为出现次数
    counter_dict = Counter(df_limit_n_list)
    #初始化 重复次数为3次 的统计元组，元素为股票代码
    df_limit_n_set = set()
    for key,value in counter_dict.items():
        if (value==n):
            df_limit_n_set.add(key)
    df_limit_set = set(df_limit['ts_code'].tolist())
    #判断重复次数为2次 的统计元组中与30个交易日涨停股票代码不同的元素
    df_set = df_limit_n_set.difference(df_limit_set) 
    #返回重复次数为2次 的统计元组 仅仅包含不同股票代码的行
    df_different = df_limit_n[df_limit_n.ts_code.isin(df_set)]
    result = pd.merge(df, df_different, how='right', on=['ts_code'])
    return result