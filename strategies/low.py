# -*- coding: utf-8 -*-
"""
Spyder Editor
低位运行筛选策略
This is a temporary script file.
"""
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math as math
# MONGODB CONNECT
from pymongo import MongoClient
import json
client = MongoClient('mongodb://127.0.0.1:27017')
#client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["stocks_daily"]
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()

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

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list


#GET ALL DAILY
def get_daily_all():
    mycollection=mydb["stocks_daily_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode

#GET ALL DAILY
def get_daily_n(n):
    mycollection=mydb["stocks_daily_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)[n:]
    return df_stockcode

#GET DAILYBASIC
def get_dailybasic():
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    return df_dailybasic

#GET ALL WEEKLY
def get_weekly_all():
    mycollection=mydb["stocks_weekly_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode

#GET ALL WEEKLY
def get_weekly_n(n):
    mycollection=mydb["stocks_weekly_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    
    return df_stockcode

#低位模型1：小于最大值算法,返回True/False
def cal_low_lessmax(df,param1,n,step):
    lowflag = False
    n_max = max(df[param1][0:n])
    n_1 = df.at[n+step,param1]
    if (n_max < n_1):
        lowflag = True   
    return lowflag
                
#低位模型2：小于最大值且向N日均值靠近算法,返回True/False
def cal_low_lessmax_avg(df,param1,n,step):
    lowflag = False
    w = 0.01
    n_max = max(df[param1][0:n])
    n_n = df.at[n+step,param1]
    n_avg = df[param1][0:n].mean()
    n_0 = df.at[1,param1]
    #n_2 = df.at[2,param1]
    if (n_max < n_n):
        if(abs(n_0-n_avg)/n_avg < w):
            lowflag = True   
    return lowflag

#低位模型3 序列第一个值与最近一个值比例相差50%，返回True/False
def cal_low_lessmax_avg_pct(df,param1,n,step):
    lowflag = False
    w = 0.01
    pct = 0.3
    n_max = max(df[param1][0:n])
    n_n = df.at[n+step,param1]
    n_avg = df[param1][0:n].mean()
    n_0 = df.at[0,param1]
    #n_1 = df.at[1,param1]
    #n_2 = df.at[2,param1]
    if (n_max < n_n):
        if(abs(n_0-n_avg)/n_avg < w):
            if(abs(n_n-n_0)/n_n < pct):
                lowflag = True   
    return lowflag

#低位模型2：小于最大值且向N日均值靠近算法,返回True/False
def cal_low_lessmax_avg_bn(df,param1,bn,n,step):
    lowflag = False
    w = 0.01
    #前n日最大值
    n_max = max(df[param1][bn:bn+n])
    #前n+1日值
    n_n = df.at[bn+n+step,param1]    
    n_avg = df[param1][bn:bn+n].mean()
    n_0 = df.at[bn,param1]
    if (n_max < n_n):
        if(abs(n_0-n_avg)/n_avg < w):
            lowflag = True   
    return lowflag

#低位模型4：小于最大值且向N日均值靠近算法,返回True/False
def cal_weekly_low_lessmax_avg(df,param1,n,w):
    lowflag = False
    #以第0根K线开始计算，获取第1根K线开始到第n根K线最大值，默认为收盘价
    n_max = max(df[param1][1:n])
    #获取第n+1根K线收盘价
    n_n = df.at[n+1,param1]
    #判断从第n根K线开始并为超过第n+1根K线，默认为收盘价
    if (n_max < n_n):
        #以第0根K线开始计算，获取第1根K线开始到第n根K线平均值，默认为收盘价
        n_avg = df[param1][1:n].mean()
        #以第0根K线开始计算，获取第1根K线值
        n_1 = df.at[1,param1]
        #判断获取第1根K线值与n根K线平均的值偏离值比例是否小于阈值比例w
        if(abs(n_1-n_avg)/n_avg < w):
            lowflag = True   
    return lowflag

#低位模型4：小于最大值且向N日均值靠近算法,返回True/False
def cal_weekly_low_lessmax(df,param1,n):
    lowflag = False
    #以第0根K线开始计算，获取第1根K线开始到第n根K线最大值，默认为收盘价
    n_max = max(df[param1][1:n])
    #获取第n+1根K线收盘价
    n_n = df.at[n+1,param1]
    #判断从第n根K线开始并为超过第n+1根K线，默认为收盘价
    if (n_max < n_n):
        lowflag = True   
    return lowflag

#周线N日低位股价策略
def get_weekly_low_n(df):
    n = 20
    result_list = []
    df_dailybasic = get_dailybasic()
    list_dailybasic_stocks = df_dailybasic['ts_code'].values.tolist()
    #获取周线行情
    df_ma = get_weekly_all()
    #df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #if (name in list_dailybasic_stocks and cal_low_lessmax(df_group,'close',10,1)):
            if (name in list_dailybasic_stocks and cal_low_lessmax_avg(df_group,'close',10,1)):
                n_max = max(df_group['close'][0:10])
                n_n = df_group.at[11,'close']
                n_avg = df_group['close'][0:10].mean()
                result_list.append(name)
                print ('OK: ',name,' ',n_max,' ',n_n,' ',n_avg,' ',df_group['trade_date'][0],' ',df_group['close'][0],' ',df_group['close'][10])
                #print ('OK: ',name,' ',df_group['close'],' ',n_max,' ',n_n,' ',n_avg,' ',df_group['trade_date'][0],' ',df_group['close'][0])
    df_result = df_dailybasic[df_dailybasic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    return result

#日线N日低位股价策略
def get_daily_low_n(df):
    n = 20
    result_list = []
    df_dailybasic = get_dailybasic()
    list_dailybasic_stocks = df_dailybasic['ts_code'].values.tolist()
    #获取周线行情
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #if (name in list_dailybasic_stocks and cal_low_lessmax(df_group,'close',10,1)):
            if (name in list_dailybasic_stocks and cal_low_lessmax_avg(df_group,'close',15,1)):
                n_max = max(df_group['close'][0:15])
                n_n = df_group.at[16,'close']
                n_avg = df_group['close'][0:15].mean()
                result_list.append(name)
                print ('OK: ',name,' ',n_max,' ',n_n,' ',n_avg,' ',df_group['trade_date'][0],' ',df_group['close'][0],' ',df_group['close'][10])
                #print ('OK: ',name,' ',df_group['close'],' ',n_max,' ',n_n,' ',n_avg,' ',df_group['trade_date'][0],' ',df_group['close'][0])
    df_result = df_dailybasic[df_dailybasic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    return result

#周线N日低位后长阳线拉升股价策略
def get_strongshort_last_long(df,param1):
    n = param1+2
    pct_chg_low = 0.03
    result_list = []
    df_result = pd.DataFrame()
    df_dailybasic = get_dailybasic()
    #保留原有索引
    df_dailybasic.set_index(['ts_code'],drop=False,inplace=True)
    list_dailybasic_stocks = df_dailybasic['ts_code'].values.tolist()
    #获取周线行情
    df_ma = get_weekly_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #if (name in list_dailybasic_stocks and cal_low_lessmax(df_group,'close',10,1)):
            if (name in list_dailybasic_stocks and cal_low_lessmax_avg(df_group,'close',param1,1)):
                n_max = max(df_group['close'][0:param1])
                n_n = df_group.at[param1+1,'close']
                n_avg = df_group['close'][0:param1].mean()
                daily_close = df_dailybasic.loc[name,'close']
                daily_tradedate = df_dailybasic.loc[name,'trade_date']                
                change_pct = float(format((daily_close-df_group['close'][0])/df_group['close'][0],'.3f'))
                print ('OK: ',name,' ',n_max,' ',n_n,' ',n_avg,' ',df_group['trade_date'][0],' ',df_group['close'][0],' ',change_pct,' ',daily_tradedate)
                if (change_pct > pct_chg_low ):
                    result_list.append(name)
    df_result = df_dailybasic[df_dailybasic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    return result

#周线N日低位后长阳线拉升股价策略
def get_weekly_strongshort_long(df,duringweek):
    n = duringweek+2
    pct_chg_low = 0.05
    result_list = []
    #获取最新收盘行情
    df_dailybasic = get_dailybasic()
    df_result = pd.DataFrame()
    #获取周线行情
    df_ma = get_weekly_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #判断从第n根K线开始并为超过第n+1根K线，默认为收盘价
            if (cal_weekly_low_lessmax(df_group,'close',duringweek)):
                #以第0根K线开始计算，获取第1根K线开始到第n根K线最大值，默认为收盘价
                n_max = max(df_group['close'][1:duringweek])
                #获取第n+1根K线收盘价             
                n_n = df_group.at[duringweek+1,'close']
                #n_avg = df_group['close'][0:duringweek].mean()  
                n_0 = df_group.at[0,'close']
                n_1 = df_group.at[1,'close']
                pct_chg_0 = df_group.at[0,'pct_chg']
                pct_chg_1 = df_group.at[1,'pct_chg']
                change_pct = (n_0-n_1)/n_1
                change_pct_str = format(change_pct,'.3f')
                if (pct_chg_0 > pct_chg_low and pct_chg_1<0):
                    result_list.append(name)
                    print ('RESULT:',name,
                           'MAX:',n_max,
                           'N_N:',n_n,
                           'N_1:',n_1,
                           'N_0:',n_0,
                           'CHG:',change_pct_str)
    df_result = df[df.ts_code.isin(result_list)]
    result = pd.merge(df_dailybasic, df_result, how='right', on=['ts_code'])
    return result
#回测 周线N日低位股价策略
def getback_weekly_low_n(df,bn):
    n = 20
    pct_chg_low = 0.02
    result_list = []
    #获取周线行情
    df_ma = get_weekly_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            if (cal_low_lessmax_avg_bn(df_group,'close',bn,10,1)):
                change_pct = float(format((df_group['close'][bn-1]-df_group['close'][bn])/df_group['close'][bn],'.3f'))
                if (change_pct > pct_chg_low):
                    result_list.append(name)
                    high_max = max(df_group['high'][0:bn-1])
                    profit_pct = float(format((high_max-df_group['close'][bn-1])/df_group['close'][bn-1],'.3f'))
                    print ('OK: ',name,' ',df_group['trade_date'][bn-1],' ',df_group['close'][bn-1],' ',df_group['trade_date'][0],' ',profit_pct)
    df_result = df[df.ts_code.isin(result_list)]
    return df_result

def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#df = get_stockbasket('SSE','主板')
#df_result = get_weekly_strongshort_long(df,10)           
#df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')
