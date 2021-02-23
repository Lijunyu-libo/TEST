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
from strategies import caltools
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

#GET DAILYBASIC
def get_dailybasic():
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    return df_dailybasic

#GET DAILYBASIC
def merg_dailybasic(df):
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    df_result = pd.merge(df_dailybasic, df, how='right', on=['ts_code'])
    return df_result

#GET DAILYBASIC
def get_ts_dailybasic():
    df_result = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    return df_result

#GET ALL WEEKLY
def get_weekly_all():
    mycollection=mydb["stocks_weekly_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode


#收窄模型1：N收窄比值序列的最大值 小于固定收窄比值算法,返回True/False
def cal_weekly_boll_narrow_lessmax(df,n,narrowratio):
    resultflag = False
    narrowratio = narrowratio #1.05
    #收窄比例计算公式为 上轨/下轨
    df['narrowratio'] = df['BOLL_UPPER']/df['BOLL_LOWER']
     #以第0根BOLL线开始计算，获取第0根BOLL线开始到第n根BOLL线收窄比例最大值
    n_max = max(df['narrowratio'][0:n])
    if (n_max < narrowratio):
        resultflag = True
    #返回多个结果
    return resultflag,n_max

#收窄模型2：N收窄比值序列的平均值 小于N+N收窄比值序列的平均值算法,返回True/False
def cal_weekly_boll_narrow_pct(df,n,m):
    resultflag = False
    #收窄比例计算公式为 上轨/下轨
    df['narrowratio'] = df['BOLL_UPPER']/df['BOLL_LOWER']
    #以第0根BOLL线开始计算，获取第0根BOLL线开始到第n根BOLL线收窄比例平均值
    n_max = max(df['narrowratio'][0:n])
    #n_avg = df['narrowratio'][0:n].mean()
    m_avg = df['narrowratio'][0:m].mean()
    if (n_max < m_avg):
        resultflag = True
    #返回多个结果
    return resultflag,n_max

#上扬模型1：日boll_middle_0大于boll_middle_1算法,返回True/False
def cal_weekly_boll_up(df):
    upflag = False
    boll_middle_0 = df.at[0,'BOLL_MIDDLE']
    boll_middle_1 = df.at[1,'BOLL_MIDDLE']
    if (boll_middle_0 > boll_middle_1):
        upflag = True
    #返回多个结果
    return upflag,boll_middle_0,boll_middle_1
#上扬模型1：日boll_middle_0大于boll_middle_1的r倍算法,返回True/False
def cal_weekly_boll_up_pct(df,r):
    upflag = False
    boll_middle_0 = df.at[0,'BOLL_MIDDLE']
    boll_middle_1 = df.at[1,'BOLL_MIDDLE']
    if (boll_middle_0 > boll_middle_1*r):
        upflag = True
    #返回多个结果
    return upflag,boll_middle_0,boll_middle_1


#日线N日BOLL收窄策略
def get_weekly_boll_narrow_n(df,n):
    n = n+1
    m = n+5
    df_dailybasic_last = get_ts_dailybasic()
    df_dailybasic_last= df_dailybasic_last.set_index(['ts_code'])
    df_result = pd.DataFrame()
    #获取周线行情
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<m+1):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #通过元组接收多个结果
            df_boll = caltools.get_boll(df_group)
            df_group['BOLL_UPPER'] = df_boll['upper']
            df_group['BOLL_MIDDLE'] = df_boll['middle']
            df_group['BOLL_LOWER'] = df_boll['lower']
            #resultflag,n_max = cal_weekly_boll_narrow_lessmax(df_group,n,1.05)
            resultflag,n_max = cal_weekly_boll_narrow_pct(df_group,n,m)
            close_0 = df_group['close'][0]
            trade_date_0 = df_group['trade_date'][0]
            if (resultflag):
                #close_last = df_dailybasic_last.at[name,'close']
                #profit_rat = format((close_last-df_group['close'][0])/df_group['close'][0],'.3f')
                #必须插入元组或列表
                new=pd.DataFrame({'ts_code':[name],'close_0':[close_0],'trade_date':[trade_date_0]})
                #print ('OK: ',name,' ',n_max)
                #print ('OK: ',name,' ',n_max,' ',df_group['trade_date'][0],' ',df_group['close'][0])
                df_result = df_result.append(new,ignore_index=True)
    df_result_dailybasic = merg_dailybasic(df_result)
    profit_rat = ((df_result_dailybasic['close']-df_result_dailybasic['close_0'])/df_result_dailybasic['close_0']).mean()
    print (len(df_result),profit_rat)
    return df_result_dailybasic

#最近N交易日个股日BOLL线收窄上扬策略
def get_weekly_boll_narrow_up_n(df,n):
    n = n+1
    m = n+5
    df_dailybasic_last = get_ts_dailybasic()
    df_dailybasic_last= df_dailybasic_last.set_index(['ts_code'])
    df_result = pd.DataFrame()
    #获取周线行情
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<m+1):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #通过元组接收多个结果
            df_boll = caltools.get_boll(df_group)
            df_group['BOLL_UPPER'] = df_boll['upper']
            df_group['BOLL_MIDDLE'] = df_boll['middle']
            df_group['BOLL_LOWER'] = df_boll['lower']
            #resultflag,n_max = cal_weekly_boll_narrow_lessmax(df_group,n,1.05)
            resultflag,n_max = cal_weekly_boll_narrow_pct(df_group,n,m)
            close_0 = df_group['close'][0]
            trade_date_0 = df_group['trade_date'][0]
            if (resultflag):
                upflag,boll_middle_0,boll_middle_1 = cal_weekly_boll_up_pct(df_group,1.005)
                if (upflag):
                    #必须插入元组或列表
                    new=pd.DataFrame({'ts_code':[name],'close_0':[close_0],'trade_date':[trade_date_0]})
                    #print ('OK: ',name,' ',n_max)
                    #print ('OK: ',name,' ',n_max,' ',df_group['trade_date'][0],' ',df_group['close'][0])
                    df_result = df_result.append(new,ignore_index=True)
    if (df_result is None):
        print('EMPTY')
    else:
        df_result_dailybasic = merg_dailybasic(df_result)
        profit_rat = ((df_result_dailybasic['close']-df_result_dailybasic['close_0'])/df_result_dailybasic['close_0']).mean()
        print (len(df_result),profit_rat)
    return df_result_dailybasic


#日线N日BOLL收窄策略
def get_daily_boll_narrow_n(df,startdate,enddate):
    m = 20
    delta_5 = datetime.timedelta(days=5)
    start_date = datetime.datetime.strptime(startdate,'%Y%m%d')
    end_date = datetime.datetime.strptime(enddate,'%Y%m%d')
    df_dailybasic_last = get_ts_dailybasic()
    df_dailybasic_last= df_dailybasic_last.set_index(['ts_code'])
    df_result = pd.DataFrame()
    #获取周线行情
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<m):            
            continue
        else:
            #通过元组接收多个结果
            df_boll = caltools.get_boll(df_group)
            df_group['BOLL_UPPER'] = df_boll['upper']
            df_group['BOLL_MIDDLE'] = df_boll['middle']
            df_group['BOLL_LOWER'] = df_boll['lower']
            resultflag = False
            #收窄比例计算公式为 上轨/下轨
            df_group['narrowratio'] = df_group['BOLL_UPPER']/df_group['BOLL_LOWER']
            df_group['index_date'] = pd.DataFrame(df_group['trade_date'], dtype=np.datetime64)
            df_group = df_group.set_index(['index_date'], drop=False, append=False, inplace=False, verify_integrity=False)
            indexlist = df_group['trade_date'].tolist()
            #print (name,indexlist)
            if (startdate in indexlist and enddate in indexlist):
                #print (name,'in list')
                #if (True):
            #以第0根BOLL线开始计算，获取第0根BOLL线开始到第n根BOLL线收窄比例平均值
                n_max = max(df_group['narrowratio'][end_date:start_date])
                m_avg = df_group['narrowratio'][end_date:start_date-delta_5].mean()
                if (n_max < m_avg):
                    resultflag = True
                close_0 = df_group['close'][end_date]
                trade_date_0 = df_group['trade_date'][end_date]
                if (resultflag):
                    #close_last = df_dailybasic_last.at[name,'close']
                    #profit_rat = format((close_last-df_group['close'][0])/df_group['close'][0],'.3f')
                    #必须插入元组或列表
                    new=pd.DataFrame({'ts_code':[name],'close_0':[close_0],'trade_date':[trade_date_0]})
                    #print ('OK: ',name,' ',n_max)
                    print ('OK: ',name,' ',n_max,' ',trade_date_0,' ',close_0)
                    df_result = df_result.append(new,ignore_index=True)
    df_result_dailybasic = merg_dailybasic(df_result)
    profit_rat = ((df_result_dailybasic['close']-df_result_dailybasic['close_0'])/df_result_dailybasic['close_0']).mean()
    print (len(df_result),profit_rat)
    return df_result_dailybasic




def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#df = get_stockbasket('SSE','主板')
#get_daily_boll_narrow_n(df,'20200701','20200804')
'''
print ('weekly_boll_narrow_5')
df_narrow = get_weekly_boll_narrow_n(df,5)
print ('weekly_boll_narrow_10')
df_narrow = get_weekly_boll_narrow_n(df,10)
print ('weekly_boll_narrow_15')
df_narrow = get_weekly_boll_narrow_n(df,15)
print ('weekly_boll_narrow_20')
df_narrow = get_weekly_boll_narrow_n(df,20)

print ('weekly_boll_narrow_up_5')
df_narrow = get_weekly_boll_narrow_up_n(df,5)
print ('weekly_boll_narrow_up_10')
df_narrow = get_weekly_boll_narrow_up_n(df,10)
print ('weekly_boll_narrow_up_15')
df_narrow = get_weekly_boll_narrow_up_n(df,15)
print ('weekly_boll_narrow_up_20')
df_narrow = get_weekly_boll_narrow_up_n(df,20)
#df_narrow_up = get_weekly_boll_narrow_up_n(df,10)
'''