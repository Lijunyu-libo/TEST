# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 14:34:09 2020
#行业分析数据入库
@author: iFunk
"""
import tushare as ts
import pandas as pd
import numpy as np
import talib
from getData import cal_index_basic
import datetime
import time
#获取日线前复权数据模块
#import toMongodb_stocks_daily
#获取最新每日指标数据模块
#import toMongodb_dailybasic_last
#获取个股资金流向
#import toMongodb_moneyflow
#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(30), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday
#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#TRADELIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    lasttradeday_list.reverse()
    return lasttradeday_list

#exchange SSE上交所 SZSE深交所
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#获取单日内行业热点分析数据
def get_daily_analysis_category_industry(tradedate):
    #获取日线数据
    df_daily = pro.daily(trade_date = tradedate)
    #数据处理
    #增加基本信息
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,area,industry')
    #df_dailybasic = pro.daily_basic(ts_code='', trade_date= tradedate)
    df_daily = pd.merge(df_daily,df_stockbasic, how='left', on='ts_code')    

    #分组数据处理    
    df_groupby_industry = df_daily.groupby('industry')
    df_groupby_industry_result = pd.DataFrame()
    index_code = 1
    for name,group in df_groupby_industry:        
        result_dict = {}
        #GET GROUP
        df_group = pd.DataFrame(group)
        #涨跌个股数量统计
        stocks_count = len(df_group['ts_code'])
        stocks_up_count = len(df_group[df_group['pct_chg']>=0])
        stocks_down_count = len(df_group[df_group['pct_chg']<0])        
        #最大涨幅
        stocks_pct_chg_max = round(df_group['pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(df_group['pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(df_group['pct_chg'].mean(),2)
        #涨停个股数量统计
        stocks_limit_count = len(df_group[df_group['pct_chg']>9.8])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)   
        result_dict['index_code'] = str(index_code)
        result_dict['industry_name'] = name
        result_dict['trade_date'] = df_group['trade_date'].head(1).iloc[0]
        result_dict['stockslist_count'] = str(len(df_group))
        result_dict['stockslist'] = df_group['ts_code'].tolist()
        result_dict['stocks_amount_total'] = df_group['amount'].sum()
        result_dict['stocks_vol_total'] = df_group['vol'].sum()
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_down_count'] = stocks_down_count
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg        
        result_dict['stocks_limit_count'] = stocks_limit_count     
        df_groupby_industry_result = df_groupby_industry_result.append(result_dict,ignore_index=True)
        index_code += 1
    return df_groupby_industry_result

#保存行业分析数据入库
def save_daily_analysis_category_industry_tradedatelist(startdate,enddate):
    df_tradedatelist = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=startdate, end_date=enddate)
    tradedatelist = df_tradedatelist['cal_date'].tolist()
    for i in tradedatelist:    
        df =   get_daily_analysis_category_industry(i)
        #定义文档名称
        mycol = mydb['daily_analysis_category_industry']
        #mycol.remove()
        mycol.insert_many(df.to_dict('records'))
        print (i,df['trade_date'][0],'daily_analysis_category_industry:'+str(len(df)))

#save_daily_analysis_category_industry_tradedatelist(20200101,20201228)
        