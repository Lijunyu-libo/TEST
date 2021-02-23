# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: iFunk
"""
from bokeh.plotting import figure,show
from bokeh.embed import components
from bokeh.resources import INLINE
from bokeh.models.sources import ColumnDataSource
from bokeh.models import BoxAnnotation, ColorBar, LabelSet
from dateutil.parser import parse
import json
from io import StringIO
import demjson
from typing import Optional
import pandas as pd
import numpy as np
import datetime
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates

#from api import caltools
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')

#查询库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
indexnamedict = {'000001.SH':'上证综指','399001.SZ':'深圳成指','399005.SZ':'深中小板指','399006.SZ':'深创业板指'}

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
#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#判断连续3日参数涨跌
def get_3days_equalzero(n1,n2,n3):
    up_data = 0
    if(n1>0):
        up_data= 1
        if(n1>0 and n2>0 and n3>0):
            up_data = 3
        if(n1>0 and n2>0 and n3<0):
            up_data = 2
    else:
        up_data= -1
        if(n1<0 and n2<0 and n3<0):
            up_data = -3
        if(n1<0 and n2<0 and n3>0):
            up_data = -2
    return up_data
#N日param均值增减幅度计算函数
def get_col_param_chg_pct(col,param,n,i):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    param_avg = df_col[param][0:n].mean()
    param_i = df_col[param][i]
    chg_pct = round((param_i-param_avg)/param_avg,3)
    return chg_pct
    

def get_data_indexs():
    indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
    indexs_data_list = []
    #获取当日指数涨跌
    for i in indexlist:
        index_dict = {}
        df_index = get_col_df(i)
        index_name = i
        #指数分析
        index_tradedate_0 = df_index['trade_date'][0]
        index_0 = df_index['close'][0]
        index_pct_chg_0 = df_index['pct_chg'][0]
        #print (index_name,index_tradedate_0,index_0,index_pct_chg_0)
        #获取昨日指数涨跌
        index_tradedate_1 = df_index['trade_date'][1]
        index_1 = df_index['close'][1]
        index_pct_chg_1 = df_index['pct_chg'][1]
        index_pct_chg_2 = df_index['pct_chg'][2]
        #print (index_name,index_tradedate_1,index_1,index_pct_chg_1)
        #获取指数连续涨跌天数
        index_updown = get_3days_equalzero(index_pct_chg_0,index_pct_chg_1,index_pct_chg_2)
        #print (index_updown)
        #获取N日0\1\2\3\4涨跌幅
        index_daily_chg_pct_0 = get_col_param_chg_pct(i,'close',5,0)
        index_daily_chg_pct_1 = get_col_param_chg_pct(i,'close',5,1)
        index_daily_chg_pct_2 = get_col_param_chg_pct(i,'close',5,2)
        index_daily_chg_pct_3 = get_col_param_chg_pct(i,'close',5,3)
        index_daily_chg_pct_4 = get_col_param_chg_pct(i,'close',5,4)
        index_daily_chg_pct_list = [index_daily_chg_pct_0,index_daily_chg_pct_1,index_daily_chg_pct_2,index_daily_chg_pct_3,index_daily_chg_pct_4]
        #获取N周0\1\2\3涨跌幅
        index_weekly_chg_pct_0 = get_col_param_chg_pct(i+'_weekly','close',4,0)        
        index_weekly_chg_pct_1 = get_col_param_chg_pct(i+'_weekly','close',4,1)
        index_weekly_chg_pct_2 = get_col_param_chg_pct(i+'_weekly','close',4,2)
        index_weekly_chg_pct_3 = get_col_param_chg_pct(i+'_weekly','close',4,3)
        index_weekly_chg_pct_list = [index_weekly_chg_pct_0,index_weekly_chg_pct_1,index_weekly_chg_pct_2,index_weekly_chg_pct_3]
        #获取N月0\1\2\3涨跌幅
        index_monthly_chg_pct_0 = get_col_param_chg_pct(i+'_monthly','close',3,0)
        index_monthly_chg_pct_1 = get_col_param_chg_pct(i+'_monthly','close',3,1)
        index_monthly_chg_pct_2 = get_col_param_chg_pct(i+'_monthly','close',3,2)
        index_monthly_chg_pct_list = [index_monthly_chg_pct_0,index_monthly_chg_pct_1,index_monthly_chg_pct_2]       
        #成交量分析 market/amount
        #今昨日成交金额差额差额比
        #index_amount_0 = df_index['amount'][0]
        index_amount_chg_0 = df_index['amount'][0]-df_index['amount'][1]
        index_amount_chg_1 = df_index['amount'][1]-df_index['amount'][2]
        index_amount_chg_2 = df_index['amount'][2]-df_index['amount'][3]
        index_amount_chg_pct_0 = round(index_amount_chg_0/df_index['amount'][1],3)
        index_amount_chg_pct_1 = round(index_amount_chg_1/df_index['amount'][2],3)
        index_amount_chg_pct_2 = round(index_amount_chg_2/df_index['amount'][3],3)
        #print (index_name,index_tradedate_0,index_amount_0,index_amount_chg_0,index_amount_chg_pct_0)
        #3日内成交金额连续放量缩量天数
        index_amount_chg_1 = df_index['amount'][1]-df_index['amount'][2]
        index_amount_chg_2 = df_index['amount'][2]-df_index['amount'][3]
        index_amount_updown = get_3days_equalzero(index_amount_chg_0,index_amount_chg_1,index_amount_chg_2)
        #print (index_amount_updown)
        #N日0\1\2\3\4成交金额平均差额比
        index_daily_amount_chg_pct_0 = get_col_param_chg_pct(i,'amount',5,0)
        index_daily_amount_chg_pct_1 = get_col_param_chg_pct(i,'amount',5,1)
        index_daily_amount_chg_pct_2 = get_col_param_chg_pct(i,'amount',5,2)
        index_daily_amount_chg_pct_3 = get_col_param_chg_pct(i,'amount',5,3)
        index_daily_amount_chg_pct_4 = get_col_param_chg_pct(i,'amount',5,4)
        index_daily_amount_chg_pct_list = [index_daily_amount_chg_pct_0,index_daily_amount_chg_pct_1,index_daily_amount_chg_pct_2,index_daily_amount_chg_pct_3,index_daily_amount_chg_pct_4]
        #获取N周0\1\2\3涨跌幅
        index_weekly_amount_chg_pct_0 = get_col_param_chg_pct(i+'_weekly','amount',4,0)        
        index_weekly_amount_chg_pct_1 = get_col_param_chg_pct(i+'_weekly','amount',4,1)
        index_weekly_amount_chg_pct_2 = get_col_param_chg_pct(i+'_weekly','amount',4,2)
        index_weekly_amount_chg_pct_3 = get_col_param_chg_pct(i+'_weekly','amount',4,3)
        index_weekly_amount_chg_pct_list = [index_weekly_amount_chg_pct_0,index_weekly_amount_chg_pct_1,index_weekly_amount_chg_pct_2,index_weekly_amount_chg_pct_3]
        #获取N月0\1\2涨跌幅
        index_monthly_amount_chg_pct_0 = get_col_param_chg_pct(i+'_monthly','amount',3,0)
        index_monthly_amount_chg_pct_1 = get_col_param_chg_pct(i+'_monthly','amount',3,1)
        index_monthly_amount_chg_pct_2 = get_col_param_chg_pct(i+'_monthly','amount',3,2)
        index_monthly_amount_chg_pct_list = [index_monthly_amount_chg_pct_0,index_monthly_amount_chg_pct_1,index_monthly_amount_chg_pct_2]
        #数据封装
        #指数名称
        index_dict['index_name'] = index_name
        #当前交易日
        index_dict['index_tradedate_0'] = index_tradedate_0
        #当日指数
        index_dict['index_0'] = index_0
        #当日涨跌
        index_dict['index_pct_chg_0'] = index_pct_chg_0

        #前一交易日
        index_dict['index_tradedate_1'] = index_tradedate_1
        #前日指数
        index_dict['index_1'] = index_1
        #前日涨跌
        index_dict['index_pct_chg_1'] = index_pct_chg_1
        #连续涨跌天数
        index_dict['index_updown'] = index_updown
        #N日平均涨跌幅
        index_dict['index_daily_chg_pct_0'] = index_daily_chg_pct_0
        index_dict['index_daily_chg_pct_1'] = index_daily_chg_pct_1
        index_dict['index_daily_chg_pct_2'] = index_daily_chg_pct_2
        index_dict['index_daily_chg_pct_3'] = index_daily_chg_pct_3 
        index_dict['index_daily_chg_pct_4'] = index_daily_chg_pct_4
        index_dict['index_daily_chg_pct_list'] = index_daily_chg_pct_list
        #N周平均涨跌幅
        index_dict['index_weekly_chg_pct_0'] = index_weekly_chg_pct_0
        index_dict['index_weekly_chg_pct_1'] = index_weekly_chg_pct_1
        index_dict['index_weekly_chg_pct_2'] = index_weekly_chg_pct_2
        index_dict['index_weekly_chg_pct_3'] = index_weekly_chg_pct_3
        index_dict['index_weekly_chg_pct_list'] = index_weekly_chg_pct_list
        #N月平均涨跌幅        
        index_dict['index_monthly_chg_pct_0'] = index_monthly_chg_pct_0
        index_dict['index_monthly_chg_pct_1'] = index_monthly_chg_pct_1
        index_dict['index_monthly_chg_pct_2'] = index_monthly_chg_pct_2
        index_dict['index_monthly_chg_pct_list'] = index_monthly_chg_pct_list
        #今昨日成交金额差额差额比
        index_dict['index_amount_chg_0'] = round(index_amount_chg_0/100000,1)
        index_dict['index_amount_chg_1'] = round(index_amount_chg_1/100000,1)
        index_dict['index_amount_chg_2'] = round(index_amount_chg_2/100000,1)
        index_dict['index_amount_chg_pct_0'] = round(index_amount_chg_pct_0*100,2)
        index_dict['index_amount_chg_pct_1'] = round(index_amount_chg_pct_1*100,2)
        index_dict['index_amount_chg_pct_2'] = round(index_amount_chg_pct_2*100,2)
        #成交金额连续放量缩量天数
        index_dict['index_amount_updown'] = index_amount_updown
        #N日成交金额平均差额比
        index_dict['index_daily_amount_chg_pct_0'] = index_daily_amount_chg_pct_0
        index_dict['index_daily_amount_chg_pct_1'] = index_daily_amount_chg_pct_1
        index_dict['index_daily_amount_chg_pct_2'] = index_daily_amount_chg_pct_2
        index_dict['index_daily_amount_chg_pct_3'] = index_daily_amount_chg_pct_3
        index_dict['index_daily_amount_chg_pct_4'] = index_daily_amount_chg_pct_4
        index_dict['index_daily_amount_chg_pct_list'] = index_daily_amount_chg_pct_list
        #N周成交金额平均差额比
        index_dict['index_weekly_amount_chg_pct_0'] = index_weekly_amount_chg_pct_0
        index_dict['index_weekly_amount_chg_pct_1'] = index_weekly_amount_chg_pct_1
        index_dict['index_weekly_amount_chg_pct_2'] = index_weekly_amount_chg_pct_2
        index_dict['index_weekly_amount_chg_pct_3'] = index_weekly_amount_chg_pct_3
        index_dict['index_weekly_amount_chg_pct_list'] = index_weekly_amount_chg_pct_list
        #N月成交金额平均差额比
        index_dict['index_monthly_amount_chg_pct_0'] = index_monthly_amount_chg_pct_0
        index_dict['index_monthly_amount_chg_pct_1'] = index_monthly_amount_chg_pct_1
        index_dict['index_monthly_amount_chg_pct_2'] = index_monthly_amount_chg_pct_2
        index_dict['index_monthly_amount_chg_pct_list'] = index_monthly_amount_chg_pct_list
        #添加到集合
        indexs_data_list.append(index_dict)
        #print (index_dict)
    #print (len(indexs_data_list))
    return indexs_data_list
#list_temp = get_data_indexs()
        
def get_data_hsgt():
    #tradedatelist = get_lasttradedatelist(0,100)
    #tradedatelist.reverse()   
    #print (tradedatelist)
    #df_hsgt = pro.moneyflow_hsgt(start_date=tradedatelist[30], end_date=tradedatelist[0])
    df_hsgt = get_col_df('hsgt')
    hsgt_dict={}
    hsgt_tradedate_0 = df_hsgt['trade_date'][0]
    hsgt_tradedate_1 = df_hsgt['trade_date'][1]
    hsgt_tradedate_2 = df_hsgt['trade_date'][2]
    hsgt_tradedate_3 = df_hsgt['trade_date'][3]
    hsgt_tradedate_4 = df_hsgt['trade_date'][4]
    hsgt_dict['hsgt_tradedate_0'] = hsgt_tradedate_0
    hsgt_dict['hsgt_tradedate_1'] = hsgt_tradedate_1 
    hsgt_dict['hsgt_tradedate_2'] = hsgt_tradedate_2 
    hsgt_dict['hsgt_tradedate_3'] = hsgt_tradedate_3 
    hsgt_dict['hsgt_tradedate_4'] = hsgt_tradedate_4
    hsgt_tradedate_list_5 = [hsgt_tradedate_0,hsgt_tradedate_1,hsgt_tradedate_2,hsgt_tradedate_3,hsgt_tradedate_4]
    hsgt_tradedate_list_5.reverse()     
    hsgt_dict['hsgt_tradedate_list_5'] = hsgt_tradedate_list_5
    #今昨日南向资金金额差额 比值
    hsgt_south_money_0 = df_hsgt['south_money'][0]*100
    hsgt_south_money_1 = df_hsgt['south_money'][1]*100
    hsgt_south_money_2 = df_hsgt['south_money'][2]*100
    hsgt_south_money_3 = df_hsgt['south_money'][3]*100
    hsgt_south_money_4 = df_hsgt['south_money'][4]*100
    #分析当前南向资金金额增减情况 （万元）
    hsgt_south_money_chg_0 = hsgt_south_money_0 - hsgt_south_money_1
    hsgt_south_money_chg_pct_0 = round(hsgt_south_money_chg_0/hsgt_south_money_1,3)
    hsgt_dict['hsgt_south_money_chg_0'] = hsgt_south_money_chg_0
    hsgt_dict['hsgt_south_money_chg_pct_0'] = hsgt_south_money_chg_pct_0   
    #分析昨日南向资金金额增减情况 （万元）
    hsgt_south_money_chg_1 = hsgt_south_money_1 - hsgt_south_money_2
    hsgt_south_money_chg_pct_1 = round(hsgt_south_money_chg_1/hsgt_south_money_2,3)
    hsgt_dict['hsgt_south_money_chg_1'] = hsgt_south_money_chg_1
    hsgt_dict['hsgt_south_money_chg_pct_1'] = hsgt_south_money_chg_pct_1    
    #分析当前、昨日港股通上海资金金额增减情况 
    hsgt_ggt_ss_money_0 = df_hsgt['ggt_ss'][0]*100
    hsgt_ggt_ss_money_1 = df_hsgt['ggt_ss'][1]*100
    hsgt_ggt_ss_money_2 = df_hsgt['ggt_ss'][2]*100
    hsgt_ggt_ss_money_3 = df_hsgt['ggt_ss'][3]*100
    hsgt_ggt_ss_money_4 = df_hsgt['ggt_ss'][4]*100    
    hsgt_ggt_ss_money_chg_0 = hsgt_ggt_ss_money_0 - hsgt_ggt_ss_money_1
    hsgt_ggt_ss_money_chg_pct_0 = round(hsgt_ggt_ss_money_chg_0/hsgt_ggt_ss_money_1,3)    
    hsgt_dict['hsgt_ggt_ss_money_chg_0'] = hsgt_ggt_ss_money_chg_0
    hsgt_dict['hsgt_ggt_ss_money_chg_pct_0'] = hsgt_ggt_ss_money_chg_pct_0    
    hsgt_ggt_ss_money_chg_1 = hsgt_ggt_ss_money_1 - hsgt_ggt_ss_money_2
    hsgt_ggt_ss_money_chg_pct_1 = round(hsgt_ggt_ss_money_chg_1/hsgt_ggt_ss_money_2,3)    
    hsgt_dict['hsgt_ggt_ss_money_chg_1'] = hsgt_ggt_ss_money_chg_1
    hsgt_dict['hsgt_ggt_ss_money_chg_pct_1'] = hsgt_ggt_ss_money_chg_pct_1
    #分析当前港股通深圳资金金额增减情况
    hsgt_ggt_sz_money_0 = df_hsgt['ggt_sz'][0]*100
    hsgt_ggt_sz_money_1 = df_hsgt['ggt_sz'][1]*100
    hsgt_ggt_sz_money_2 = df_hsgt['ggt_sz'][2]*100
    hsgt_ggt_sz_money_3 = df_hsgt['ggt_sz'][3]*100
    hsgt_ggt_sz_money_4 = df_hsgt['ggt_sz'][4]*100
    hsgt_ggt_sz_money_chg_0 = hsgt_ggt_sz_money_0 - hsgt_ggt_sz_money_1
    hsgt_ggt_sz_money_chg_pct_0 = round(hsgt_ggt_sz_money_chg_0/hsgt_ggt_sz_money_1,3)       
    hsgt_dict['hsgt_ggt_sz_money_chg_0'] = hsgt_ggt_sz_money_chg_0
    hsgt_dict['hsgt_ggt_sz_money_chg_pct_0'] = hsgt_ggt_sz_money_chg_pct_0
    hsgt_ggt_sz_money_chg_1 = hsgt_ggt_sz_money_1 - hsgt_ggt_sz_money_2
    hsgt_ggt_sz_money_chg_pct_1 = round(hsgt_ggt_sz_money_chg_1/hsgt_ggt_sz_money_2,3)       
    hsgt_dict['hsgt_ggt_sz_money_chg_1'] = hsgt_ggt_sz_money_chg_1
    hsgt_dict['hsgt_ggt_sz_money_chg_pct_1'] = hsgt_ggt_sz_money_chg_pct_1
    
    #今昨日北向资金金额差额 比值
    #分析当前北向资金金额增减情况 
    hsgt_north_money_0 = df_hsgt['north_money'][0]*100
    hsgt_north_money_1 = df_hsgt['north_money'][1]*100
    hsgt_north_money_2 = df_hsgt['north_money'][2]*100
    hsgt_north_money_3 = df_hsgt['north_money'][3]*100
    hsgt_north_money_4 = df_hsgt['north_money'][4]*100
    hsgt_north_money_chg_0 = hsgt_north_money_0 - hsgt_north_money_1
    hsgt_north_money_chg_pct_0 = round(hsgt_north_money_chg_0/hsgt_north_money_1,3)
    hsgt_dict['hsgt_north_money_chg_0'] = hsgt_north_money_chg_0
    hsgt_dict['hsgt_north_money_chg_pct_0'] = hsgt_north_money_chg_pct_0
    #分析昨日北向资金金额增减情况 
    hsgt_north_money_chg_1 = hsgt_north_money_1 - hsgt_north_money_2
    hsgt_north_money_chg_pct_1 = round(hsgt_north_money_chg_1/hsgt_north_money_2,3)
    hsgt_dict['hsgt_north_money_chg_1'] = hsgt_north_money_chg_1
    hsgt_dict['hsgt_north_money_chg_pct_1'] = hsgt_north_money_chg_pct_1
    #分析当前昨日沪港通资金金额增减情况
    hsgt_hgt_money_0 = df_hsgt['hgt'][0]*100
    hsgt_hgt_money_1 = df_hsgt['hgt'][1]*100
    hsgt_hgt_money_2 = df_hsgt['hgt'][2]*100
    hsgt_hgt_money_3 = df_hsgt['hgt'][3]*100
    hsgt_hgt_money_4 = df_hsgt['hgt'][4]*100    
    hsgt_hgt_money_chg_0 = hsgt_hgt_money_0 - hsgt_hgt_money_1
    hsgt_hgt_money_chg_pct_0 = round(hsgt_hgt_money_chg_0/hsgt_hgt_money_1,3)    
    hsgt_dict['hsgt_hgt_money_chg_0'] = hsgt_hgt_money_chg_0
    hsgt_dict['hsgt_hgt_money_chg_pct_0'] = hsgt_hgt_money_chg_pct_0
    hsgt_hgt_money_chg_1 = hsgt_hgt_money_1 - hsgt_hgt_money_2
    hsgt_hgt_money_chg_pct_1 = round(hsgt_hgt_money_chg_1/hsgt_hgt_money_2,3)    
    hsgt_dict['hsgt_hgt_money_chg_1'] = hsgt_hgt_money_chg_1
    hsgt_dict['hsgt_hgt_money_chg_pct_1'] = hsgt_hgt_money_chg_pct_1    
    #分析当前昨日深港通资金金额增减情况
    hsgt_sgt_money_0 = df_hsgt['sgt'][0]*100
    #print (hsgt_sgt_money_0)
    hsgt_sgt_money_1 = df_hsgt['sgt'][1]*100
    #print (hsgt_sgt_money_1)
    hsgt_sgt_money_2 = df_hsgt['sgt'][2]*100
    hsgt_sgt_money_3 = df_hsgt['sgt'][3]*100
    hsgt_sgt_money_4 = df_hsgt['sgt'][4]*100    
    hsgt_sgt_money_chg_0 = hsgt_sgt_money_0 - hsgt_sgt_money_1
    #print (hsgt_sgt_money_chg_0)
    hsgt_sgt_money_chg_pct_0 = round(hsgt_sgt_money_chg_0/hsgt_sgt_money_1,3)
    hsgt_dict['hsgt_sgt_money_chg_0'] = hsgt_sgt_money_chg_0
    hsgt_dict['hsgt_sgt_money_chg_pct_0'] = hsgt_sgt_money_chg_pct_0
    hsgt_sgt_money_chg_1 = hsgt_sgt_money_1 - hsgt_sgt_money_2
    hsgt_sgt_money_chg_pct_1 = round(hsgt_sgt_money_chg_1/hsgt_sgt_money_2,3)
    hsgt_dict['hsgt_sgt_money_chg_1'] = hsgt_sgt_money_chg_1
    hsgt_dict['hsgt_sgt_money_chg_pct_1'] = hsgt_sgt_money_chg_pct_1    
    
    #南向资金连续放量缩量天数
    #分析3日内市场南向资金成交金额连续缩放量情况
    hsgt_south_money_chg_1 = hsgt_south_money_1 - hsgt_south_money_2
    hsgt_south_money_chg_2 = hsgt_south_money_2 - hsgt_south_money_3
    hsgt_south_money_updown = get_3days_equalzero(hsgt_south_money_chg_0,hsgt_south_money_chg_1,hsgt_south_money_chg_2)
    hsgt_dict['hsgt_south_money_updown'] = hsgt_south_money_updown
    #分析3日内市场港股通上海资金成交金额连续缩放量情况
    hsgt_ggt_ss_money_chg_1 = hsgt_ggt_ss_money_1 - hsgt_ggt_ss_money_2
    hsgt_ggt_ss_money_chg_2 = hsgt_ggt_ss_money_2 - hsgt_ggt_ss_money_3
    hsgt_ggt_ss_money_updown = get_3days_equalzero(hsgt_ggt_ss_money_chg_0,hsgt_ggt_ss_money_chg_1,hsgt_ggt_ss_money_chg_2)
    hsgt_dict['hsgt_ggt_ss_money_updown'] = hsgt_ggt_ss_money_updown
    #分析3日内市场港股通深圳资金成交金额连续缩放量情况
    hsgt_ggt_sz_money_chg_1 = hsgt_ggt_sz_money_1 - hsgt_ggt_sz_money_2
    hsgt_ggt_sz_money_chg_2 = hsgt_ggt_sz_money_2 - hsgt_ggt_sz_money_3
    hsgt_ggt_sz_money_updown = get_3days_equalzero(hsgt_ggt_sz_money_chg_0,hsgt_ggt_sz_money_chg_1,hsgt_ggt_sz_money_chg_2)
    hsgt_dict['hsgt_ggt_sz_money_updown'] = hsgt_ggt_sz_money_updown
    #北向资金连续放量缩量天数
    #分析3日内市场北向资金成交金额连续缩放量情况
    hsgt_north_money_chg_1 = hsgt_north_money_1 - hsgt_north_money_2
    hsgt_north_money_chg_2 = hsgt_north_money_2 - hsgt_north_money_3
    hsgt_north_money_updown = get_3days_equalzero(hsgt_north_money_chg_0,hsgt_north_money_chg_1,hsgt_north_money_chg_2)
    #print (hsgt_north_money_updown)
    hsgt_dict['hsgt_north_money_updown'] = hsgt_north_money_updown
    #分析3日内市场沪港通资金成交金额连续缩放量情况
    hsgt_hgt_money_chg_1 = hsgt_hgt_money_1 - hsgt_hgt_money_2
    hsgt_hgt_money_chg_2 = hsgt_hgt_money_2 - hsgt_hgt_money_3
    hsgt_hgt_money_updown = get_3days_equalzero(hsgt_hgt_money_chg_0,hsgt_hgt_money_chg_1,hsgt_hgt_money_chg_2)
    #print (hsgt_hgt_money_updown)
    hsgt_dict['hsgt_hgt_money_updown'] = hsgt_hgt_money_updown    
    #分析3日内市场深港通资金成交金额连续缩放量情况
    hsgt_sgt_money_chg_1 = hsgt_sgt_money_1 - hsgt_sgt_money_2
    hsgt_sgt_money_chg_2 = hsgt_sgt_money_2 - hsgt_sgt_money_3
    hsgt_sgt_money_updown = get_3days_equalzero(hsgt_sgt_money_chg_0,hsgt_sgt_money_chg_1,hsgt_sgt_money_chg_2)
    #print (hsgt_sgt_money_updown)
    hsgt_dict['hsgt_sgt_money_updown'] = hsgt_sgt_money_updown
    
    #N日成交金额平均差额比
    #分析1、2、3、4、5日短期市场南向资金成交较平均值波动情况
    hsgt_south_money_5_list=[]
    hsgt_south_money_avg_5 = round(df_hsgt['south_money'][0:5].mean()*100,2)
    hsgt_south_money_0_diff = round(hsgt_south_money_0-hsgt_south_money_avg_5,2)
    hsgt_south_money_1_diff = round(hsgt_south_money_1-hsgt_south_money_avg_5,2)
    hsgt_south_money_2_diff = round(hsgt_south_money_2-hsgt_south_money_avg_5,2)
    hsgt_south_money_3_diff = round(hsgt_south_money_3-hsgt_south_money_avg_5,2)
    hsgt_south_money_4_diff = round(hsgt_south_money_4-hsgt_south_money_avg_5,2)
    hsgt_south_money_5_list = [hsgt_south_money_0_diff,hsgt_south_money_1_diff,hsgt_south_money_2_diff,hsgt_south_money_3_diff,hsgt_south_money_4_diff]
    hsgt_south_money_5_list.reverse()
    hsgt_dict['hsgt_south_money_avg_5'] = hsgt_south_money_avg_5
    hsgt_dict['hsgt_south_money_5_list'] = hsgt_south_money_5_list
    #分析1、2、3、4、5日短期市场港股通上海资金成交较平均值波动情况
    hsgt_ggt_ss_money_5_list=[]
    hsgt_ggt_ss_money_avg_5 = round(df_hsgt['ggt_ss'][0:5].mean()*100,2)
    hsgt_ggt_ss_money_0_diff = round(hsgt_ggt_ss_money_0-hsgt_ggt_ss_money_avg_5,2)
    hsgt_ggt_ss_money_1_diff = round(hsgt_ggt_ss_money_1-hsgt_ggt_ss_money_avg_5,2)
    hsgt_ggt_ss_money_2_diff = round(hsgt_ggt_ss_money_2-hsgt_ggt_ss_money_avg_5,2)
    hsgt_ggt_ss_money_3_diff = round(hsgt_ggt_ss_money_3-hsgt_ggt_ss_money_avg_5,2)
    hsgt_ggt_ss_money_4_diff = round(hsgt_ggt_ss_money_4-hsgt_ggt_ss_money_avg_5,2)
    hsgt_dict['hsgt_ggt_ss_money_avg_5'] = hsgt_ggt_ss_money_avg_5
    hsgt_ggt_ss_money_5_list = [hsgt_ggt_ss_money_0_diff,
                                hsgt_ggt_ss_money_1_diff,
                                hsgt_ggt_ss_money_2_diff,
                                hsgt_ggt_ss_money_3_diff,
                                hsgt_ggt_ss_money_4_diff
                                ]
    hsgt_ggt_ss_money_5_list.reverse()
    hsgt_dict['hsgt_ggt_ss_money_5_list'] = hsgt_ggt_ss_money_5_list
    #分析1、2、3、4、5日短期市场港股通深圳资金成交较平均值波动情况
    hsgt_ggt_sz_money_5_list = []
    hsgt_ggt_sz_money_avg_5 = round(df_hsgt['ggt_sz'][0:5].mean()*100,2)
    hsgt_ggt_sz_money_0_diff = round(hsgt_ggt_sz_money_0-hsgt_ggt_sz_money_avg_5,2)
    hsgt_ggt_sz_money_1_diff = round(hsgt_ggt_sz_money_1-hsgt_ggt_sz_money_avg_5,2)
    hsgt_ggt_sz_money_2_diff = round(hsgt_ggt_sz_money_2-hsgt_ggt_sz_money_avg_5,2)
    hsgt_ggt_sz_money_3_diff = round(hsgt_ggt_sz_money_3-hsgt_ggt_sz_money_avg_5,2)
    hsgt_ggt_sz_money_4_diff = round(hsgt_ggt_sz_money_4-hsgt_ggt_sz_money_avg_5,2)
    hsgt_dict['hsgt_ggt_sz_money_avg_5'] = hsgt_ggt_sz_money_avg_5
    hsgt_ggt_sz_money_5_list = [hsgt_ggt_sz_money_0_diff,
                                hsgt_ggt_sz_money_1_diff,
                                hsgt_ggt_sz_money_2_diff,
                                hsgt_ggt_sz_money_3_diff,
                                hsgt_ggt_sz_money_4_diff
                                ]
    
    hsgt_dict['hsgt_ggt_sz_money_5_list'] = hsgt_ggt_sz_money_5_list
    hsgt_ggt_sz_money_5_list.reverse()
    #N日北向资金成交金额平均差额比
    #分析1、2、3、4、5日短期市场北向资金成交较平均值波动情况
    hsgt_north_money_5_list=[]
    hsgt_north_money_avg_5 = round(df_hsgt['north_money'][0:5].mean()*100,2)
    hsgt_north_money_0_diff = round(hsgt_north_money_0-hsgt_north_money_avg_5,2)
    hsgt_north_money_1_diff = round(hsgt_north_money_1-hsgt_north_money_avg_5,2)
    hsgt_north_money_2_diff = round(hsgt_north_money_2-hsgt_north_money_avg_5,2)
    hsgt_north_money_3_diff = round(hsgt_north_money_3-hsgt_north_money_avg_5,2)
    hsgt_north_money_4_diff = round(hsgt_north_money_4-hsgt_north_money_avg_5,2)
    #print (hsgt_north_money_avg_5,hsgt_north_money_0_diff,hsgt_north_money_1_diff,hsgt_north_money_2_diff,hsgt_north_money_3_diff,hsgt_north_money_4_diff)    
    hsgt_dict['hsgt_north_money_avg_5'] = hsgt_north_money_avg_5
    hsgt_north_money_5_list = [hsgt_north_money_0_diff,
                               hsgt_north_money_1_diff,
                               hsgt_north_money_2_diff,
                               hsgt_north_money_3_diff,
                               hsgt_north_money_4_diff]
    hsgt_north_money_5_list.reverse()
    hsgt_dict['hsgt_north_money_5_list'] = hsgt_north_money_5_list
    #分析1、2、3、4、5日短期市场沪港通资金成交较平均值波动情况
    hsgt_hgt_money_5_list = []
    hsgt_hgt_money_avg_5 = round(df_hsgt['hgt'][0:5].mean()*100,2)
    hsgt_hgt_money_0_diff = round(hsgt_hgt_money_0-hsgt_hgt_money_avg_5,2)
    hsgt_hgt_money_1_diff = round(hsgt_hgt_money_1-hsgt_hgt_money_avg_5,2)
    hsgt_hgt_money_2_diff = round(hsgt_hgt_money_2-hsgt_hgt_money_avg_5,2)
    hsgt_hgt_money_3_diff = round(hsgt_hgt_money_3-hsgt_hgt_money_avg_5,2)
    hsgt_hgt_money_4_diff = round(hsgt_hgt_money_4-hsgt_hgt_money_avg_5,2)
    hsgt_dict['hsgt_hgt_money_avg_5'] = hsgt_hgt_money_avg_5
    hsgt_hgt_money_5_list = [hsgt_hgt_money_0_diff,
                             hsgt_hgt_money_1_diff,
                             hsgt_hgt_money_2_diff,
                             hsgt_hgt_money_3_diff,
                             hsgt_hgt_money_4_diff]
    hsgt_hgt_money_5_list.reverse()
    hsgt_dict['hsgt_hgt_money_5_list'] = hsgt_hgt_money_5_list
    #分析1、2、3、4、5日短期市场深港通资金成交较平均值波动情况
    hsgt_sgt_money_5_list=[]
    hsgt_sgt_money_avg_5 = round(df_hsgt['sgt'][0:5].mean()*100,2)
    hsgt_sgt_money_0_diff = round(hsgt_sgt_money_0-hsgt_sgt_money_avg_5,2)
    hsgt_sgt_money_1_diff = round(hsgt_sgt_money_1-hsgt_sgt_money_avg_5,2)
    hsgt_sgt_money_2_diff = round(hsgt_sgt_money_2-hsgt_sgt_money_avg_5,2)
    hsgt_sgt_money_3_diff = round(hsgt_sgt_money_3-hsgt_sgt_money_avg_5,2)
    hsgt_sgt_money_4_diff = round(hsgt_sgt_money_4-hsgt_sgt_money_avg_5,2)   
    hsgt_dict['hsgt_sgt_money_avg_5'] = hsgt_sgt_money_avg_5
    hsgt_sgt_money_5_list = [hsgt_sgt_money_0_diff,
                             hsgt_sgt_money_1_diff,
                             hsgt_sgt_money_2_diff,
                             hsgt_sgt_money_3_diff,
                             hsgt_sgt_money_4_diff]
    hsgt_sgt_money_5_list.reverse()
    hsgt_dict['hsgt_sgt_money_5_list'] = hsgt_sgt_money_5_list    
    return hsgt_dict

def get_data_limits():
    ttlist = get_lasttradedatelist(0,100)
    ttlist.reverse()
    limits_dict = {}
    #今日昨日涨跌停个股数
    df_limit_u_0 = pro.limit_list(trade_date=ttlist[0], limit_type='U')
    df_limit_d_0 = pro.limit_list(trade_date=ttlist[0], limit_type='D')
    if (df_limit_u_0.empty):
        limits_dict['tradedate_0'] = ttlist[1]
        limits_dict['tradedate_1'] = ttlist[2]
        limits_dict['tradedate_2'] = ttlist[3]
        limits_dict['tradedate_3'] = ttlist[4]
        limits_dict['tradedate_4'] = ttlist[5]
        limits_dict['tradedate_5_list'] = ttlist[1:6]
        
        df_limit_u_0 = pro.limit_list(trade_date=ttlist[1], limit_type='U')
        df_limit_u_1 = pro.limit_list(trade_date=ttlist[2], limit_type='U')
        df_limit_u_2 = pro.limit_list(trade_date=ttlist[3], limit_type='U')
        df_limit_u_3 = pro.limit_list(trade_date=ttlist[4], limit_type='U')
        df_limit_u_4 = pro.limit_list(trade_date=ttlist[5], limit_type='U')
        
        df_limit_d_0 = pro.limit_list(trade_date=ttlist[1], limit_type='D')
        df_limit_d_1 = pro.limit_list(trade_date=ttlist[2], limit_type='D')
        df_limit_d_2 = pro.limit_list(trade_date=ttlist[3], limit_type='D')
        df_limit_d_3 = pro.limit_list(trade_date=ttlist[4], limit_type='D')
        df_limit_d_4 = pro.limit_list(trade_date=ttlist[5], limit_type='D')        
    else:
        limits_dict['tradedate_5_list'] = ttlist[0:5]
        limits_dict['tradedate_0'] = ttlist[0]
        limits_dict['tradedate_1'] = ttlist[1]
        limits_dict['tradedate_2'] = ttlist[2]
        limits_dict['tradedate_3'] = ttlist[3]
        limits_dict['tradedate_4'] = ttlist[4]
        df_limit_u_1 = pro.limit_list(trade_date=ttlist[1], limit_type='U')
        df_limit_u_2 = pro.limit_list(trade_date=ttlist[2], limit_type='U')
        df_limit_u_3 = pro.limit_list(trade_date=ttlist[3], limit_type='U')
        df_limit_u_4 = pro.limit_list(trade_date=ttlist[4], limit_type='U')        
        df_limit_d_1 = pro.limit_list(trade_date=ttlist[1], limit_type='D')
        df_limit_d_2 = pro.limit_list(trade_date=ttlist[2], limit_type='D')
        df_limit_d_3 = pro.limit_list(trade_date=ttlist[3], limit_type='D')
        df_limit_d_4 = pro.limit_list(trade_date=ttlist[4], limit_type='D')        
    df_limit_u_0_len = len(df_limit_u_0)
    df_limit_u_1_len = len(df_limit_u_1)
    df_limit_u_2_len = len(df_limit_u_2)
    df_limit_u_3_len = len(df_limit_u_3)
    df_limit_u_4_len = len(df_limit_u_4)
    
    df_limit_d_0_len = len(df_limit_d_0)
    df_limit_d_1_len = len(df_limit_d_1)
    df_limit_d_2_len = len(df_limit_d_2)
    df_limit_d_3_len = len(df_limit_d_3)
    df_limit_d_4_len = len(df_limit_d_4)      
    #分析当前市场涨停个股数情况
    limits_dict['df_limit_u_0_len'] = df_limit_u_0_len
    limits_dict['df_limit_u_1_len'] = df_limit_u_1_len
    limits_dict['df_limit_u_2_len'] = df_limit_u_2_len
    limits_dict['df_limit_u_3_len'] = df_limit_u_3_len
    limits_dict['df_limit_u_4_len'] = df_limit_u_4_len    
    #分析当前市场跌停个股数情况
    limits_dict['df_limit_d_0_len'] = df_limit_d_0_len
    limits_dict['df_limit_d_1_len'] = df_limit_d_1_len
    limits_dict['df_limit_d_2_len'] = df_limit_d_2_len
    limits_dict['df_limit_d_3_len'] = df_limit_d_3_len
    limits_dict['df_limit_d_4_len'] = df_limit_d_4_len    
    #N日平均涨跌停个股数差额
    #分析1、2、3、4、5日短期市场较5日平均涨停个股数波动情况
    df_limit_u_5_list = []
    df_limit_u_5_list = [df_limit_u_0_len,
                         df_limit_u_1_len,
                         df_limit_u_2_len,
                         df_limit_u_3_len,
                         df_limit_u_4_len]
    limits_dict['df_limit_u_5_list'] = df_limit_u_5_list    
    limits_dict['df_limit_u_5_avg'] = round(np.mean(df_limit_u_5_list),1)
    #分析1、2、3、4、5日短期市场较5日平均跌停个股数波动情况
    df_limit_d_5_list = []
    df_limit_d_5_list = [df_limit_d_0_len,
                         df_limit_d_1_len,
                         df_limit_d_2_len,
                         df_limit_d_3_len,
                         df_limit_d_4_len]
    limits_dict['df_limit_d_5_list'] = df_limit_d_5_list    
    limits_dict['df_limit_d_5_avg'] = round(np.mean(df_limit_d_5_list),1)
    return limits_dict
#limits_dict = get_data_limits()

#柱状图函数
def draw_vbar_data_avg_width(data,colorname,avgcolorname,width):
    data.reverse()
    data_array = np.array(data)
    avgline = round(np.mean(data_array),4)
    p = figure(width=width,height=300)
    p.vbar(x=range(len(data_array)), width=0.3,top=data_array,color=colorname)
    p.line(range(len(data_array)),avgline,color=avgcolorname,legend=str(avgline))
    p.legend.location = "top_left"
    return p

#折线图函数
def draw_line_data_datetime_avg_width(data,datearray,colorname,avgcolorname,width):
    data_array = np.array(data)
    date_array = np.array(datearray)
    avgline = round(np.mean(data_array),2)
    p = figure(width=width,height=300,x_range=date_array)
    #p.vbar(x=date_array, width=0.3,top=data_array,color=colorname)
    p.line(date_array,data_array,color=colorname)
    p.line(date_array,avgline,color=avgcolorname,legend=str(avgline))
    p.legend.location = "top_left"
    source = ColumnDataSource(
    data=dict(
        tradedatearry=date_array,
        paramarry = data_array))
    labels = LabelSet(
    x="tradedatearry",
    y="paramarry",
    text="paramarry",
    level="glyph",
    x_offset=5,
    y_offset=0,
    source = source
    #render_mode="canvas"
    )
    p.add_layout(labels)
    return p

def get_daily_analysis_category_industry():
    df = get_col_df('daily_analysis_category_industry')
    df_select = df[(df['stocks_up_count_ratio']==1.0) & (df['stocks_limit_count']>1)]
    #df_result = pd.DataFrame()
    #df_result['title'] = df_select['industry_name']
    #df_result['start'] = df_select['trade_date']
    #df_result['end'] = df_select['trade_date']
    arrlist = []
    for index, row in df_select.iterrows():
        #print (row['industry_name'],row['trade_date'])
        item_dict = {}
        item_dict['title'] = row['industry_name']
        item_dict['start'] = row['trade_date']
        item_dict['end'] = row['trade_date']
        arrlist.append(item_dict)    
    #print (arrlist)
    return arrlist
#arrlist = get_daily_analysis_category_industry()

def get_year_analysis_category_industry():
    df = get_col_df('daily_analysis_category_industry')
    df_industry_list = get_col_df('stocksbasic_industry_list')
    df_select = df[(df['stocks_up_count_ratio']==1.0) & (df['stocks_limit_count']>1)]
    df_select_industry_name = df_select.groupby('industry_name')
    result_df = pd.DataFrame()
    for name,group in df_select_industry_name:
        result_dict = {}
        df_group=pd.DataFrame(group)
        result_dict['industry_name'] = name
        trade_date_list = df_group['trade_date'].tolist()
        result_dict['trade_date_list'] = trade_date_list
        for tradedate in trade_date_list:
            trademonth = tradedate[:6]
            month = tradedate[4:6]
            result_dict['month_'+month] = trademonth        
        result_df = result_df.append(result_dict,ignore_index=True)
    result_df = pd.merge(df_industry_list, result_df, how='left', on='industry_name')
    result_df = result_df.where((result_df.notna()),None)
    return result_df
#df = get_year_analysis_category_industry()

def get_daily_analysis_category_concept():
    df = get_col_df('daily_analysis_category_concept')
    df_select = df[(df['stocks_up_count_ratio']==1.0) & (df['stocks_limit_count']>1)]
    arrlist = []
    for index, row in df_select.iterrows():
        item_dict = {}
        item_dict['title'] = row['concept_name']
        item_dict['start'] = row['trade_date']
        item_dict['end'] = row['trade_date']
        arrlist.append(item_dict)    
    return arrlist
df = get_daily_analysis_category_concept()
def get_year_analysis_category_concept():
    df = get_col_df('daily_analysis_category_concept')
    df_concept_list = get_col_df('concept_list')
    df_select = df[(df['stocks_up_count_ratio']==1.0) & (df['stocks_limit_count']>1)]
    df_select_concept_name = df_select.groupby('concept_name')
    result_df = pd.DataFrame()
    for name,group in df_select_concept_name:
        result_dict = {}
        df_group=pd.DataFrame(group)
        result_dict['concept_name'] = name
        trade_date_list = df_group['trade_date'].tolist()
        result_dict['trade_date_list'] = trade_date_list
        for tradedate in trade_date_list:
            trademonth = tradedate[:6]
            month = tradedate[4:6]
            result_dict['month_'+month] = trademonth        
        result_df = result_df.append(result_dict,ignore_index=True)
    result_df = pd.merge(df_concept_list, result_df, how='left', on='concept_name')
    result_df = result_df.where((result_df.notna()),None)
    return result_df
#df_year = get_year_analysis_category_concept()
#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#主面板
@router.get('/market/mainpanel/')
async def get_market_panel(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    limits_data = get_data_limits()
    p1 = draw_line_data_datetime_avg_width(limits_data['df_limit_u_5_list'],limits_data['tradedate_5_list'],'blue','black',900)
    script1,div1 = components(p1)
    p2 = draw_line_data_datetime_avg_width(limits_data['df_limit_d_5_list'],limits_data['tradedate_5_list'],'green','black',900)
    script2,div2 = components(p2)
    return tmp.TemplateResponse('market_mainpanel.html',
                                {'request':request,
                                 'limits_dict':limits_data,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })

    
#涨停个股数走势分析
@router.get('/market/limitstocks/')
async def get_market_limitstocks(request:Request):  # async加了就支持异步  把Request赋值给request    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    limits_data = get_data_limits()
    p1 = draw_line_data_datetime_avg_width(limits_data['df_limit_u_5_list'],limits_data['tradedate_5_list'],'blue','black',900)
    script1,div1 = components(p1)
    p2 = draw_line_data_datetime_avg_width(limits_data['df_limit_d_5_list'],limits_data['tradedate_5_list'],'green','black',900)
    script2,div2 = components(p2)
    return tmp.TemplateResponse('market_limits.html',
                                {'request':request,
                                 'limits_dict':limits_data,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "js_res":js_res,
                                 "css_res":css_res                            
                                 })    
    
#南北资金走势分析
@router.get('/market/hsgt/')
async def get_market_hsgt(request:Request):  # async加了就支持异步  把Request赋值给request    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    hsgt_data = get_data_hsgt()
    p1 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_south_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'blue','black',600)
    script1,div1 = components(p1)
    p2 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_ggt_ss_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'blue','black',600)
    script2,div2 = components(p2)
    p3 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_ggt_sz_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'blue','black',600)
    script3,div3 = components(p3)
    p4 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_north_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'red','black',600)
    script4,div4 = components(p4)
    p5 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_hgt_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'red','black',600)
    script5,div5 = components(p5)
    p6 = draw_line_data_datetime_avg_width(hsgt_data['hsgt_sgt_money_5_list'],hsgt_data['hsgt_tradedate_list_5'],'red','black',600)
    script6,div6 = components(p6)
    return tmp.TemplateResponse('market_snmoneyflow.html',
                                {'request':request,
                                 'hsgt_data':hsgt_data,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "p_script5":script5,
                                 "p_div5":div5,
                                 "p_script6":script6,
                                 "p_div6":div6,
                                 "js_res":js_res,
                                 "css_res":css_res                            
                                 })


#指数走势分析
@router.get('/market/indexs/')
async def get_market_index(request:Request):  # async加了就支持异步  把Request赋值给request    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    indexs_data = get_data_indexs()
    indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
    indexs_data_000001 = indexs_data[0]
    p1 = draw_vbar_data_avg_width(indexs_data_000001['index_daily_chg_pct_list'],'red','black',600)
    script1,div1 = components(p1)
    p2 = draw_vbar_data_avg_width(indexs_data_000001['index_weekly_chg_pct_list'],'blue','black',600)
    script2,div2 = components(p2)
    p3 = draw_vbar_data_avg_width(indexs_data_000001['index_monthly_chg_pct_list'],'green','black',600)
    script3,div3 = components(p3)
    p4 = draw_vbar_data_avg_width(indexs_data_000001['index_daily_amount_chg_pct_list'],'red','black',600)
    script4,div4 = components(p4)
    p5 = draw_vbar_data_avg_width(indexs_data_000001['index_weekly_amount_chg_pct_list'],'blue','black',600)
    script5,div5 = components(p5)
    p6 = draw_vbar_data_avg_width(indexs_data_000001['index_monthly_amount_chg_pct_list'],'green','black',600)
    script6,div6 = components(p6)
    
    indexs_data_399001 = indexs_data[1]
    p7 = draw_vbar_data_avg_width(indexs_data_399001['index_daily_chg_pct_list'],'red','black',600)
    script7,div7 = components(p7)
    p8 = draw_vbar_data_avg_width(indexs_data_399001['index_weekly_chg_pct_list'],'blue','black',600)
    script8,div8 = components(p8)
    p9 = draw_vbar_data_avg_width(indexs_data_399001['index_monthly_chg_pct_list'],'green','black',600)
    script9,div9 = components(p9)
    p10 = draw_vbar_data_avg_width(indexs_data_399001['index_daily_amount_chg_pct_list'],'red','black',600)
    script10,div10 = components(p10)
    p11 = draw_vbar_data_avg_width(indexs_data_399001['index_weekly_amount_chg_pct_list'],'blue','black',600)
    script11,div11 = components(p11)
    p12 = draw_vbar_data_avg_width(indexs_data_399001['index_monthly_amount_chg_pct_list'],'green','black',600)
    script12,div12 = components(p12)
    
    indexs_data_399005 = indexs_data[2]
    p13 = draw_vbar_data_avg_width(indexs_data_399005['index_daily_chg_pct_list'],'red','black',600)
    script13,div13 = components(p13)
    p14 = draw_vbar_data_avg_width(indexs_data_399005['index_weekly_chg_pct_list'],'blue','black',600)
    script14,div14 = components(p14)
    p15 = draw_vbar_data_avg_width(indexs_data_399005['index_monthly_chg_pct_list'],'green','black',600)
    script15,div15 = components(p15)
    p16 = draw_vbar_data_avg_width(indexs_data_399005['index_daily_amount_chg_pct_list'],'red','black',600)
    script16,div16 = components(p16)
    p17 = draw_vbar_data_avg_width(indexs_data_399005['index_weekly_amount_chg_pct_list'],'blue','black',600)
    script17,div17 = components(p17)
    p18 = draw_vbar_data_avg_width(indexs_data_399005['index_monthly_amount_chg_pct_list'],'green','black',600)
    script18,div18 = components(p18)
    
    indexs_data_399006 = indexs_data[3]
    p19 = draw_vbar_data_avg_width(indexs_data_399006['index_daily_chg_pct_list'],'red','black',600)
    script19,div19 = components(p19)
    p20 = draw_vbar_data_avg_width(indexs_data_399006['index_weekly_chg_pct_list'],'blue','black',600)
    script20,div20 = components(p20)
    p21 = draw_vbar_data_avg_width(indexs_data_399006['index_monthly_chg_pct_list'],'green','black',600)
    script21,div21 = components(p21)
    p22 = draw_vbar_data_avg_width(indexs_data_399006['index_daily_amount_chg_pct_list'],'red','black',600)
    script22,div22 = components(p22)
    p23 = draw_vbar_data_avg_width(indexs_data_399006['index_weekly_amount_chg_pct_list'],'blue','black',600)
    script23,div23 = components(p23)
    p24 = draw_vbar_data_avg_width(indexs_data_399006['index_monthly_amount_chg_pct_list'],'green','black',600)
    script24,div24 = components(p24)    
    return tmp.TemplateResponse('market_index.html',
                                {'request':request,
                                 'indexs_data_000001':indexs_data_000001,
                                 'indexcode1':'000001.SH',
                                 'indexname1':indexnamedict['000001.SH'],
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "p_script5":script5,
                                 "p_div5":div5,
                                 "p_script6":script6,
                                 "p_div6":div6,
                                 'indexs_data_399001':indexs_data_399001,
                                 'indexcode2':'399001.SZ',
                                 'indexname2':indexnamedict['399001.SZ'],
                                 "p_script7":script7,
                                 "p_div7":div7,
                                 "p_script8":script8,
                                 "p_div8":div8,
                                 "p_script9":script9,
                                 "p_div9":div9,
                                 "p_script10":script10,
                                 "p_div10":div10,
                                 "p_script11":script11,
                                 "p_div11":div11,
                                 "p_script12":script12,
                                 "p_div12":div12, 
                                 'indexs_data_399005':indexs_data_399005,
                                 'indexcode3':'399005.SZ',
                                 'indexname3':indexnamedict['399005.SZ'],
                                 "p_script13":script13,
                                 "p_div13":div13,
                                 "p_script14":script14,
                                 "p_div14":div14,
                                 "p_script15":script15,
                                 "p_div15":div15,
                                 "p_script16":script16,
                                 "p_div16":div16,
                                 "p_script17":script17,
                                 "p_div17":div17,
                                 "p_script18":script18,
                                 "p_div18":div18,
                                 'indexs_data_399006':indexs_data_399006,
                                 'indexcode4':'399006.SZ',
                                 'indexname4':indexnamedict['399006.SZ'],
                                 "p_script19":script19,
                                 "p_div19":div19,
                                 "p_script20":script20,
                                 "p_div20":div20,
                                 "p_script21":script21,
                                 "p_div21":div21,
                                 "p_script22":script22,
                                 "p_div22":div22,
                                 "p_script23":script23,
                                 "p_div23":div23,
                                 "p_script24":script24,
                                 "p_div24":div24,                                     
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
@router.get('/market/industry/calendar/')
async def get_market_industry_calendar(request:Request):  # async加了就支持异步  把Request赋值给request
    industry_list = get_daily_analysis_category_industry()
    return tmp.TemplateResponse('market_industry_calendar.html',
                                {'request':request,
                                 'industry_list':industry_list
                                 })

@router.get('/market/industry/year/')
async def get_market_industry_year(request:Request):  # async加了就支持异步  把Request赋值给request
    industry_list = get_year_analysis_category_industry().to_dict('records')
    return tmp.TemplateResponse('market_industry_year.html',
                                {'request':request,
                                 'industry_list':industry_list
                                 })
    
@router.get('/market/concept/calendar/')
async def get_market_concept_calendar(request:Request):  # async加了就支持异步  把Request赋值给request
    concept_list = get_daily_analysis_category_concept()
    return tmp.TemplateResponse('market_concept_calendar.html',
                                {'request':request,
                                 'concept_list':concept_list
                                 })

@router.get('/market/concept/year/')
async def get_market_concept_year(request:Request):  # async加了就支持异步  把Request赋值给request
    concept_list = get_year_analysis_category_concept().to_dict('records')
    return tmp.TemplateResponse('market_concept_year.html',
                                {'request':request,
                                 'concept_list':concept_list
                                 })