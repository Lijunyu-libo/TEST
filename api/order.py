# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 18:06:27 2021
#下单模块
@author: iFunk
"""
import pandas as pd
import numpy as np
import math
import inspect
import time
import datetime


from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates

import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

DATESTAMP = time.strftime("%Y%m%d",)

#TEXT TO LIST
def get_order_list():
    with open('./data/order_'+DATESTAMP+'.txt','r') as f:
    #with open('./data/test_macd_cross_abovezreo_20210105.txt','r') as f:
        text_data = f.read()
        text_list = eval(text_data)
    for item in text_list:
        print (item)
    return text_list

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

def get_order_analysis():
    order_list = get_order_list()
    df_result = pd.DataFrame()
    #DAILYBASIC
    df_dailybasic_lastday = get_col_df('stocks_dailybasic_lastday')
    df_result = df_dailybasic_lastday[df_dailybasic_lastday.ts_code.isin(order_list)]
    df_result = df_result.round(2)
    df_result['total_mv'] = (df_result['total_mv']/10000).round(0)
    df_result['circ_mv'] = (df_result['circ_mv']/10000).round(0)
    df_result = df_result.drop(['name','close','industry'],axis=1)
    #DAILY
    df_daily_lastday = get_col_df('stocks_daily_lastday')
    df_daily_lastday['pct_chg'] = df_daily_lastday['pct_chg'].round(1)
    df_daily_lastday['amount'] = (df_daily_lastday['amount']/100000).round(1)
    df_result = pd.merge(df_result, df_daily_lastday, how='left', on='ts_code')
    
    #strategy    
    df_stock_strategy = pd.DataFrame()
    for stockcode in order_list:
        stock_dict = {}
        df_daily_stock = get_col_df('daily_qfq_macd_'+stockcode)
        #change_in_5 = (df_daily_stock['close'][0]-df_daily_stock['close'][5])/df_daily_stock['close'][5]
        stock_dict['ts_code'] = stockcode
        stock_dict['pct_chg_in_5'] = round(df_daily_stock['pct_chg'][0:5].sum(),2)
        stock_dict['pct_chg_in_10'] = round(df_daily_stock['pct_chg'][0:10].sum(),2)
        stock_dict['pct_chg_in_20'] = round(df_daily_stock['pct_chg'][0:20].sum(),2)
        stock_dict['pct_chg_in_60'] = round(df_daily_stock['pct_chg'][0:60].sum(),2)
        df_stock_strategy = df_stock_strategy.append(stock_dict,ignore_index=True)
    df_result = pd.merge(df_result, df_stock_strategy, how='left', on='ts_code')
    return df_result

# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

@router.get('/order/analysis/')
async def get_market_concept_year(request:Request):  # async加了就支持异步  把Request赋值给request
    data_list = get_order_analysis().to_dict('records')
    return tmp.TemplateResponse('order_analysis.html',
                                {'request':request,
                                 'trade_date':DATESTAMP,
                                 'data_count':len(data_list),
                                 'data_list':data_list
                                 })