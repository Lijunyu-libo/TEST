# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: iFunk
"""
import json
from io import StringIO
import demjson
from typing import Optional
import pandas as pd
from starlette.requests import Request

from fastapi import APIRouter,Form
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
#获取指数函数 参数
def get_col_json(col):
    mycollection=mydb[col]
    index_rs = mycollection.find()
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json

#获条件数据函数 参数小于
def get_data_less(index,param,value):
    col=mydb[index]
    value = int(value)
    query = {param:{ "$lt": value }}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获条件数据函数 参数大于等于
def get_data_greateq(index,param,value):
    col=mydb[index]
    value = float(value)
    query = {param:{ "$gte": value }}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获条件数据函数 参数大于等于 小于等于
def get_data_between(index,param,value_gte,value_lte):
    col=mydb[index]
    value_gte = float(value_gte)
    value_lte = float(value_lte)
    query = { "$and": [{param:{"$gt":value_gte}},{param:{"$lte":value_lte}}]}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')


#获取最新daily数据列表
@router.get('/stocks/daily/lastday/')
async def get_daily_lastday(request:Request):  # async加了就支持异步  把Request赋值给request
    stocks_daily_lastday = get_col_json('stocks_daily_lastday')
    return tmp.TemplateResponse('stocks_daily_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'period':'日',
                                 'stockscount':len(stocks_daily_lastday),  # 额外的参数可有可无
                                 'stocks':stocks_daily_lastday                                 
                                 })

#获取最新weekly数据列表
@router.get('/stocks/weekly/last/')
async def get_weekly_last(request:Request):  # async加了就支持异步  把Request赋值给request
    stocks_weekly_last = get_col_json('stocks_weekly_last')
    return tmp.TemplateResponse('stocks_daily_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'period':'周',
                                 'stockscount':len(stocks_weekly_last),  # 额外的参数可有可无
                                 'stocks':stocks_weekly_last                                
                                 })
    

#获取最新monthly数据列表
@router.get('/stocks/monthly/last/')
async def get_monthly_last(request:Request):  # async加了就支持异步  把Request赋值给request
    stocks_monthly_last = get_col_json('stocks_monthly_last')
    return tmp.TemplateResponse('stocks_daily_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'period':'月',
                                 'stockscount':len(stocks_monthly_last),  # 额外的参数可有可无
                                 'stocks':stocks_monthly_last                                
                                 })    