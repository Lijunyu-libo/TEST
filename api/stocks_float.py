# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
限售股解禁数据
@author: iFunk
"""
import json
from io import StringIO
import datetime
from typing import Optional
import pandas as pd
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
#查询库

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

#获取指数函数 参数
def get_data(col):
    mycollection=mydb[col]
    rs = mycollection.find()
    rs_json = []
    for i in rs:
        rs_json.append(i)
    return rs_json

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

@router.get('/stocks/fina/float/last/')
async def get_float_last(request:Request):  # async加了就支持异步  把Request赋值给request
    #anndates =  [get_day_time(1),get_day_time(2),get_day_time(3),get_day_time(4),get_day_time(5)]
    #lastperiod = get_day_time(1)
    data_forecast_last = get_data('float_last')
    return tmp.TemplateResponse('float_stocks.html',
                                {'request':request,  # 一定要返回request
                                 #'anndates':anndates,
                                 #'period':lastperiod,
                                 'stockscount':len(data_forecast_last),
                                 'stocks':data_forecast_last                             
                                 })

@router.get('/stocks/fina/float/{period}/')
async def get_float_periodt(period:str,request:Request):  # async加了就支持异步  把Request赋值给request
    anndates =  [get_day_time(1),get_day_time(2),get_day_time(3),get_day_time(4),get_day_time(5)]
    data_forecast_period = get_data('float_'+period)
    return tmp.TemplateResponse('float_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'anndates':anndates,
                                 'period':period,
                                 'stockscount':len(data_forecast_period),
                                 'stocks':data_forecast_period                             
                                 })


@router.get('/stocks/fina/float/year/')
async def get_float_year(request:Request):  # async加了就支持异步  把Request赋值给request
    data_forecast_last = get_data('float_year')
    return tmp.TemplateResponse('float_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_forecast_last),
                                 'stocks':data_forecast_last                             
                                 })