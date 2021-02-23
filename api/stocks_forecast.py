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
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#查询库

from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
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

@router.get('/stocks/fina/forecast/last/')
async def get_forecast_last(request:Request):  # async加了就支持异步  把Request赋值给request
    anndates =  ['20200331','20200630','20200930','20201231']
    data_forecast_last = get_data('forecast_last')
    return tmp.TemplateResponse('forecast_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'anndates':anndates,
                                 'stockscount':len(data_forecast_last),
                                 'stocks':data_forecast_last                             
                                 })

@router.get('/stocks/fina/forecast/{period}/')
async def get_forecast_periodt(period:str,request:Request):  # async加了就支持异步  把Request赋值给request
    anndates =  ['20200331','20200630','20200930','20201231']
    data_forecast_period = get_data('forecast_'+period)
    return tmp.TemplateResponse('forecast_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'anndates':anndates,
                                 'period':period,
                                 'stockscount':len(data_forecast_period),
                                 'stocks':data_forecast_period                             
                                 })


