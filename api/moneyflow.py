# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 10:42:26 2021
#资金流向数据
@author: iFunk
"""
import json
from io import StringIO
import demjson
from typing import Optional
import pandas as pd
import numpy as np
from starlette.requests import Request
from fastapi import APIRouter,Form
router = APIRouter()
from starlette.templating import Jinja2Templates
import tushare as ts
import datetime
from bokeh.plotting import figure,show
from bokeh.embed import components
from bokeh.resources import INLINE
from dateutil.parser import parse
from collections import Counter
from math import pi
from bokeh.palettes import Category20c
from bokeh.transform import cumsum
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
#查询库

from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

#获取库数据函数 返回df
def get_data_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df
#获取指数函数 参数
def get_data_dict(col):
    mycollection=mydb[col]
    index_rs = mycollection.find()
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json


# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#最近交易日个股资金流向数据
@router.get('/moneyflow/lastday/')
async def get_moneyflow_lastday(request:Request):  # async加了就支持异步  把Request赋值给request
    moneyflow_lastday = get_data_df('moneyflow_lastday').to_dict('records')
    return tmp.TemplateResponse('moneyflow_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'data_count':len(moneyflow_lastday),
                                 'data_list':moneyflow_lastday,
                                 'data_name':'最近交易日资金流向'
                                 })

#最近交易周个股资金流向数据
@router.get('/moneyflow/lastweek/')
async def get_moneyflow_lastweek(request:Request):  # async加了就支持异步  把Request赋值给request
    moneyflow_lastweek = get_data_df('moneyflow_lastweek').to_dict('records')
    return tmp.TemplateResponse('moneyflow_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'data_count':len(moneyflow_lastweek),
                                 'data_list':moneyflow_lastweek,
                                 'data_name':'最近交易周资金流向'
                                 })

#最近交易周个股资金流向数据
@router.get('/moneyflow/lastmonth/')
async def get_moneyflow_lastmonth(request:Request):  # async加了就支持异步  把Request赋值给request
    moneyflow_lastmonth = get_data_df('moneyflow_lastmonth').to_dict('records')
    return tmp.TemplateResponse('moneyflow_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'data_count':len(moneyflow_lastmonth),
                                 'data_list':moneyflow_lastmonth,
                                 'data_name':'最近交易月资金流向'
                                 })
#个股日资金流向数据

#个股周资金流向数据

#个股月资金流向数据
