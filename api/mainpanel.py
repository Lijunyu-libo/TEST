# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 10:51:28 2020
#行业分析模块
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

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取条件集合函数 参数 col param 返回df
def get_col_param_df(col,param,value):
    mycollection=mydb[col]
    query = {param:value}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取数据函数 排序
def get_col_sort_df(collection,colum,asc,count):
    mycollection=mydb[collection]
    rs_col = mycollection.find().limit(count).sort([(colum,asc)])
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取数据函数 排序
def get_col_sort2_df(collection,colum1,asc1,colum2,asc2,count):
    mycollection=mydb[collection]
    rs_col = mycollection.find().limit(count).sort([(colum1,asc1),(colum2,asc2)])
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col



#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#主面板

#行业

#三大产业分类数据
@router.get('/mainpanel/')
async def get_three(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #industry_dict = get_col_df('industry_three').to_dict('records')
    return tmp.TemplateResponse('mainpanel.html',
                                {'request':request,
                                 #'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })

