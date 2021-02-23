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
#获取指数函数 参数"swl1"
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

@router.get('/stocks/sw/{indexlevel}/')
async def get_tmp(indexlevel,request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('sw_stocks_level.html',
                                {'request':request,  # 一定要返回request
                                 'indexlevel':indexlevel,
                                 'indexscount':len(get_data(indexlevel)),
                                 'indexs':get_data(indexlevel),
                                 'stockscount':0,  # 额外的参数可有可无
                                 'stocks':[]                                 
                                 })


# 获取index代码
@router.get('/stocks/sw/{indexlevel}/{indexcode}/') 
async def get_index_levelstocks(indexlevel,indexcode,request:Request):
    stocks = get_data('stocks_'+indexcode)
    #return {"stockcodes":stocks_items,"stocknames":stocks_items,"count":len(stocks_items)}
    return tmp.TemplateResponse('sw_stocks_level.html',
                                {'request':request,  # 一定要返回request
                                 'indexlevel':indexlevel,
                                 'indexcode':indexcode,
                                 'indexscount':len(get_data(indexlevel)),
                                 'indexs':get_data(indexlevel),
                                 'stockscount':len(stocks),
                                 'stocks':stocks
                                                              
                                 })

# 获取index代码
@router.get('/stocks/swstocks/{indexcode}/') 
async def get_index_stocks(indexcode,request:Request):
    stocks = get_data('stocks_'+indexcode)
    #return {"stockcodes":stocks_items,"stocknames":stocks_items,"count":len(stocks_items)}
    return tmp.TemplateResponse('sw_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'indexcode':indexcode,
                                 'stockscount':len(stocks),
                                 'stocks':stocks
                                                              
                                 })