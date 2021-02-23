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
def getindex(col):
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


#获取最新dailybasic数据列表
@router.get('/stocks/dailybasic/lastday/')
async def get_dailybasic_lastday(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('stocks_dailybasic_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(getindex('stocks_dailybasic_lastday')),  # 额外的参数可有可无
                                 'stocks':getindex('stocks_dailybasic_lastday')                                 
                                 })

@router.get('/stocks/dailybasic/')
async def get_dailybasic(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('dailybasic_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(getindex('stocks_dailybasic_lastday')),  # 额外的参数可有可无
                                 'stocks':getindex('stocks_dailybasic_lastday')                                 
                                 })
    
#流通市值条件 获取最新dailybasic数据列表
@router.post('/stocks/dailybasic/lastday/mvless/')
async def get_dailybasic_lastday_mvless(request:Request,mv:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_mvless = get_data_less('stocks_dailybasic_lastday','circ_mv',mv)
    return tmp.TemplateResponse('dailybasic_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_mvless),  # 额外的参数可有可无
                                 'stocks':data_mvless                                
                                 })
    
#换手率条件 获取最新dailybasic数据列表
@router.post('/stocks/dailybasic/lastday/turnovergreateq/')
async def get_dailybasic_lastday_tunovergreat(request:Request,turnover:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_turnover = get_data_greateq('stocks_dailybasic_lastday','turnover_rate',turnover)
    return tmp.TemplateResponse('dailybasic_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_turnover),  # 额外的参数可有可无
                                 'stocks':data_turnover                                
                                 })
    
#换手率条件 获取最新dailybasic数据列表
@router.post('/stocks/dailybasic/lastday/turnoverbetween/')
async def get_dailybasic_lastday_tunoverbetween(request:Request,turnovergte:str=Form(...),turnoverlte:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_turnover = get_data_between('stocks_dailybasic_lastday','turnover_rate',turnovergte,turnoverlte)
    return tmp.TemplateResponse('dailybasic_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_turnover),  # 额外的参数可有可无
                                 'stocks':data_turnover                                
                                 })

# 获取某概念代码中成分股列表和龙头股属性表
@router.get('/stocks/topstocks/concept/{ccode}') 
async def get_index_stocks(ccode,request:Request):
    #查询条件下行业代码数据 结果返回应该为1条
    query = { "code": ccode}
    mycollection = mydb['concept']
    stocks_index = mycollection.find(query)   
    stocks_items=stocks_index[0]['stocks']
    stocks_json = eval(stocks_items)
    topstockscollection = mydb['topstocks']
    topstocksarr = []
    for stock in stocks_json:
        #print (stock['stockcode'])
        query = { "ts_code": stock['stockcode']}
        rs_stock = topstockscollection.find(query)
        #print (rs_stock[0]['top_flag'])
        stock['top_flag'] = rs_stock[0]['top_flag']
        stock['top_year'] = rs_stock[0]['top_year']
        stock['top_weighting'] = rs_stock[0]['top_weighting']
        stock['top_concenpt'] = rs_stock[0]['top_concenpt']
        stock['top_industry'] = rs_stock[0]['top_industry']
        stock['top_memo'] = rs_stock[0]['top_memo']
        topstocksarr.append(stock)
    #print (topstocksarr)
    #return {"stockcodes":stocks_items,"stocknames":stocks_items,"count":len(stocks_items)}
    return tmp.TemplateResponse('topstocks_concept_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'count':len(getindex('concept')),
                                 'ccodes':getindex('concept'),
                                 'stockscount':len(topstocksarr),
                                 'stocks':topstocksarr,
                                 'code':ccode                               
                                 })

@router.post('/topstock/save/')  # 接受post请求
async def asve_topstock(request:Request,
                   stockcode:str=Form(...), 
                   top_flag:str=Form(...),
                   top_year:str=Form(...),  # 直接去请求体里面获取username键对应的值并自动转化成字符串类型
                   top_weighting:str=Form(...),  # 直接去请求体里面获取pwd键对应的值并自动转化成整型
                   top_concenpt:str=Form(...),
                   top_industry:str=Form(...),
                   top_memo:str=Form(...),
                   ):    
    print (top_flag)
    topstockscollection = mydb['topstocks']
    query = { "ts_code": stockcode}
    #stock = topstockscollection.find(query)[0]
    newvalues = { "$set": {"top_flag": top_flag,
                           "top_year": top_year,
                           "top_weighting": top_weighting,
                           "top_concenpt": top_concenpt,
                           "top_industry": top_industry,
                           "top_memo": top_memo}}
    topstockscollection.update_one(query, newvalues)
    return 'success'
    
    
