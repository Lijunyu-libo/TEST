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
from fastapi import FastAPI,Form
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#查询库

from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
#获取指数函数 参数
def getindex(col):
    mycollection=mydb[col]
    index_rs = mycollection.find()
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    return rs_json
#print (rs_json)

#模板渲染    
app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='templates')

#获取概念列表
@app.get('/stocks/topstocks/concept/')
async def get_tmp(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('topstocks_concept_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'count':len(getindex('concept')),
                                 'ccodes':getindex('concept'),
                                 'stockscount':0,  # 额外的参数可有可无
                                 'stocks':[]                                 
                                 })



# 获取某概念代码中成分股列表和龙头股属性表
@app.get('/stocks/topstocks/concept/{ccode}') 
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

@app.post('/topstock/save/')  # 接受post请求
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
    
    
    
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)