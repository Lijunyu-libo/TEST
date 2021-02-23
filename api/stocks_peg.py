# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
列表显示各季度PEG
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
    #print (rs_json)
    return rs_json

#获条件数据函数 参数等于
def get_data_eq(index,param,value):
    col=mydb[index]
    #value = int(value)
    query = {param:value}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获条件数据函数 参数小于
def get_data_less(index,param,value):
    col=mydb[index]
    value = float(value)
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
app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='templates')


#获取20200630季度PEG数据列表
@app.get('/stocks/peg/')
async def get_peg(request:Request):  # async加了就支持异步  把Request赋值给request
    period = '20200630'
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,  # 一定要返回request
                                 'period':period,
                                 'stockscount':len(getindex(period+'_peg')),  # 额外的参数可有可无
                                 'stocks':getindex(period+'_peg')                                 
                                 })
#获取当前季度PEG数据列表
@app.get('/stocks/peg/{period}/')
async def get_peg_period(request:Request,period:str):  # async加了就支持异步  把Request赋值给request   
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,  # 一定要返回request
                                 'period':period,
                                 'stockscount':len(getindex(period+'_peg')),  # 额外的参数可有可无
                                 'stocks':getindex(period+'_peg')                                 
                                 })    

@app.post('/stocks/peg/{period}/stockcode/')
async def get_peg_period_stockcode(request:Request,period:str,stockcode:str=Form(...)):  # async加了就支持异步  把Request赋值给request   
    data_stockcode = get_data_eq(period+'_peg','ts_code',stockcode)
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,  # 一定要返回request
                                 'period':period,
                                 'stockscount':len(data_stockcode),  # 额外的参数可有可无
                                 'stocks':data_stockcode                                 
                                 })  
@app.post('/stocks/peg/{period}/pegless/')
async def get_peg_period_pegless(request:Request,period:str,peg:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_pegless = get_data_less(period+'_peg','PEG',peg)
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,
                                 'period':period,
                                 'stockscount':len(data_pegless),  # 额外的参数可有可无
                                 'stocks':data_pegless                                
                                 })
    

@app.post('/stocks/peg/{period}/ggreateq/')
async def get_peg_period_ggreat(request:Request,period:str,g:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_g = get_data_greateq(period+'_peg','G',g)
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,  # 一定要返回request
                                 'period':period,
                                 'stockscount':len(data_g),  # 额外的参数可有可无
                                 'stocks':data_g                                
                                 })
    

@app.post('/stocks/peg/{period}/pebetween/')
async def get_peg_period_pebetween(request:Request,period:str,pegte:str=Form(...),pelte:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_pe = get_data_between(period+'_peg','pe_ttm',pegte,pelte)
    return tmp.TemplateResponse('peg_stocks_period.html',
                                {'request':request,  # 一定要返回request
                                 'period':period,
                                 'stockscount':len(data_pe),  # 额外的参数可有可无
                                 'stocks':data_pe                                
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