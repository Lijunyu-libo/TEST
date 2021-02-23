# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: iFunk
"""
from bokeh.plotting import figure,show
from bokeh.embed import components
from bokeh.resources import INLINE
from dateutil.parser import parse
import json
from io import StringIO
import demjson
from typing import Optional
import pandas as pd
import numpy as np
from starlette.requests import Request
from fastapi import FastAPI,Form
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#查询库

from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
sheet_list = ['stocks_income']
sheet_namedict = {'stocks_income':'利润表'}

#获取数据函数 参数
def get_col_data(index):
    col=mydb[index]
    rs = col.find()
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获股票代码条件数据函数 参数股票代码
def get_nincomestockcode_data(index,stockcode):
    col=mydb[index]
    query = {"ts_code":stockcode}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获净利润条件数据函数 参数净利润下限
def get_nincomegreat_data(index,value):
    col=mydb[index]
    value = int(value)
    query = {"n_income":{ "$gt": value }}
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

#获净利润条件数据函数 参数净利润下限和上限
def get_nincomebetween_data(index,value_gt,value_lt):
    col=mydb[index]
    value_gt = int(value_gt)
    value_lt = int(value_lt)
    query = { "$and": [{"n_income":{"$gt":value_gt}},{"n_income":{"$lt":value_lt}}]} 
    rs = col.find(query)    
    rs_dict = []
    for i in rs:
        rs_dict.append(i)
    return rs_dict

periods = ['20181231','20190331','20190630','20190930','20191231','20200331','20200630']
#获取某个股历史利润表数据
def get_stock_income(stockcode,periods):
    list_stock_income = []
    for period in periods:          
        col=mydb[period+'_income']
        query = {'ts_code':stockcode}
        rs = col.find(query)
        if rs.count() == 0:
            print ('no data')
        else: 
            print(rs[0])
            list_stock_income.append(rs[0])
    return list_stock_income
    
#get_stock_income('002567.SZ','20200630')
#利润表折线图函数
def draw_line_income(stockcode,periods):
    # EPS 总收入 总成本 净利润
    df_stock_income = pd.DataFrame(get_stock_income(stockcode,periods))
    list_revenue = np.array(df_stock_income['revenue'])
    list_total_cogs = np.array(df_stock_income['total_cogs'])
    list_n_income = np.array(df_stock_income['n_income'])   
    df_stock_income['end_date']=df_stock_income['end_date'].apply(str)
    df_stock_income['end_date']=df_stock_income['end_date'].apply(parse)
    datetime_array = np.array(df_stock_income['end_date'], dtype=np.datetime64)
    p = figure(width=900,height=400,x_axis_type="datetime")
    p.line(datetime_array,list_revenue,color='green',legend='营业总收入')
    p.line(datetime_array,list_total_cogs,color='blue',legend='营业总成本')
    p.line(datetime_array,list_n_income,color='yellow',legend='净利润')
    p.legend.location = "top_left"
    return p

#利润表折线图函数
def draw_line_eps(stockcode,periods):
    # EPS 总收入 总成本 净利润
    df_stock_income = pd.DataFrame(get_stock_income(stockcode,periods))
    list_basic_eps = np.array(df_stock_income['basic_eps'])
    df_stock_income['end_date']=df_stock_income['end_date'].apply(str)
    df_stock_income['end_date']=df_stock_income['end_date'].apply(parse)
    datetime_array = np.array(df_stock_income['end_date'], dtype=np.datetime64)
    p = figure(width=900,height=400,x_axis_type="datetime")
    p.line(datetime_array,list_basic_eps,color='red',legend='每股收益')
    p.legend.location = "top_left"
    return p
#模板渲染    
app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='templates')


@app.get('/fina/{sheet_id}')
async def get_fina_data(sheet_id:str,request:Request):  # async加了就支持异步  把Request赋值给request

    return tmp.TemplateResponse('stocks_fina_income.html',
                                {'request':request,  # 一定要返回request
                                 'sheetlist':sheet_list,
                                 'sheetid':sheet_id,
                                 'sheetname':sheet_namedict[sheet_id],
                                 'count':len(get_col_data(sheet_id)),
                                 'finadata':get_col_data(sheet_id)
                                 })

#净利润筛选函数 条件从表单传入
@app.post('/fina/stocks_income/stockcode')
async def get_fina_nincomegreat_data(request:Request,stockcode:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    nincomelist = get_nincomestockcode_data('stocks_income',stockcode)
    return tmp.TemplateResponse('stocks_fina_income.html',
                                {'request':request,  # 一定要返回request
                                 'sheetlist':sheet_list,
                                 'sheetid':'stocks_income',
                                 'sheetname':sheet_namedict['stocks_income'],
                                 'count':len(nincomelist),
                                 'finadata':nincomelist
                                 })
    
#净利润筛选函数 条件从表单传入
@app.post('/fina/stocks_income/nincomegreat')
async def get_fina_nincomegreat_data(request:Request,nincome:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    nincomelist = get_nincomegreat_data('stocks_income',nincome)
    return tmp.TemplateResponse('stocks_fina_income.html',
                                {'request':request,  # 一定要返回request
                                 'sheetlist':sheet_list,
                                 'sheetid':'stocks_income',
                                 'sheetname':sheet_namedict['stocks_income'],
                                 'count':len(nincomelist),
                                 'finadata':nincomelist
                                 })
#净利润筛选函数 条件从表单传入
@app.post('/fina/stocks_income/nincomebetween')
async def get_fina_nincomebetween_data(request:Request,nincomegt:str=Form(...),nincomelt:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    nincomelist = get_nincomebetween_data('stocks_income',nincomegt,nincomelt)
    return tmp.TemplateResponse('stocks_fina_income.html',
                                {'request':request,  # 一定要返回request
                                 'sheetlist':sheet_list,
                                 'sheetid':'stocks_income',
                                 'sheetname':sheet_namedict['stocks_income'],
                                 'count':len(nincomelist),
                                 'finadata':nincomelist
                                 })

@app.get('/fina/{sheet_id}/{stockcode}')
async def get_stock_fina_data(sheet_id:str,stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    plot1 = draw_line_income(stockcode,periods)
    script1,div1 = components(plot1)
    plot2 = draw_line_eps(stockcode,periods)
    script2,div2 = components(plot2)
    return tmp.TemplateResponse('stock_fina_income.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'sheetlist':sheet_list,
                                 'sheetid':sheet_id,
                                 'sheetname':sheet_namedict[sheet_id],
                                 'count':len(get_stock_income(stockcode,periods)),
                                 'finadata':get_stock_income(stockcode,periods),
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

    
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)