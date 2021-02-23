# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
最新交易日涨停个股分析  近10个交易日涨停个股分析
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
import tushare as ts
import datetime
from bokeh.plotting import figure,show
from bokeh.embed import components
from bokeh.resources import INLINE
from dateutil.parser import parse
from math import pi
from bokeh.palettes import Category20c
from bokeh.transform import cumsum
from bokeh.models import ColumnDataSource
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
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

#获取数据函数 排序
def get_data_sort(collection,colum,asc,count):
    mycollection=mydb[collection]
    rs = mycollection.find().limit(count).sort([(colum,asc)])
    rs_json = []
    for i in rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json
    

#获取stockcode条件集合函数 参数 stockcode
def get_data_stockcode(col,stockcode):
    mycollection=mydb[col]
    query = {'ts_code':stockcode}
    index_rs = mycollection.find(query)
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json

#获条件数据函数 参数小于
def get_data_less(index,param,value):
    col=mydb[index]
    value = int(value)
    query = {param:{ "$lte": value }}
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

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#绘制30日蜡烛图函数
def draw_candlestick_nday(stockcode,nday):
    START_DATE = int(get_day_time(nday))
    END_DATE = int(get_day_time(0))
    '''
    ST_CODE = stockcode
    mycollection=mydb['dailytest']
    query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}},{'ts_code':ST_CODE}]} 
    cursor_stock = mycollection.find(query)
    # Expand the cursor and construct the DataFrame
    df_stock = pd.DataFrame(list(cursor_stock))
    '''
    df_stock = pro.daily(ts_code=stockcode, start_date=START_DATE, end_date=END_DATE)
    df_stock['trade_date']=df_stock['trade_date'].apply(str)
    df_stock['trade_date']=df_stock['trade_date'].apply(parse)
    inc = df_stock.close > df_stock.open
    dec = df_stock.open > df_stock.close
    w = 12*60*60*1000
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.segment(df_stock.trade_date,df_stock.high,df_stock.trade_date,df_stock.low,color="black")
    p.vbar(df_stock.trade_date[inc],w,df_stock.open[inc],df_stock.close[inc],fill_color="red",line_color="red")
    p.vbar(df_stock.trade_date[dec],w,df_stock.open[dec],df_stock.close[dec],fill_color="green",line_color="green")
    return p

#绘制饼图函数
def draw_piechart(collection,colum,count,asc):
    mycollection=mydb[collection]
    rs = mycollection.find().limit(count).sort([(colum,asc)])
    #data_lastday = get_data_sort('moneyflow_lastday','net_mf_amount',-1,100)
    #按照industry分类
    #将查询结果转换为Df
    data = pd.DataFrame(list(rs))
    grouped = data.groupby('industry')
    groupcount = grouped.size()
    pie_data =  pd.DataFrame()
    pie_data['value'] = groupcount
    #data拼装 industry angle color 
    pie_data['angle'] = pie_data['value']/count * 2*pi
    pie_data['color'] = Category20c[len(pie_data)]
    p = figure(plot_height=400, plot_width=400,toolbar_location=None,
           tools="hover", tooltips="@industry: @value")
    p.wedge(x=0, y=1, radius=0.4,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend='industry', source=pie_data)
    p.axis.axis_label=None
    p.axis.visible=False
    p.grid.grid_line_color = None # 网格线颜色
    return p
'''

'''
#绘制柱状图函数
def draw_vbar(collection,colum,count,asc):
    mycollection=mydb[collection]
    rs = mycollection.find().limit(count).sort([(colum,asc)])
    #data_lastday = get_data_sort('moneyflow_lastday','net_mf_amount',-1,100)
    #按照industry分类
    #将查询结果转换为Df
    
    data = pd.DataFrame(list(rs))
    grouped = data.groupby('industry')
    groupcount = grouped.size()
    pie_data =  pd.DataFrame()
    pie_data['value'] = groupcount
    data_x = list(groupcount.index)
    data_top = list(groupcount)
    source = ColumnDataSource(data=dict(data_x=data_x, data_top=data_top))
    p = figure(x_range=data_x, y_range=(0,9), plot_height=400,plot_width=1800, title="涨停强度排名前50涨停个股行业分布")
    #plot_height=350高度
    p.vbar(x='data_x', top='data_top', source=source, width=0.5, alpha = 1.0,color = 'red')
    p.grid.grid_line_color = None # 网格线颜色
    return p



# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')


#获取最新limit数据列表
@router.get('/stocks/limit/last/')
async def get_limit_lastday(request:Request):  # async加了就支持异步  把Request赋值给request
    #data_lastday = getindex('limit_lastday')
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    plot1 = draw_vbar('limit_last','strth',50,-1)
    script1,div1 = components(plot1)
    data_lastday = get_data_sort('limit_last','strth',-1,9999)
    return tmp.TemplateResponse('limit_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_lastday),  # 额外的参数可有可无
                                 'stocks':data_lastday,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "js_res":js_res,
                                 "css_res":css_res                                     
                                 })
    
#获取最新limit数据列表
@router.get('/stocks/limit/last/{colum}/{count}')
async def get_limit_lastday_top(request:Request,colum:str,count:int):  # async加了就支持异步  把Request赋值给request
    #data_lastday = getindex('limit_lastday')
    data_lastday = get_data_sort('limit_last',colum,-1,count)
    return tmp.TemplateResponse('limit_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_lastday),  # 额外的参数可有可无
                                 'stocks':data_lastday                                 
                                 })
    

    
#打开次数条件 获取最新limit数据列表
@router.post('/stocks/limit/last/open_timeslesseq/')
async def get_limit_lastday_opentimeslesseq(request:Request,open_times:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_mvless = get_data_less('limit_last','open_times',open_times)
    return tmp.TemplateResponse('limit_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_mvless),  # 额外的参数可有可无
                                 'stocks':data_mvless                                
                                 })
    
#封单金额条件 获取最新limit数据列表
@router.post('/stocks/limit/last/fd_amountgreateq/')
async def get_limit_lastday_fd_amountgreateq(request:Request,fd_amount:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_fdamount = get_data_greateq('limit_last','fd_amount',fd_amount)
    return tmp.TemplateResponse('limit_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_fdamount),  # 额外的参数可有可无
                                 'stocks':data_fdamount                                
                                 })
    
#获取最新30日内所有涨停股票
@router.get('/stocks/limit/{n}/')
async def get_limit_n(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    data_n = getindex('limit_'+str(n))
    return tmp.TemplateResponse('limit_stocks.html',
                                {'request':request,
                                 'days':n,# 一定要返回request
                                 'stockscount':len(data_n),  # 额外的参数可有可无
                                 'stocks':data_n                                
                                 })

#获取最新30日内所有涨停股票
@router.get('/stocks/limit/{n}/{stockcode}')
async def get_limit_stockcode(request:Request,stockcode:str,n:int):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_candlestick_nday(stockcode,int(n+10)) # 额外的参数可有可无
    script,div = components(p)
    data_stockcode = get_data_stockcode('limit_'+str(n),stockcode)
    return tmp.TemplateResponse('limit_stocks.html',
                                {'request':request,
                                 'days':n,
                                 'stockscount':len(data_stockcode),  # 额外的参数可有可无
                                 'stocks':data_stockcode,
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

#打开次数条件 获取最新limit数据列表
@router.post('/stocks/limit/{n}/open_timeslesseq/')
async def get_limit_n_opentimeslesseq(n:int,request:Request,open_times:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_mvless = get_data_less('limit_'+str(n),'open_times',open_times)
    return tmp.TemplateResponse('limit_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_mvless),  # 额外的参数可有可无
                                 'stocks':data_mvless                                
                                 })
    
#封单金额条件 获取最新limit数据列表
@router.post('/stocks/limit/{n}/fd_amountgreateq/')
async def get_limit_n_fd_amountgreateq(n:int,request:Request,fd_amount:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_fdamount = get_data_greateq('limit_'+str(n),'fd_amount',fd_amount)
    return tmp.TemplateResponse('limit_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_fdamount),  # 额外的参数可有可无
                                 'stocks':data_fdamount                                
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
    