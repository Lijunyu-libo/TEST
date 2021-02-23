# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
最新交易日资金流向分析
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
def getindex(col):
    mycollection=mydb[col]
    index_rs = mycollection.find()
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json

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
def get_data_greateq(index,param,value,asc):
    col=mydb[index]
    value = float(value)
    query = {param:{ "$gte": value }}
    rs = col.find(query).sort([(param,asc)])    
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
    ST_CODE = stockcode
    mycollection=mydb['dailytest']
    query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}},{'ts_code':ST_CODE}]} 
    cursor_stock = mycollection.find(query)
    # Expand the cursor and construct the DataFrame
    df_stock = pd.DataFrame(list(cursor_stock))
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

#近30日资金净流入（手）走势（内外盘）图
def draw_line_param_avgline(indexcode,param,colorname,avgparam,n,avgcolorname):
    df_index = get_data_df(indexcode)
    data_array = np.array(df_index[param])
    avgline = round(np.array(df_index[avgparam][0:n]).mean(),2)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color=colorname,legend=param)
    p.line(datetime_array,avgline,color=avgcolorname,legend=avgparam+str(n)+'='+str(avgline))
    p.legend.location = "top_left"
    return p

#近30日特大单流入流出（手）合计走势图
def draw_line_param1_param2_avgline(indexcode,param1,param2,colorname,n,avgcolorname):
    df_index = get_data_df(indexcode)
    df_result = df_index[param1]-df_index[param2]
    data_array = np.array(df_result)
    avgline = round(np.array(df_result[0:n]).mean(),2)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color=colorname,legend=param1+'-'+param2)
    p.line(datetime_array,avgline,color=avgcolorname,legend=param1+'-'+param2+str(n)+'='+str(avgline))
    p.legend.location = "top_left"
    return p    

def cal_moneyflow_pct(indexcode):
    df_index = get_data_df('moneyflow_'+indexcode)
    elg_vol = (df_index['buy_elg_vol']-df_index['sell_elg_vol']).sum()
    lg_vol = (df_index['buy_lg_vol']-df_index['sell_lg_vol']).sum()
    md_vol = (df_index['buy_md_vol']-df_index['sell_md_vol']).sum()
    sm_vol = (df_index['buy_sm_vol']-df_index['sell_sm_vol']).sum()
    #获取流通股本数（手）
    dailybasic_last_stockcode = get_data_stockcode('dailybasic_last',indexcode)
    total_vol = dailybasic_last_stockcode[0]['float_share']*100
    print (total_vol)
    #total_vol = elg_vol+lg_vol+md_vol+sm_vol
    elg_vol_pct = format(elg_vol/total_vol,'.3f')
    lg_vol_pct = format(lg_vol/total_vol,'.3f')
    md_vol_pct = format(md_vol/total_vol,'.3f')
    sm_vol_pct = format(sm_vol/total_vol,'.3f')
    data_array = np.array([elg_vol_pct,lg_vol_pct,md_vol_pct,sm_vol_pct])
    return data_array

#print (cal_moneyflow_pct('000002.SZ'))
#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#获取最新个股moneyflow数据列表
@router.get('/stock/moneyflow/{stockcode}/')
async def get_stock_moneyflow_(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_data = getindex('moneyflow_'+stockcode)
    stock_moneyflow_pct = cal_moneyflow_pct(stockcode)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1 = draw_line_param_avgline('moneyflow_'+stockcode,'net_mf_vol','blue','net_mf_vol',30,'black')
    script1,div1 = components(p1)
    #近30日特大单流入流出（手）合计走势图
    p2 = draw_line_param1_param2_avgline('moneyflow_'+stockcode,'buy_elg_vol','sell_elg_vol','red',30,'black')
    script2,div2 = components(p2)
    #近30日大单流入流出（手）合计走势图
    p3 = draw_line_param1_param2_avgline('moneyflow_'+stockcode,'buy_lg_vol','sell_lg_vol','orange',30,'black')
    script3,div3 = components(p3)
    #近30日中单流入流出（手）合计走势图
    p4 = draw_line_param1_param2_avgline('moneyflow_'+stockcode,'buy_md_vol','sell_md_vol','blue',30,'black')
    script4,div4 = components(p4)
    #近30日小单流入流出（手）合计走势图
    p5 = draw_line_param1_param2_avgline('moneyflow_'+stockcode,'buy_sm_vol','sell_sm_vol','green',30,'black')
    script5,div5 = components(p5)
    return tmp.TemplateResponse('stock_moneyflow.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockcount':len(stock_data),  # 额外的参数可有可无
                                 'stock':stock_data,
                                 'stock_moneyflow_pct':stock_moneyflow_pct,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "p_script5":script5,
                                 "p_div5":div5,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
#获取最新moneyflow数据列表
@router.get('/stocks/moneyflow/last/')
async def get_moneyflow_lastday(request:Request):  # async加了就支持异步  把Request赋值给request
    #data_lastday = getindex('moneyflow_lastday')
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    plot1 = draw_piechart('moneyflow_lastday','net_mf_amount',20,-1)
    script1,div1 = components(plot1)
    plot2 = draw_piechart('moneyflow_lastday','net_mf_amount',20,1)
    script2,div2 = components(plot2)
    plot3 = draw_piechart('moneyflow_lastday','buy_lg_amount',20,-1)
    script3,div3 = components(plot3)
    plot4 = draw_piechart('moneyflow_lastday','buy_elg_amount',20,-1)
    script4,div4 = components(plot4)
    data_lastday = get_data_sort('moneyflow_lastday','net_mf_amount',-1,9999)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_lastday),  # 额外的参数可有可无
                                 'stocks':data_lastday,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res                                 
                                 })
    
#获取最新moneyflow数据列表top
@router.get('/stocks/moneyflow/lastday/{colum}/{count}')
async def get_moneyflow_lastday_top(request:Request,colum:str,count:int):  # async加了就支持异步  把Request赋值给request
    #data_lastday = getindex('moneyflow_lastday')
    data_lastday = get_data_sort('moneyflow_lastday',colum,-1,count)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_lastday),  # 额外的参数可有可无
                                 'stocks':data_lastday                                 
                                 })
    
#stockcode条件 获取最新moneyflow数据列表
@router.post('/stocks/moneyflow/lastday/stockcode/')
async def get_moneyflow_lastday_stockcode(request:Request,stockcode:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data = get_data_stockcode('moneyflow_lastday',stockcode)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data),  # 额外的参数可有可无
                                 'stocks':data                                
                                 })
    
#净金额流入条件 获取最新moneyflow数据列表
@router.post('/stocks/moneyflow/lastday/net_mf_amountgreateq/')
async def get_moneyflow_lastday_net_mf_amountgreateq(request:Request,net_mf_amount:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_amount = get_data_greateq('moneyflow_lastday','net_mf_amount',net_mf_amount,-1)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_amount),  # 额外的参数可有可无
                                 'stocks':data_amount                                
                                 })
    
#大单金额流入条件 获取最新moneyflow数据列表
@router.post('/stocks/moneyflow/lastday/buy_lg_amountgreateq/')
async def get_moneyflow_lastday_buy_lg_amountgreateq(request:Request,buy_lg_amount:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_amount = get_data_greateq('moneyflow_lastday','buy_lg_amount',buy_lg_amount,-1)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_amount),  # 额外的参数可有可无
                                 'stocks':data_amount                                
                                 })
#超大单金额流入条件 获取最新moneyflow数据列表
@router.post('/stocks/moneyflow/lastday/buy_elg_amountgreateq/')
async def get_moneyflow_lastday_buy_elg_amountgreateq(request:Request,buy_elg_amount:str=Form(...)):  # async加了就支持异步  把Request赋值给request
    data_amount = get_data_greateq('moneyflow_lastday','buy_elg_amount',buy_elg_amount,-1)
    return tmp.TemplateResponse('moneyflow_stocks_lastday.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(data_amount),  # 额外的参数可有可无
                                 'stocks':data_amount                                
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
    