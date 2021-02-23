# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
获取最新申万行业指数数据
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
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
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
#折线图函数 含0轴
def draw_line_param(indexcode,param,colorname):
    cursor_index = get_data(indexcode+'_daily')
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    close_array = np.array(df_index[param])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,0,color='black',legend='0')
    p.legend.location = "top_left"
    return p

#折线图函数含自定义N平均线
def draw_line_param_avgline(indexcode,param,colorname,avgparam,n,avgcolorname):
    cursor_index = get_data(indexcode+'_daily')
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    close_array = np.array(df_index[param])
    avgline = round(np.array(df_index[avgparam][0:n]).mean(),2)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,avgline,color=avgcolorname,legend=avgparam+str(n)+'='+str(avgline))
    p.legend.location = "top_left"
    return p

#收盘指数折线图函数
def draw_line_amount_avgline(indexcode,param,colorname,n1,n1colorname,n2,n2colorname,n3,n3colorname,n4,n4colorname):
    cursor_index = get_data(indexcode+'_daily')
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    close_array = np.array(df_index[param])
    avgline1 = round(np.array(df_index[param][0:n1]).mean(),2)
    avgline2 = round(np.array(df_index[param][0:n2]).mean(),2)
    avgline3 = round(np.array(df_index[param][0:n3]).mean(),2)
    avgline4 = round(np.array(df_index[param][0:n4]).mean(),2)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,avgline1,color=n1colorname,legend=param+str(n1)+'='+str(avgline1))
    p.line(datetime_array,avgline2,color=n2colorname,legend=param+str(n2)+'='+str(avgline2))
    p.line(datetime_array,avgline3,color=n3colorname,legend=param+str(n3)+'='+str(avgline3))
    p.line(datetime_array,avgline4,color=n4colorname,legend=param+str(n4)+'='+str(avgline4))
    p.legend.location = "top_left"
    return p

#绘制蜡烛图函数
def draw_candlestick(indexcode):
    cursor_index = get_data(indexcode+'_daily')
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    ma5_array = np.array(df_index['MA5'])
    ma10_array = np.array(df_index['MA10'])
    ma20_array = np.array(df_index['MA20'])
    ma30_array = np.array(df_index['MA30'])
    inc = df_index.close > df_index.open
    dec = df_index.open > df_index.close
    w = 12*60*60*1000
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    p.line(datetime_array,ma10_array,color='red',legend='MA10')
    p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    #add boll
    upper_array = np.array(df_index['BOLL_UPPER'])
    middle_array = np.array(df_index['BOLL_MIDDLE'])
    lower_array = np.array(df_index['BOLL_LOWER'])
    p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    p.line(datetime_array,middle_array,color='blue',legend='BOLL_MIDDLE')
    p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
    p.legend.location = "top_left"
    return p
#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

@router.get('/indexs/sw/last/')
async def get_sw_last(request:Request):  # async加了就支持异步  把Request赋值给request
    indexs_sw_last = get_data('swindexs_daily_last')
    return tmp.TemplateResponse('sw_indexs_last.html',
                                {'request':request,  # 一定要返回request
                                 'stockscount':len(indexs_sw_last),  # 额外的参数可有可无
                                 'stocks':indexs_sw_last                                 
                                 })

@router.get('/indexs/sw/daily/{indexcode}')
async def get_sw_indexcode(indexcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    indexs_sw_daily = get_data(indexcode+'_daily')
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p =draw_line_amount_avgline(indexcode,'amount','blue',3,'red',5,'blue',10,'green',20,'black')
    script,div = components(p)
    p1 =draw_candlestick(indexcode)
    script1,div1 = components(p1)
    p2 =draw_line_param_avgline(indexcode,'close','red','close',10,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param(indexcode,'pct_change','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline(indexcode,'pct_high_low','blue','pct_high_low',10,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('sw_indexs_daily.html',
                                {'request':request,
                                 'indexcode':indexcode,# 一定要返回request
                                 'stockscount':len(indexs_sw_daily),  # 额外的参数可有可无
                                 'stocks':indexs_sw_daily,
                                 "p_script":script,
                                 "p_div":div,
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