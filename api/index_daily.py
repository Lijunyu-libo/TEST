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
indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
indexnamedict = {'000001.SH':'上证综指','399001.SZ':'深圳成指','399005.SZ':'深中小板指','399006.SZ':'深创业板指'}

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col
#获取条件集合函数 参数 col 返回df
def get_col_param_df(col,param,paramvalue):
    mycollection=mydb[col]
    query = {param:paramvalue}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取指数数据函数 参数
def get_index_daily(index):
    col=mydb[index]
    rs_index = col.find()
    rs_dictlist = []
    for i in rs_index:
        rs_dictlist.append(i)
    return rs_dictlist

#获取指数数据函数 参数
def get_data_list(index):
    col=mydb[index]
    rs_index = col.find()
    rs_dictlist = []
    for i in rs_index:
        rs_dictlist.append(i)
    return rs_dictlist

#计算MA函数
def get_ma(df,maname,nday):
    df[maname] = df['close'].rolling(nday).mean()
    df[maname].dropna(inplace=True)
    ma_array = np.array(df[maname])
    print (df[maname])
    return ma_array
    
cursor_index = get_index_daily('000001.SH')
# Expand the cursor and construct the DataFrame
df_index = pd.DataFrame(list(cursor_index))
close_array = np.array(df_index['close'])
#df_index['trade_date'] = pd.to_datetime(df_index['trade_date'])
df_index['trade_date']=df_index['trade_date'].apply(str)
df_index['trade_date']=df_index['trade_date'].apply(parse)
#datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
#p = figure(width=800,height=400,x_axis_type="datetime")
datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
ma5_array = np.array(df_index['MA5'])
ma10_array = np.array(df_index['MA10'])
ma20_array = np.array(df_index['MA20'])
ma30_array = np.array(df_index['MA30'])
#get_ma(df_index,'MA10',10)
#get_ma(df_index,'MA20',20)
#get_ma(df_index,'MA30',30)
#get_ma(df_index,'MA60',60)
'''
p = figure(width=800,height=400,x_axis_type="datetime")
p.line(datetime_array,close_array,color='red',legend='CLOSE')
p.legend.location = "top_left"
show(p)
'''
#收盘指数折线图函数
def draw_line(indexcode,param):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    data_array = np.array(df_index[param])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color='red',legend=indexcode+param)
    p.legend.location = "top_left"
    return p

#收盘指数RSI折线图函数
def draw_line_rsi(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    rsi6_array = np.array(df_index['RSI6'])
    rsi12_array = np.array(df_index['RSI12'])
    rsi24_array = np.array(df_index['RSI24'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,rsi6_array,color='red',legend='RSI6')
    p.line(datetime_array,rsi12_array,color='green',legend='RSI12')
    p.line(datetime_array,rsi24_array,color='blue',legend='RSI24')
    p.legend.location = "top_left"
    return p

#指数MTM折线图函数
def draw_line_mtm(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    data_array = np.array(df_index['MTM'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color='lime',legend='MTM')
    p.legend.location = "top_left"
    return p

#指数MACD折线图函数
def draw_line_macd(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    macddif_array = np.array(df_index['DIF'])
    macddea_array = np.array(df_index['DEA'])
    macd_array = np.array(df_index['MACD'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,macddif_array,color='red',legend='DIF')
    p.line(datetime_array,macddea_array,color='green',legend='DEA')
    #p.line(datetime_array,macd_array,color='blue',legend='MACD')
    p.vbar(x=datetime_array, top=macd_array, color='blue',width=0.9)
    p.legend.location = "top_left"
    return p

#收盘指数折线图函数
def draw_line_param(indexcode,param,colorname):
    cursor_index = get_index_daily(indexcode)
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

#收盘指数折线图函数
def draw_line_param_avgline(indexcode,param,colorname,avgparam,n,avgcolorname):
    cursor_index = get_index_daily(indexcode)
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
#绘制蜡烛图函数
def draw_candlestick(indexcode):
    cursor_index = get_index_daily(indexcode)
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
    p.legend.location = "top_left"
    return p

#成交量折线函数   
def draw_line_volume(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    vol_array = np.array(df_index['vol'])
    amount_array = np.array(df_index['amount'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,vol_array,color='red',legend='成交量（手）')
    p.line(datetime_array,amount_array,color='blue',legend='成交额（千）')
    p.legend.location = "top_left"
    return p
#收盘指数折线图函数
def draw_line_amount_avgline(indexcode,param,colorname,n1,n1colorname,n2,n2colorname,n3,n3colorname,n4,n4colorname):
    cursor_index = get_index_daily(indexcode)
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
#折线图函数含4条参考线 窗体宽度参数
def draw_line_amount_4avgline_width(indexcode,width,param,colorname,n1,n1colorname,n2,n2colorname,n3,n3colorname,n4,n4colorname):
    cursor_index = get_index_daily(indexcode)
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
    p = figure(width=width,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,avgline1,color=n1colorname,legend=param+str(n1)+'='+str(avgline1))
    p.line(datetime_array,avgline2,color=n2colorname,legend=param+str(n2)+'='+str(avgline2))
    p.line(datetime_array,avgline3,color=n3colorname,legend=param+str(n3)+'='+str(avgline3))
    p.line(datetime_array,avgline4,color=n4colorname,legend=param+str(n4)+'='+str(avgline4))
    p.legend.location = "top_left"
    return p
#CCI折线函数
def draw_line_cci(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    cci_array = np.array(df_index['CCI'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,cci_array,color='red',legend='CCI超买超卖指标')
    p.legend.location = "top_left"
    return p

#KDJ折线函数
def draw_line_kdj(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    k_array = np.array(df_index['K'])
    d_array = np.array(df_index['D'])
    j_array = np.array(df_index['J'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,k_array,color='yellow',legend='K')
    p.line(datetime_array,d_array,color='green',legend='D')
    p.line(datetime_array,j_array,color='red',legend='J')
    p.legend.location = "top_left"
    return p

#BOLL折线函数
def draw_line_boll(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    upper_array = np.array(df_index['BOLL_UPPER'])
    middle_array = np.array(df_index['BOLL_MIDDLE'])
    lower_array = np.array(df_index['BOLL_LOWER'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    p.line(datetime_array,middle_array,color='yellow',legend='BOLL_MIDDLE')
    p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
    p.legend.location = "top_left"
    return p

#PB折线函数
def draw_line_pb(indexcode):
    cursor_index = get_index_daily(indexcode)
    # Expand the cursor and construct the DataFrame
    df_index = pd.DataFrame(list(cursor_index))
    pb_array = np.array(df_index['PB'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,pb_array,color='black',legend='PB')
    p.legend.location = "top_left"
    return p
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
#show(p)


#绘制蜡烛图函数
def draw_candlestick_ma_weektime_width(indexcode,width):
    cursor_index = get_data_list(indexcode)
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
    p = figure(width=width,height=400,x_axis_type="datetime")
    p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    p.line(datetime_array,ma10_array,color='red',legend='MA10')
    p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    p.legend.location = "top_left"
    return p

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

@router.get('/indexs/panel/')
async def get_indexs_panel(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1 = draw_candlestick_ma_weektime_width('000001.SH_monthly',900)
    script1,div1 = components(p1)
    p2 = draw_line_amount_4avgline_width('000001.SH_monthly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script2,div2 = components(p2)
    p3 = draw_candlestick_ma_weektime_width('000001.SH_weekly',900)
    script3,div3 = components(p3)
    p4 = draw_line_amount_4avgline_width('000001.SH_weekly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script4,div4 = components(p4)
    p5 = draw_candlestick_ma_weektime_width('000001.SH',1800)
    script5,div5 = components(p5)
    p6 = draw_line_amount_4avgline_width('000001.SH',1800,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script6,div6 = components(p6)
    df_daily1 = get_col_df('000001.SH')
    lasttradedate_daily1 = df_daily1['trade_date'][0]
    df_weekly1 = get_col_df('000001.SH_weekly')
    lasttradedate_weekly1 = df_weekly1['trade_date'][0]
    df_monthly1 = get_col_df('000001.SH_monthly')
    lasttradedate_monthly1 = df_monthly1['trade_date'][0]
    #399001
    p7 = draw_candlestick_ma_weektime_width('399001.SZ_monthly',900)
    script7,div7 = components(p7)
    p8 = draw_line_amount_4avgline_width('399001.SZ_monthly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script8,div8 = components(p8)
    p9 = draw_candlestick_ma_weektime_width('399001.SZ_weekly',900)
    script9,div9 = components(p9)
    p10 = draw_line_amount_4avgline_width('399001.SZ_weekly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script10,div10 = components(p10)
    p11 = draw_candlestick_ma_weektime_width('399001.SZ',1800)
    script11,div11 = components(p11)
    p12 = draw_line_amount_4avgline_width('399001.SZ',1800,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script12,div12 = components(p12)
    df_daily2 = get_col_df('399001.SZ')
    lasttradedate_daily2 = df_daily2['trade_date'][0]
    df_weekly2 = get_col_df('399001.SZ_weekly')
    lasttradedate_weekly2 = df_weekly2['trade_date'][0]
    df_monthly2 = get_col_df('399001.SZ_monthly')
    lasttradedate_monthly2 = df_monthly2['trade_date'][0]
    
    return tmp.TemplateResponse('index_datapanel.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode1':'000001.SH',
                                 'lasttradedate_daily1':lasttradedate_daily1,
                                 'lasttradedate_weekly1':lasttradedate_weekly1,
                                 'lasttradedate_monthly1':lasttradedate_monthly1,
                                 'indexname1':indexnamedict['000001.SH'],
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
                                 "p_script6":script6,
                                 "p_div6":div6,
                                 #399001
                                 'indexcode2':'399001.SZ',
                                 'lasttradedate_daily2':lasttradedate_daily2,
                                 'lasttradedate_weekly2':lasttradedate_weekly2,
                                 'lasttradedate_monthly2':lasttradedate_monthly2,
                                 'indexname2':indexnamedict['399001.SZ'],
                                 "p_script7":script7,
                                 "p_div7":div7,
                                 "p_script8":script8,
                                 "p_div8":div8,
                                 "p_script9":script9,
                                 "p_div9":div9,
                                 "p_script10":script10,
                                 "p_div10":div10,
                                 "p_script11":script11,
                                 "p_div11":div11,
                                 "p_script12":script12,
                                 "p_div12":div12,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/panel/test/')
async def get_indexs_panel_tset(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    indexdrawlist = []
    for i in indexlist:
        drawdict={}
        drawdict['indexcode'] = i
        drawdict['indexname'] = indexnamedict[i]
        '''
        p1 = draw_candlestick_ma_weektime_width(i+'_monthly',900)
        script1,div1 = components(p1)
        p2 = draw_line_amount_4avgline_width(i+'_monthly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
        script2,div2 = components(p2)
        p3 = draw_candlestick_ma_weektime_width(i+'_weekly',900)
        script3,div3 = components(p3)
        p4 = draw_line_amount_4avgline_width(i+'_weekly',900,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
        script4,div4 = components(p4)
        p5 = draw_candlestick_ma_weektime_width(i,1800)
        script5,div5 = components(p5)
        p6 = draw_line_amount_4avgline_width(i,1800,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
        script6,div6 = components(p6)
        drawdict['p1'] = p1
        drawdict['script1'] = script1
        drawdict['div1'] = div1
        
        drawdict['p2'] = p2
        drawdict['script2'] = script2
        drawdict['div2'] = div2
        
        drawdict['p3'] = p3
        drawdict['script3'] = script3
        drawdict['div3'] = div3
        
        drawdict['p4'] = p4
        drawdict['script4'] = script4
        drawdict['div4'] = div4
        
        drawdict['p5'] = p5
        drawdict['script5'] = script5
        drawdict['div5'] = div5  
        
        drawdict['p6'] = p6
        drawdict['script6'] = script6
        drawdict['div6'] = div6
        '''
        indexdrawlist.append(drawdict)
        
    return tmp.TemplateResponse('index_datapanel_one.html',
                                {'request':request,  # 一定要返回request
                                 'indexdrawlist':indexdrawlist,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
@router.get('/indexs/daily/')
async def get_indexs(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    script,div = components(p)
    #p2 = draw_line_param('000001.SH','close','red')
    #script2,div2 = components(p2)
    p2 =draw_line_param_avgline('000001.SH','close','red','close',10,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param('000001.SH','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline('000001.SH','pct_high_low','blue','pct_high_low',10,'black')
    script4,div4 = components(p4)
    #p2 = draw_line_rsi('000001.SH')
    #script2,div2 = components(p2)
    #p3 = draw_line_mtm('000001.SH')
    #script3,div3 = components(p3)
    #p4 = draw_line_macd('000001.SH')
    #script4,div4 = components(p4)
    #p5 = draw_line_cci('000001.SH')
    #script5,div5 = components(p5)
    #p6 = draw_line_kdj('000001.SH')
    #script6,div6 = components(p6)
    #p7 = draw_line_boll('000001.SH')
    #script7,div7 = components(p7)
    #p8 = draw_line_pb('000001.SH')
    #script8,div8 = components(p8)
    return tmp.TemplateResponse('index_daily.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH')),
                                 'indexdaily':get_index_daily('000001.SH'),
                                 "p_script":script,
                                 "p_div":div,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/weekly/')
async def get_indexs_weekly(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p2 = draw_line_param_avgline('000001.SH_weekly','close','red','close',3,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param('000001.SH_weekly','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline('000001.SH_weekly','pct_high_low','blue','pct_high_low',3,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_weekly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH_weekly')),
                                 'indexdaily':get_index_daily('000001.SH_weekly'),
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/monthly/')
async def get_indexs_monthly(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p2 = draw_line_param_avgline('000001.SH_monthly','close','red','close',3,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param('000001.SH_monthly','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline('000001.SH_monthly','pct_high_low','blue','pct_high_low',3,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_monthly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH_monthly')),
                                 'indexdaily':get_index_daily('000001.SH_monthly'),
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/daily/{index_id}')
async def get_index(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_candlestick(index_id)
    script,div = components(p)
    p2 = draw_line_param_avgline(index_id,'close','red','close',10,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param(index_id,'pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline(index_id,'pct_high_low','blue','pct_high_low',10,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_daily.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id)),
                                 'indexdaily':get_index_daily(index_id),
                                 "p_script":script,
                                 "p_div":div,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/weekly/{index_id}')
async def get_index_weekly_id(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p2 = draw_line_param_avgline(index_id+'_weekly','close','red','close',3,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param(index_id+'_weekly','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline(index_id+'_weekly','pct_high_low','blue','pct_high_low',3,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_weekly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id+'_weekly')),
                                 'indexdaily':get_index_daily(index_id+'_weekly'),
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/monthly/{index_id}')
async def get_index_monthly_id(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p2 = draw_line_param_avgline(index_id+'_monthly','close','red','close',3,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param(index_id+'_monthly','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param(index_id+'_monthly','pct_high_low','blue','pct_high_low',3,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_monthly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id+'_monthly')),
                                 'indexdaily':get_index_daily(index_id+'_monthly'),
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "p_script3":script3,
                                 "p_div3":div3,
                                 "p_script4":script4,
                                 "p_div4":div4,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/volume/')
async def get_indexs_volume(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    plot = draw_line_amount_avgline('000001.SH','amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_daily.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH')),
                                 'indexdaily':get_index_daily('000001.SH'),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/volume/weekly/')
async def get_indexs_volume_weekly(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #plot = draw_line_param_avgline('000001.SH_weekly','amount_y','red','amount_y',3,'black')
    plot = draw_line_amount_avgline('000001.SH_weekly','amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_weekly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH_weekly')),
                                 'indexdaily':get_index_daily('000001.SH_weekly'),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/volume/monthly/')
async def get_indexs_volume_monthly(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #plot = draw_line_param_avgline('000001.SH_monthly','amount_y','red','amount_y',3,'black')    
    plot = draw_line_amount_avgline('000001.SH_monthly','amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_monthly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':'000001.SH',
                                 'indexname':indexnamedict['000001.SH'],
                                 'count':len(get_index_daily('000001.SH_monthly')),
                                 'indexdaily':get_index_daily('000001.SH_monthly'),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })    

@router.get('/indexs/volume/{index_id}')
async def get_index_vol(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    plot = draw_line_volume(index_id)
    #plot = draw_line_param_avgline(index_id,'amount_y','red','amount_y',5,'black')
    plot = draw_line_amount_avgline(index_id,'amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_daily.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id)),
                                 'indexdaily':get_index_daily(index_id),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/volume/weekly/{index_id}')
async def get_index_vol_weekly_id(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #plot = draw_line_param_avgline(index_id+'_weekly','amount_y','red','amount_y',5,'black')
    plot = draw_line_amount_avgline(index_id+'_weekly','amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_weekly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id+'_weekly')),
                                 'indexdaily':get_index_daily(index_id+'_weekly'),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/indexs/volume/monthly/{index_id}')
async def get_index_vol_monthly_id(index_id:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #plot = draw_line_param_avgline(index_id+'_monthly','amount_y','red','amount_y',5,'black')
    plot = draw_line_amount_avgline(index_id+'_monthly','amount_y','red',3,'lime',5,'black',10,'blue',20,'green')
    script,div = components(plot)
    return tmp.TemplateResponse('index_volume_monthly.html',
                                {'request':request,  # 一定要返回request
                                 'indexlist':indexlist,
                                 'indexcode':index_id,
                                 'indexname':indexnamedict[index_id],
                                 'count':len(get_index_daily(index_id+'_monthly')),
                                 'indexdaily':get_index_daily(index_id+'_monthly'),
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })    
