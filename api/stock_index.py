# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

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
from api import caltools
from api import stocks_moneyflow
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
#查询库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#获取库数据函数 参数
def get_data(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    if (len(df)>=30):
        df['MA5'] = caltools.get_ma(df,'MA5',5)
        df['MA10'] = caltools.get_ma(df,'MA10',10)
        df['MA20'] = caltools.get_ma(df,'MA20',20)
        df['MA30'] = caltools.get_ma(df,'MA30',30)
        df['RSI6'] = caltools.get_rsi(df,6)
        df['RSI12'] = caltools.get_rsi(df,12)
        df['RSI24'] = caltools.get_rsi(df,24)
        df['MTM'] = caltools.get_mom(df,6)
        #df_macd = caltools.get_MACDS(df)
        #df['MACD'] = df_macd['MACD']
        #df['DIF'] = df_macd['DIF']
        #df['DEA'] = df_macd['DEA']
        df_boll = caltools.get_boll(df)
        df['BOLL_UPPER'] = df_boll['upper']
        df['BOLL_MIDDLE'] = df_boll['middle']
        df['BOLL_LOWER'] = df_boll['lower']
    return df
#获取库数据函数 参数
def get_col_cal_index_df(col):
    col=mydb[col]
    rs = col.find()
    df = pd.DataFrame(list(rs))
    if (len(df)>=30):
        df['MA5'] = caltools.get_ma(df,'MA5',5)
        df['MA10'] = caltools.get_ma(df,'MA10',10)
        df['MA20'] = caltools.get_ma(df,'MA20',20)
        df['MA30'] = caltools.get_ma(df,'MA30',30)
        df['RSI6'] = caltools.get_rsi(df,6)
        df['RSI12'] = caltools.get_rsi(df,12)
        df['RSI24'] = caltools.get_rsi(df,24)
        df['MTM'] = caltools.get_mom(df,6)
        df_boll = caltools.get_boll(df)
        df['BOLL_UPPER'] = df_boll['upper']
        df['BOLL_MIDDLE'] = df_boll['middle']
        df['BOLL_LOWER'] = df_boll['lower']
    return df.round(3)

#获取库数据函数 参数
def get_col_cal_rsi_df(col):
    col=mydb[col]
    rs = col.find()
    df = pd.DataFrame(list(rs))[0:500]
    if (len(df)>=30):
        df['RSI6'] = caltools.get_rsi(df,6)
        df['RSI12'] = caltools.get_rsi(df,12)
        df['RSI24'] = caltools.get_rsi(df,24)
    return df.round(3)

#获取库数据函数 参数
def get_col_cal_mtm_df(col):
    col=mydb[col]
    rs = col.find()
    df = pd.DataFrame(list(rs))[0:500]
    if (len(df)>=12):
        df['MTM'] = caltools.get_mom(df,12)
        temp_serise = df['MTM'].rolling(6).mean()
        temp_serise.dropna(inplace=True)
        mamtm_serise = temp_serise.reset_index(drop=True)
        df['MAMTM'] = mamtm_serise
    return df.round(4)
#获取库数据函数 参数
def get_col_cal_ma_df(col):
    col=mydb[col]
    rs = col.find()
    df = pd.DataFrame(list(rs))
    if (len(df)>=60):
        df['MA5'] = caltools.get_ma(df,'MA5',5)
        df['MA10'] = caltools.get_ma(df,'MA10',10)
        df['MA20'] = caltools.get_ma(df,'MA20',20)
        df['MA30'] = caltools.get_ma(df,'MA30',30)
        df['MA60'] = caltools.get_ma(df,'MA60',60)
    return df.round(2)
#获取库数据函数 参数
def get_col_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df


#获取库数据函数 参数
def get_col_dict(col):
    col=mydb[col]
    rs = col.find()
    rs_dictlist = []
    for i in rs:
        rs_dictlist.append(i)
    return rs_dictlist
#获取库数据函数 参数
def get_col_500_dict(col):
    col=mydb[col]
    rs = col.find()[0:500]
    rs_dictlist = []
    for i in rs:
        rs_dictlist.append(i)
    return rs_dictlist
#获取库数据函数 参数 param 返回dict
def get_col_param_dict(col,param,paramvalue):
    col=mydb[col]
    query = {param:paramvalue}
    rs = col.find(query)
    rs_dictlist = []
    for i in rs:
        rs_dictlist.append(i)
    return rs_dictlist
#参数折线图函数
def draw_line(code,param):
    df_data = get_data(code)   
    data_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color='red',legend=code+param)
    p.legend.location = "top_left"
    return p


#指数MTM折线图函数
def draw_line_mtm(code):
    df_data = get_data(code)
    data_array = np.array(df_data['MTM'])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color='lime',legend='MTM')
    p.legend.location = "top_left"
    return p



#参数折线图函数 含自定义颜色
def draw_line_param(code,param,colorname):
    df_data = get_data(code)
    close_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,0,color='black',legend='0')
    p.legend.location = "top_left"
    return p
#参数折线图函数 含自定义颜色
def draw_line_df_param(df_data,param,colorname):
    close_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,0,color='black',legend='0')
    p.legend.location = "top_left"
    return p
#参数折线图函数 含自定义颜色
def draw_line_df_param_n(df_data,param,colorname,n):
    df_data = df_data[0:n]
    close_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.legend.location = "top_left"
    return p
#参数折线图函数 含自定义颜色
def draw_line_df_param_n_line(df_data,param,colorname,n):
    df_data = df_data[0:n]
    close_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,0.6,color='black',legend='0.6')
    p.legend.location = "top_left"
    return p
#参数折线图函数 含自定义颜色
def draw_line_df_param1_param2_n(df_data,param1,colorname1,param2,colorname2,n):
    df_data = df_data[0:n]
    param1_array = np.array(df_data[param1])
    param2_array = np.array(df_data[param2])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=800,x_axis_type="datetime")
    p.line(datetime_array,param1_array,color=colorname1,legend=param1)
    p.line(datetime_array,param2_array,color=colorname2,legend=param2)
    p.legend.location = "top_left"
    return p
#收盘指数折线图函数
def draw_line_param_avgline(indexcode,param,colorname,avgparam,n,avgcolorname):
    df_index = get_data(indexcode)
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
    df_index = get_data(indexcode)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    datetime_str_array = np.array(df_index['trade_date'])
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    ma5_array = np.array(df_index['MA5'])
    ma10_array = np.array(df_index['MA10'])
    ma20_array = np.array(df_index['MA20'])
    ma30_array = np.array(df_index['MA30'])
    #add boll
    upper_array = np.array(df_index['BOLL_UPPER'])
    middle_array = np.array(df_index['BOLL_MIDDLE'])
    lower_array = np.array(df_index['BOLL_LOWER'])
    inc = df_index.close > df_index.open
    dec = df_index.open > df_index.close
    w = 12*60*60*1000
    source = ColumnDataSource(data=dict(tradedateindexs=datetime_array,
                                    tradedates=datetime_str_array,
                                    highs= np.array(df_index['high']),
                                    closes= np.array(df_index['close']),
                                    lows= np.array(df_index['low']),
                                    pct_chgs= np.array(df_index['pct_chg'])))
    #拾取工具
    TOOLSTIPS = [('交易日','@tradedates'),('收盘价','@closes'),('最高价','@highs'),('最低价','@lows'),('涨跌幅','@pct_chgs')]

    p = figure(width=1800,height=400,x_axis_type="datetime",tooltips=TOOLSTIPS)
    #p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.segment('tradedateindexs','highs','tradedateindexs','lows',source=source,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    p.line(datetime_array,ma10_array,color='red',legend='MA10')
    p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    p.line(datetime_array,middle_array,color='blue',legend='BOLL_MIDDLE')
    p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
    p.legend.location = "top_left"
    '''
    #标注

    labels = LabelSet(
    x="tradedates",
    y="highs",
    text="closes",
    x_offset=5,
    y_offset=5,
    source=source,
    render_mode="css",
    )
    p.add_layout(labels)
    '''
    return p

#绘制TM蜡烛图函数
def draw_tradememo_candlestick(stockcode,trigger_trade_date,price_buy):
    df_index = get_col_cal_index_df(stockcode)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    datetime_str_array = np.array(df_index['trade_date'])
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    #tradememo_datetime_array = np.array(pd.date_range(start=trigger_trade_date,end=get_day_time(-1)))
    tradememo_datetime_array = np.array(pd.date_range(start=trigger_trade_date,periods=60))
    ma5_array = np.array(df_index['MA5'])
    ma10_array = np.array(df_index['MA10'])
    ma20_array = np.array(df_index['MA20'])
    ma30_array = np.array(df_index['MA30'])
    #add boll
    upper_array = np.array(df_index['BOLL_UPPER'])
    middle_array = np.array(df_index['BOLL_MIDDLE'])
    lower_array = np.array(df_index['BOLL_LOWER'])
    inc = df_index.close > df_index.open
    dec = df_index.open > df_index.close
    w = 12*60*60*1000
    source = ColumnDataSource(data=dict(tradedateindexs=datetime_array,
                                    tradedates=datetime_str_array,
                                    highs= np.array(df_index['high']),
                                    closes= np.array(df_index['close']),
                                    lows= np.array(df_index['low']),
                                    pct_chgs= np.array(df_index['pct_chg'])))
    #拾取工具
    TOOLSTIPS = [('交易日','@tradedates'),('收盘价','@closes'),('最高价','@highs'),('最低价','@lows'),('涨跌幅','@pct_chgs')]

    p = figure(width=1800,height=400,x_axis_type="datetime",tooltips=TOOLSTIPS)
    #p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.segment('tradedateindexs','highs','tradedateindexs','lows',source=source,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    p.line(datetime_array,ma10_array,color='green',legend='MA10')
    p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    p.line(datetime_array,middle_array,color='blue',legend='BOLL_MIDDLE')
    p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
    p.line(tradememo_datetime_array,price_buy,color='red',legend='price_buy='+str(price_buy))
    p.legend.location = "top_left"
    '''
    #标注

    labels = LabelSet(
    x="tradedates",
    y="highs",
    text="closes",
    x_offset=5,
    y_offset=5,
    source=source,
    render_mode="css",
    )
    p.add_layout(labels)
    '''
    return p


#成交量折线函数   
def draw_line_volume(indexcode):
    df_index = get_data(indexcode)
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

#参数折线图函数 自定义多参考线
def draw_line_amount_avgline(indexcode,param,colorname,n1,n1colorname,n2,n2colorname,n3,n3colorname,n4,n4colorname):
    df_index = get_data(indexcode)
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





#BOLL折线函数
def draw_line_boll(indexcode):
    df_index = get_data(indexcode)
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
    df_index = get_col_df(indexcode)
    pb_array = np.array(df_index['PB'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,pb_array,color='black',legend='PB')
    p.legend.location = "top_left"
    return p

#指数MACD折线图函数
def draw_line_macd(col):
    df_data = get_col_df(col)[0:500]
    macddif_array = np.array(df_data['DIF'])
    macddea_array = np.array(df_data['DEA'])
    macd_array = np.array(df_data['MACD'])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,macddif_array,color='red',legend='DIF')
    p.line(datetime_array,macddea_array,color='blue',legend='DEA')
    #p.line(datetime_array,macd_array,color='blue',legend='MACD')
    p.vbar(x=datetime_array, top=macd_array, color='green',width=0.9,legend='MACD')
    p.legend.location = "top_left"
    return p

#KDJ折线函数
def draw_line_kdj(col):
    df_index = get_col_df(col)[0:200]
    k_array = np.array(df_index['K'])
    d_array = np.array(df_index['D'])
    j_array = np.array(df_index['J'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,k_array,color='green',legend='K')
    p.line(datetime_array,d_array,color='red',legend='D')
    p.line(datetime_array,j_array,color='blue',legend='J')
    p.line(datetime_array,20,color='grey')
    p.line(datetime_array,50,color='grey')
    p.line(datetime_array,80,color='grey')
    p.line(datetime_array,100,color='grey')
    p.legend.location = "top_left"
    return p

#CCI折线函数
def draw_line_cci(col):
    df_index = get_col_df(col)[0:500]
    cci_array = np.array(df_index['CCI'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,cci_array,color='red',legend='CCI超买超卖指标')
    p.line(datetime_array,200,color='grey')
    p.line(datetime_array,0,color='grey')
    p.line(datetime_array,-200,color='grey')
    p.legend.location = "top_left"
    return p

#RSI折线函数
def draw_line_rsi(col):
    df_index = get_col_cal_rsi_df(col)[0:200]
    rsi6_array = np.array(df_index['RSI6'])
    rsi12_array = np.array(df_index['RSI12'])
    rsi24_array = np.array(df_index['RSI24'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,rsi6_array,color='red',legend='RSI6')
    p.line(datetime_array,rsi12_array,color='blue',legend='RSI12')
    p.line(datetime_array,rsi24_array,color='green',legend='RSI24')
    p.line(datetime_array,80,color='grey')
    p.line(datetime_array,50,color='grey')
    p.line(datetime_array,20,color='grey')
    p.legend.location = "top_left"
    return p

#绘制蜡烛图函数
def draw_boll_candlestick(col):
    df_index = get_col_cal_index_df(col)[0:500]
    df_index['trade_date']=df_index['trade_date'].apply(str)
    datetime_str_array = np.array(df_index['trade_date'])
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    #add boll
    upper_array = np.array(df_index['BOLL_UPPER'])
    middle_array = np.array(df_index['BOLL_MIDDLE'])
    lower_array = np.array(df_index['BOLL_LOWER'])
    inc = df_index.close > df_index.open
    dec = df_index.open > df_index.close
    w = 12*60*60*1000
    source = ColumnDataSource(data=dict(tradedateindexs=datetime_array,
                                    tradedates=datetime_str_array,
                                    highs= np.array(df_index['high']),
                                    closes= np.array(df_index['close']),
                                    lows= np.array(df_index['low']),
                                    pct_chgs= np.array(df_index['pct_chg'])))
    #拾取工具
    TOOLSTIPS = [('交易日','@tradedates'),('收盘价','@closes'),('最高价','@highs'),('最低价','@lows'),('涨跌幅','@pct_chgs')]
    p = figure(width=1800,height=400,x_axis_type="datetime",tooltips=TOOLSTIPS)
    #p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.segment('tradedateindexs','highs','tradedateindexs','lows',source=source,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    p.line(datetime_array,middle_array,color='blue',legend='BOLL_MIDDLE')
    p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
    p.legend.location = "top_left"
    return p

#绘制MA蜡烛图函数
def draw_ma_candlestick(col):
    df_index = get_col_cal_ma_df(col)[0:200]
    df_index['trade_date']=df_index['trade_date'].apply(str)
    datetime_str_array = np.array(df_index['trade_date'])
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    #MA
    ma5_array = np.array(df_index['MA5'])
    ma10_array = np.array(df_index['MA10'])
    ma20_array = np.array(df_index['MA20'])
    ma30_array = np.array(df_index['MA30'])
    ma60_array = np.array(df_index['MA60'])
    inc = df_index.close > df_index.open
    dec = df_index.open > df_index.close
    w = 12*60*60*1000
    source = ColumnDataSource(data=dict(tradedateindexs=datetime_array,
                                    tradedates=datetime_str_array,
                                    highs= np.array(df_index['high']),
                                    closes= np.array(df_index['close']),
                                    lows= np.array(df_index['low']),
                                    pct_chgs= np.array(df_index['pct_chg'])))
    #拾取工具
    TOOLSTIPS = [('交易日','@tradedates'),('收盘价','@closes'),('最高价','@highs'),('最低价','@lows'),('涨跌幅','@pct_chgs')]
    p = figure(width=1800,height=400,x_axis_type="datetime",tooltips=TOOLSTIPS)
    #p.segment(df_index.trade_date,df_index.high,df_index.trade_date,df_index.low,color="black")
    p.segment('tradedateindexs','highs','tradedateindexs','lows',source=source,color="black")
    p.vbar(df_index.trade_date[inc],w,df_index.open[inc],df_index.close[inc],fill_color="red",line_color="red")
    p.vbar(df_index.trade_date[dec],w,df_index.open[dec],df_index.close[dec],fill_color="green",line_color="green")
    p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    p.line(datetime_array,ma10_array,color='green',legend='MA10')
    p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    p.line(datetime_array,ma60_array,color='red',legend='MA60')
    p.legend.location = "top_left"
    return p

#MTM折线函数
def draw_line_mtm_matmt(col):
    df_index = get_col_cal_mtm_df(col)[0:200]
    mtm_array = np.array(df_index['MTM'])
    mamtm_array = np.array(df_index['MAMTM'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,mtm_array,color='red',legend='MTM')
    p.line(datetime_array,mamtm_array,color='green',legend='MAMTM')
    p.line(datetime_array,0,color='grey',legend='CENTER')
    p.legend.location = "top_left"
    return p

#OBV折线函数
def draw_line_obv_maobv(col):
    df_index = get_col_df(col)[0:200]
    obv_array = np.array(df_index['OBV'])
    maobv_array = np.array(df_index['MAOBV'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,obv_array,color='red',legend='OBV')
    p.line(datetime_array,maobv_array,color='green',legend='MAOBV')
    #p.line(datetime_array,0,color='grey',legend='CENTER')
    p.legend.location = "top_left"
    return p

#RSI折线函数
def draw_line_vrsi(col):
    df_index = get_col_df(col)[0:200]
    vrsi6_array = np.array(df_index['VRSI6'])
    vrsi12_array = np.array(df_index['VRSI12'])
    vrsi24_array = np.array(df_index['VRSI24'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,vrsi6_array,color='red',legend='VRSI6')
    p.line(datetime_array,vrsi12_array,color='blue',legend='VRSI12')
    p.line(datetime_array,vrsi24_array,color='green',legend='VRSI24')
    p.line(datetime_array,80,color='grey')
    p.line(datetime_array,50,color='grey')
    p.line(datetime_array,20,color='grey')
    p.legend.location = "top_left"
    return p
#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')
#MACD
@router.get('/stock/macd/{stockcode}')
async def get_stock_macd(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_500_dict('daily_qfq_macd_'+stockcode)
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)
    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_macd('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_macd.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
#KDJ
@router.get('/stock/kdj/{stockcode}')
async def get_stock_kdj(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_500_dict('daily_qfq_macd_'+stockcode)
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_kdj('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_kdj.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
#CCI
@router.get('/stock/cci/{stockcode}')
async def get_stock_cci(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_500_dict('daily_qfq_macd_'+stockcode)
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_cci('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_cci.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
#BOLL
@router.get('/stock/boll/{stockcode}')
async def get_stock_boll(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_cal_index_df('daily_qfq_macd_'+stockcode)[0:500].to_dict('records')
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_boll_candlestick('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_boll.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
#MA
@router.get('/stock/ma/{stockcode}')
async def get_stock_ma(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_cal_ma_df('daily_qfq_macd_'+stockcode)[0:200].to_dict('records')
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_ma_candlestick('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_ma.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
#RSI
@router.get('/stock/rsi/{stockcode}')
async def get_stock_rsi(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_cal_rsi_df('daily_qfq_macd_'+stockcode)[0:500].to_dict('records')
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_rsi('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_rsi.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

#MTM
@router.get('/stock/mtm/{stockcode}')
async def get_stock_mtm(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_cal_mtm_df('daily_qfq_macd_'+stockcode)[0:500].to_dict('records')
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_mtm_matmt('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_mtm.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
#OBV
@router.get('/stock/obv/{stockcode}')
async def get_stock_obv(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_500_dict('daily_qfq_macd_'+stockcode)
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_obv_maobv('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_obv.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
    
    
#OBV
@router.get('/stock/vrsi/{stockcode}')
async def get_stock_vrsi(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_col_500_dict('daily_qfq_macd_'+stockcode)
    stock_dailybasic = get_col_param_dict('stocks_dailybasic_lastday','ts_code',stockcode)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_line_vrsi('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    return tmp.TemplateResponse('stock_vrsi.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_dailybasic':stock_dailybasic[0],
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })    