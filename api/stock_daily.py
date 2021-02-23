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
        df_macd = caltools.get_MACDS(df)
        df['MACD'] = df_macd['MACD']
        #df['DIF'] = df_macd['DIF']
        #df['DEA'] = df_macd['DEA']
        df_boll = caltools.get_boll(df)
        df['BOLL_UPPER'] = df_boll['upper']
        df['BOLL_MIDDLE'] = df_boll['middle']
        df['BOLL_LOWER'] = df_boll['lower']
    return df
#获取库数据函数 参数
def get_data_df(col):
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
        df_boll = caltools.get_boll(df)
        df['BOLL_UPPER'] = df_boll['upper']
        df['BOLL_MIDDLE'] = df_boll['middle']
        df['BOLL_LOWER'] = df_boll['lower']
    return df
#获取库数据函数 参数
def get_col_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df
#获取库数据函数 参数
def get_data_dict(col):
    col=mydb[col]
    rs = col.find()
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

#收盘指数RSI折线图函数
def draw_line_rsi(code):
    df_data = get_data(code)
    rsi6_array = np.array(df_data['RSI6'])
    rsi12_array = np.array(df_data['RSI12'])
    rsi24_array = np.array(df_data['RSI24'])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,rsi6_array,color='red',legend='RSI6')
    p.line(datetime_array,rsi12_array,color='green',legend='RSI12')
    p.line(datetime_array,rsi24_array,color='blue',legend='RSI24')
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

#指数MACD折线图函数
def draw_line_macd(code):
    df_data = get_data(code)
    macddif_array = np.array(df_data['DIF'])
    macddea_array = np.array(df_data['DEA'])
    macd_array = np.array(df_data['MACD'])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,macddif_array,color='red',legend='DIF')
    p.line(datetime_array,macddea_array,color='green',legend='DEA')
    #p.line(datetime_array,macd_array,color='blue',legend='MACD')
    p.vbar(x=datetime_array, top=macd_array, color='blue',width=0.9)
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
    df_index = get_data_df(stockcode)
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

#CCI折线函数
def draw_line_cci(indexcode):
    df_index = get_data(indexcode)
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
    df_index = get_data(indexcode)
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
    df_index = get_data(indexcode)
    pb_array = np.array(df_index['PB'])
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,pb_array,color='black',legend='PB')
    p.legend.location = "top_left"
    return p

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

@router.get('/stocks/{stockcode}')
async def get_stocks_daily(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_data_dict('daily_qfq_macd_'+stockcode)
    stock_moneyflow_pct = stocks_moneyflow.cal_moneyflow_pct(stockcode)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_candlestick('daily_qfq_macd_'+stockcode)
    script,div = components(p)
    p2 = draw_line_param_avgline('daily_qfq_macd_'+stockcode,'close','red','close',10,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param('daily_qfq_macd_'+stockcode,'pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline('daily_qfq_macd_'+stockcode,'vol','blue','vol',10,'red')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('stock_daily.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_moneyflow_pct':stock_moneyflow_pct,
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

@router.get('/stocksdetail/{stockcode}')
async def get_stocks_detail(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_data_dict('daily_'+stockcode)
    stock_moneyflow_pct = stocks_moneyflow.cal_moneyflow_pct(stockcode)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_candlestick('daily_'+stockcode)
    script,div = components(p)
    p1 = draw_candlestick('weekly_'+stockcode)
    script1,div1 = components(p1)
    p2 = draw_line_param_avgline('daily_'+stockcode,'close','red','close',10,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param('daily_'+stockcode,'pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline('daily_'+stockcode,'vol','blue','vol',10,'red')
    script4,div4 = components(p4)    
    p5 = draw_line_macd('daily_'+stockcode)
    script5,div5 = components(p5)
    return tmp.TemplateResponse('stock_detail.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 'stock_moneyflow_pct':stock_moneyflow_pct,
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
                                 "p_script5":script5,
                                 "p_div5":div5,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/tradememo/stock/{stockcode}/{trigger_trade_date}/{price_buy}')
async def get_tradememo_stocks_daily(stockcode:str,price_buy:float,trigger_trade_date:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_dict = get_data_dict('daily_qfq_macd_'+stockcode)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p = draw_tradememo_candlestick('daily_qfq_macd_'+stockcode,trigger_trade_date,price_buy)
    script,div = components(p)
    return tmp.TemplateResponse('tradememo_stock_daily.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 "p_script":script,
                                 "p_div":div,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

@router.get('/stock/amountvol/{stockcode}/')
async def get_stock_daily_amnountvol(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    stock_daily_df = get_data_df('daily_qfq_macd_'+stockcode)
    stock_daily_moneyflow_df = get_col_df('daily_moneyflow_'+stockcode)
    stock_daily_moneyflow_df['buy_elg_lg_amount_ratio'] = round((stock_daily_moneyflow_df['buy_elg_amount']+stock_daily_moneyflow_df['buy_lg_amount'])/(stock_daily_moneyflow_df['buy_sm_amount']+stock_daily_moneyflow_df['buy_md_amount']+stock_daily_moneyflow_df['buy_lg_amount']+stock_daily_moneyflow_df['buy_elg_amount']),2)
    stock_daily_moneyflow_df['buy_lg_amount_ratio'] = round(stock_daily_moneyflow_df['buy_lg_amount']/(stock_daily_moneyflow_df['buy_sm_amount']+stock_daily_moneyflow_df['buy_md_amount']+stock_daily_moneyflow_df['buy_lg_amount']+stock_daily_moneyflow_df['buy_elg_amount']),2)    
    stock_daily_df['amount_vol'] = round(stock_daily_df['amount']/stock_daily_df['vol'],3)
    stock_daily_dict = stock_daily_df.to_dict('records')
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1 = draw_candlestick('daily_qfq_macd_'+stockcode)
    script1,div1 = components(p1)
    p2 = draw_line_df_param_n(stock_daily_df,'close','red',60)
    script2,div2 = components(p2)
    p0 = draw_line_df_param_n(stock_daily_df,'amount','black',60)
    script0,div0 = components(p0)    
    p3 = draw_line_df_param_n_line(stock_daily_moneyflow_df,'buy_elg_lg_amount_ratio','green',60)
    script3,div3 = components(p3)
    p4 = draw_line_df_param_n_line(stock_daily_moneyflow_df,'buy_lg_amount_ratio','blue',60)
    script4,div4 = components(p4)
    p5 = draw_line_df_param_n(stock_daily_moneyflow_df,'buy_lg_amount','red',60)
    script5,div5 = components(p5)
    p6 = draw_line_df_param_n(stock_daily_moneyflow_df,'buy_elg_amount','black',60)
    script6,div6 = components(p6) 
    #p4 = draw_line_df_param_n(stock_daily_df,'amount_vol','blue',60)
    #script4,div4 = components(p4)   
    return tmp.TemplateResponse('stock_daily_amount_vol.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
                                 'stockdailycount':len(stock_daily_dict),
                                 'stockdaily':stock_daily_dict,
                                 "p_script0":script0,
                                 "p_div0":div0,                                 
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
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })
'''
@router.get('/indexs/weekly/{stockcode}')
async def get_index_weekly_id(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p2 = draw_line_param_avgline(stockcode+'_weekly','close','red','close',3,'black')
    script2,div2 = components(p2)
    p3 = draw_line_param(stockcode+'_weekly','pct_chg','green')
    script3,div3 = components(p3)
    p4 = draw_line_param_avgline(stockcode+'_weekly','pct_high_low','blue','pct_high_low',3,'black')
    script4,div4 = components(p4)
    return tmp.TemplateResponse('index_weekly.html',
                                {'request':request,  # 一定要返回request
                                 'stockcode':stockcode,
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
'''
