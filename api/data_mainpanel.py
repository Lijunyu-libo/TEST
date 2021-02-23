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
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates

from api import caltools
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')

#查询库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
indexlist = ['000001.SH','399001.SZ','399005.SZ','399006.SZ']
indexnamedict = {'000001.SH':'上证综指','399001.SZ':'深圳成指','399005.SZ':'深中小板指','399006.SZ':'深创业板指'}

#获取库数据函数 参数
def get_data_dict(col):
    col=mydb[col]
    rs = col.find()
    rs_dictlist = []
    for i in rs:
        rs_dictlist.append(i)
    return rs_dictlist
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
        df_macd = caltools.get_macd_mas(df)
        df['MACD'] = df_macd['MACD']
        df['DIF'] = df_macd['DIF']
        df['DEA'] = df_macd['DEA']
        df_boll = caltools.get_boll(df)
        df['BOLL_UPPER'] = df_boll['upper']
        df['BOLL_MIDDLE'] = df_boll['middle']
        df['BOLL_LOWER'] = df_boll['lower']
    return df
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
                                    highs= np.array(df_index['high'].apply(str)),
                                    closes= np.array(df_index['close'].apply(str)),
                                    lows= np.array(df_index['low'].apply(str)),
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

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#主面板
@router.get('/data/mainpanel/')
async def get_mainpanel(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1 = draw_candlestick('000001.SH')
    script1,div1 = components(p1)
    index_dict_0 = get_data_dict('000001.SH')[0]
    index_dict_1 = get_data_dict('000001.SH')[1]
    index_df = get_data('000001.SH')
    index_close_avg_5= index_df['close'][0:4].mean()
    pct_chg_avg_5 = format((index_df['close'][0]-index_close_avg_5)/index_close_avg_5*100,'.2f')
    return tmp.TemplateResponse('data_mainpanel.html',
                                {'request':request,  # 一定要返回request
                                 'index_dict_0':index_dict_0,
                                 'index_dict_1':index_dict_1,
                                 'index_close_avg_5':index_close_avg_5,
                                 'pct_chg_avg_5':pct_chg_avg_5,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 })

