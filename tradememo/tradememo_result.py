# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 06:45:19 2020
策略执行结果列表
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

import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

from starlette.requests import Request
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

# MONGODB CONNECT
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
DATE_TODAY_STR = get_day_time(0)
#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list
#获取历史交易日
def get_tradedate_count(df):
    q = np.array(get_lasttradedatelist(0,500))
    create_date=np.argwhere(q==df['create_date'])
    trade_date=np.argwhere(q==df['trade_date'])
    print (df['ts_code'],df['create_date'],df['trade_date'],create_date,trade_date)
    #df['tradedate_count']=trade_date-create_date
    #print (df['tradedate_count'])
    #return df['tradedate_count']
    
#获取库数据函数 返回df
def get_data_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df
#按条件获取库数据函数 参数1,值1 返回df
def get_data_df_findby(col,param1,value1):
    col=mydb[col]
    query = {param1:value1}
    rs = col.find(query)
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df
#按条件获取库数据函数 参数1,值1 ,参数2,值2返回df
def get_data_df_findbyboth(col,param1,value1,param2,value2):
    col=mydb[col]
    query = { "$and": [{param1:value1},{param2:value2}]}
    #{ "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}}]} 
    rs = col.find(query)
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df

#按条件获取库数据函数 参数1,值1 ,参数2,值2,参数3,值3返回df
def get_data_df_findbythree(col,param1,value1,param2,value2,param3,value3):
    col=mydb[col]
    query = { "$and": [{param1:value1},{param2:value2},{param3:value3}]}
    #{ "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}}]} 
    rs = col.find(query)
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df

#query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}},{'ts_code':ST_CODE}]} 
def get_data_df_findbytradedaterange(col,ST_CODE,START_DATE,END_DATE):
    col=mydb[col]
    query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}},{'ts_code':ST_CODE}]} 
    #{ "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}}]} 
    rs = col.find(query)
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df


#获取指数函数 返回JSON
def get_data_json(col):
    mycollection=mydb[col]
    rs = mycollection.find()
    rs_json = []
    for i in rs:
        rs_json.append(i)
    return rs_json


#参数折线图函数 含自定义颜色
def draw_line_param_zeroline(df_data,param,colorname):
    data_array = np.array(df_data[param])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array,color=colorname,legend=param)
    p.line(datetime_array,0,color='black',legend='0')
    p.legend.location = "top_left"
    source = ColumnDataSource(
    data=dict(
        tradedatearry=datetime_array,
        paramarry = df_data[param].tolist()))
    labels = LabelSet(
    x="tradedatearry",
    y="paramarry",
    text="paramarry",
    level="glyph",
    x_offset=5,
    y_offset=0,
    source = source
    #render_mode="canvas"
    )
    p.add_layout(labels)
    return p

#参数折线图函数 含自定义颜色
def draw_line_param3_zeroline(df_data,param1,colorname1,param2,colorname2,param3,colorname3):
    data_array1 = np.array(df_data[param1])
    data_array2 = np.array(df_data[param2])
    data_array3 = np.array(df_data[param3])
    df_data['trade_date']=df_data['trade_date'].apply(str)
    df_data['trade_date']=df_data['trade_date'].apply(parse)
    datetime_array = np.array(df_data['trade_date'], dtype=np.datetime64)
    p = figure(width=1800,height=400,x_axis_type="datetime")
    p.line(datetime_array,data_array1,color=colorname1,legend=param1)
    p.line(datetime_array,data_array2,color=colorname2,legend=param2)
    p.line(datetime_array,data_array3,color=colorname3,legend=param3)
    p.line(datetime_array,0,color='black',legend='0')
    p.legend.location = "top_left"
    source = ColumnDataSource(
        data=dict(
        tradedatearry=datetime_array,
        paramarry1 = df_data[param1].tolist(),
        paramarry2 = df_data[param2].tolist(),
        paramarry3 = df_data[param3].tolist()
        )
    )
    labels1 = LabelSet(
    x="tradedatearry",
    y="paramarry1",
    text="paramarry1",
    level="glyph",
    x_offset=5,
    y_offset=0,
    text_color = colorname1,
    source = source
    #render_mode="canvas"
    )
    p.add_layout(labels1)
    labels2 = LabelSet(
    x="tradedatearry",
    y="paramarry2",
    text="paramarry2",
    level="glyph",
    x_offset=5,
    y_offset=0,
    text_color = colorname2,
    source = source
    #render_mode="canvas"
    )
    p.add_layout(labels2)
    labels3 = LabelSet(
    x="tradedatearry",
    y="paramarry3",
    text="paramarry3",
    level="glyph",
    x_offset=5,
    y_offset=0,
    text_color = colorname3,
    source = source
    #render_mode="canvas"
    )
    p.add_layout(labels3)
    return p
#绘制TM蜡烛图函数
def draw_tradememo_candlestick(df_index,trigger_trade_date,price_buy):
    df_index['trade_date']=df_index['trade_date'].apply(str)
    datetime_str_array = np.array(df_index['trade_date'])
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    #tradememo_datetime_array = np.array(pd.date_range(start=trigger_trade_date,end=get_day_time(-1)))
    tradememo_datetime_array = np.array(pd.date_range(start=trigger_trade_date,periods=60))
    #ma5_array = np.array(df_index['MA5'])
    #ma10_array = np.array(df_index['MA10'])
    #ma20_array = np.array(df_index['MA20'])
    #ma30_array = np.array(df_index['MA30'])
    #add boll
    #upper_array = np.array(df_index['BOLL_UPPER'])
    #middle_array = np.array(df_index['BOLL_MIDDLE'])
    #lower_array = np.array(df_index['BOLL_LOWER'])
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
    #p.line(datetime_array,ma5_array,color='blue',legend='MA5')
    #p.line(datetime_array,ma10_array,color='green',legend='MA10')
    #p.line(datetime_array,ma20_array,color='lime',legend='MA20')
    #p.line(datetime_array,ma30_array,color='orange',legend='MA30')
    #p.line(datetime_array,upper_array,color='blue',legend='BOLL_UPPER')
    #p.line(datetime_array,middle_array,color='blue',legend='BOLL_MIDDLE')
    #p.line(datetime_array,lower_array,color='blue',legend='BOLL_LOWER')
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

@router.get('/tradememo/profit/all/')
async def get_tradememo_strategy_all(request:Request):  # async加了就支持异步  把Request赋值给request
    list_result=[]
    tradememo_df = get_data_df('tradememo')
    df_tradememo_reason = tradememo_df.groupby('reason')
    for name,group in df_tradememo_reason:
        #GET GROUP
        dict_result={}
        df = pd.DataFrame(group)
        stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
        df = pd.merge(df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
        df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
        #df['profit_best'] = (df['high']-df['trigger_close'])/df['trigger_close']
        df = df.round(4)
        profit_max = df['profit'].max()
        profit_min = df['profit'].min()
        #profit_best = df['profit_best'].max()
        profit_up_df = df[df['profit'] > 0]
        profit_down_df = df[df['profit'] < 0]
        data = df[df['profit'] != 0]
        profit_serise = data['profit'].dropna(axis=0,how='any')
        profit_avg = round(profit_serise.mean(),4)
        dict_result['reason'] = name
        dict_result['profit_up_count'] = len(profit_up_df)
        dict_result['profit_down_count'] = len(profit_down_df)
        dict_result['profit_avg'] = profit_avg
        dict_result['profit_max'] = profit_max
        dict_result['profit_min'] = profit_min
        dict_result['trigger_trade_date'] = df['trigger_trade_date'][0]
        dict_result['trade_date'] = df['trade_date'][0]
        dict_result['create_date'] = df['create_date'][0]        
        list_result.append(dict_result)
    #dict_result= df.to_dict(orient='records')
    #dict_result= get_data_df('tradememo').to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_profit_all.html',
                                {'request':request,  # 一定要返回request
                                 'title':'统计所有策略平均收益率',                              
                                 'resultcount':len(list_result),  # 额外的参数可有可无
                                 'results':list_result                              
                                 })

@router.get('/tradememo/profit/all/card/')
async def get_tradememo_profit_all_card(request:Request):  # async加了就支持异步  把Request赋值给request
    list_result=[]
    tradememo_df = get_data_df('tradememo')
    df_tradememo_reason = tradememo_df.groupby('reason')
    for name,group in df_tradememo_reason:
        #GET GROUP
        dict_result={}
        df = pd.DataFrame(group)
        stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
        df = pd.merge(df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
        df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
        #df['profit_best'] = (df['high']-df['trigger_close'])/df['trigger_close']
        df = df.round(4)
        profit_max = df['profit'].max()
        profit_min = df['profit'].min()
        #profit_best = df['profit_best'].max()
        profit_up_df = df[df['profit'] > 0]
        profit_down_df = df[df['profit'] < 0]
        data = df[df['profit'] != 0]
        profit_serise = data['profit'].dropna(axis=0,how='any')
        profit_avg = round(profit_serise.mean(),4)
        dict_result['reason'] = name
        dict_result['profit_up_count'] = len(profit_up_df)
        dict_result['profit_down_count'] = len(profit_down_df)
        dict_result['profit_avg'] = profit_avg
        dict_result['profit_max'] = profit_max
        dict_result['profit_min'] = profit_min
        dict_result['trigger_trade_date'] = df['trigger_trade_date'][0]
        dict_result['trade_date'] = df['trade_date'][0]
        dict_result['create_date'] = df['create_date'][0]
        #BY CREATE_DATE
        reason_trigger_trade_date_list = []
        df_trigger_trade_date = df.groupby('trigger_trade_date')
        for trigger_trade_date,group in df_trigger_trade_date:
            profit_dict = {}
            df_bytrigger_trade_date = pd.DataFrame(group)
            profit_max = df_bytrigger_trade_date['profit'].max()
            profit_min = df_bytrigger_trade_date['profit'].min()
            #profit_best = df['profit_best'].max()
            profit_up_df = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] > 0]
            profit_down_df = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] < 0]
            data = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] != 0]
            profit_serise = data['profit'].dropna(axis=0,how='any')
            profit_avg = round(profit_serise.mean(),4)
            profit_dict['trigger_trade_date'] = trigger_trade_date
            profit_dict['profit_max'] = profit_max
            profit_dict['profit_avg'] = profit_avg
            profit_dict['profit_min'] = profit_min
            reason_trigger_trade_date_list.append(profit_dict)
        dict_result['reason_trigger_trade_date_list'] = reason_trigger_trade_date_list        
        list_result.append(dict_result)
    #dict_result= df.to_dict(orient='records')
    #dict_result= get_data_df('tradememo').to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_profit_all_card.html',
                                {'request':request,  # 一定要返回request
                                 'title':'策略自动模拟交易收益面板',                              
                                 'resultcount':len(list_result),  # 额外的参数可有可无
                                 'results':list_result                              
                                 })
    
@router.get('/tradememo/profit/all/card/chart/')
async def get_tradememo_profit_all_card_chart(request:Request):  # async加了就支持异步  把Request赋值给request
    list_result=[]
    tradememo_df = get_data_df('tradememo')
    df_tradememo_reason = tradememo_df.groupby('reason')
    for name,group in df_tradememo_reason:
        #GET GROUP
        dict_result={}
        df = pd.DataFrame(group)
        stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
        df = pd.merge(df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
        df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
        #df['profit_best'] = (df['high']-df['trigger_close'])/df['trigger_close']
        df = df.round(4)
        profit_max = df['profit'].max()
        profit_min = df['profit'].min()
        #profit_best = df['profit_best'].max()
        profit_up_df = df[df['profit'] > 0]
        profit_down_df = df[df['profit'] < 0]
        data = df[df['profit'] != 0]
        profit_serise = data['profit'].dropna(axis=0,how='any')
        profit_avg = round(profit_serise.mean(),4)
        dict_result['reason'] = name
        dict_result['profit_up_count'] = len(profit_up_df)
        dict_result['profit_down_count'] = len(profit_down_df)
        dict_result['profit_avg'] = profit_avg
        dict_result['profit_max'] = profit_max
        dict_result['profit_min'] = profit_min
        dict_result['trigger_trade_date'] = df['trigger_trade_date'][0]
        dict_result['trade_date'] = df['trade_date'][0]
        dict_result['create_date'] = df['create_date'][0]
        #BY CREATE_DATE
        reason_trigger_trade_date_list = []
        df_trigger_trade_date = df.groupby('trigger_trade_date')
        for trigger_trade_date,group in df_trigger_trade_date:
            profit_dict = {}
            df_bytrigger_trade_date = pd.DataFrame(group)
            profit_max = df_bytrigger_trade_date['profit'].max()
            profit_min = df_bytrigger_trade_date['profit'].min()
            #profit_best = df['profit_best'].max()
            profit_up_df = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] > 0]
            profit_down_df = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] < 0]
            data = df_bytrigger_trade_date[df_bytrigger_trade_date['profit'] != 0]
            profit_serise = data['profit'].dropna(axis=0,how='any')
            profit_avg = round(profit_serise.mean(),4)
            profit_dict['trigger_trade_date'] = trigger_trade_date
            profit_dict['profit_max'] = profit_max
            profit_dict['profit_avg'] = profit_avg
            profit_dict['profit_min'] = profit_min
            #计算触发交易日到当前交易日列表
            after_tradetdate_df=pd.date_range(trigger_trade_date,DATE_TODAY_STR,freq='D')
            after_tradetdate_list = [datetime.datetime.strftime(x,'%F') for x in after_tradetdate_df]
            print (after_tradetdate_list)
            #for i i nafter_tradetdate_list:               
            reason_trigger_trade_date_list.append(profit_dict)
        dict_result['reason_trigger_trade_date_list'] = reason_trigger_trade_date_list        
        list_result.append(dict_result)
    #dict_result= df.to_dict(orient='records')
    #dict_result= get_data_df('tradememo').to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_profit_all_card.html',
                                {'request':request,  # 一定要返回request
                                 'title':'策略自动模拟交易收益面板',                              
                                 'resultcount':len(list_result),  # 额外的参数可有可无
                                 'results':list_result                              
                                 })
    
@router.get('/tradememo/result/all/')
async def get_tradememo_result_all(request:Request):  # async加了就支持异步  把Request赋值给request
    tradememo_df = get_data_df('tradememo')
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
    df = df.round(4)
    profit_up_df = df[df['profit'] > 0]
    profit_down_df = df[df['profit'] < 0]
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    #df['tradedate_count'] = df.apply(get_tradedate_count,axis = 1)
    df['tradedate_count'] = pd.DataFrame(pd.to_datetime(df['trade_date'],format='%Y%m%d') - pd.to_datetime(df['create_date'],format='%Y%m%d'))
    dict_result= df.to_dict(orient='records')
    #dict_result= get_data_df('tradememo').to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':'返回所有交易单',
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'profit_avg':profit_avg,                                 
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })
@router.get('/tradememo/result/strategy/{strategyname}')
async def get_tradememo_result_strategyname(strategyname:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','reason',strategyname).to_dict(orient='records')
    tradememo_df = get_data_df_findby('tradememo','reason',strategyname)
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
    df = df.round(4)
    profit_up_df = df[df['profit'] > 0]
    profit_down_df = df[df['profit'] < 0]
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    dict_result= df.to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':strategyname+'策略交易单',
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'profit_avg':profit_avg,
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })
@router.get('/tradememo/result/trigger_trade_date/{trigger_trade_date}')
async def get_tradememo_result_tradedate(trigger_trade_date:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','trade_date',tradedate).to_dict(orient='records')
    tradememo_df = get_data_df_findby('tradememo','trigger_trade_date',trigger_trade_date)
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['trigger_close'])/df['trigger_close']
    df = df.round(4)
    profit_up_df = df[df['profit'] > 0]
    profit_down_df = df[df['profit'] < 0]
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    dict_result= df.to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':trigger_trade_date+'日创建交易单',
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'profit_avg':profit_avg,                                 
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })
@router.get('/tradememo/result/reason_trigger_trade_date/{reason}/{trigger_trade_date}')
async def get_tradememo_result_reason_trigger_trade_date(reason:str,trigger_trade_date:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','trade_date',tradedate).to_dict(orient='records')
    tradememo_df = get_data_df_findbyboth('tradememo','reason',reason,'trigger_trade_date',trigger_trade_date)
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['trigger_close'])/df['trigger_close']
    df = df.round(4)
    profit_up_df = df[df['profit'] > 0]
    profit_down_df = df[df['profit'] < 0]
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    dict_result= df.to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':trigger_trade_date+'日创建交易单',
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'profit_avg':profit_avg,                                 
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })
@router.get('/tradememo/result/createdate/{createdate}')
async def get_tradememo_result_createdate(createdate:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','create_date',createdate).to_dict(orient='records')
    tradememo_df = get_data_df_findby('tradememo','create_date',createdate)
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['trigger_close'])/df['trigger_close']
    df = df.round(4)
    profit_up_df = df[df['profit'] > 0]
    profit_down_df = df[df['profit'] < 0]
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    dict_result= df.to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':createdate+'日创建交易单',
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'profit_avg':profit_avg,                                  
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })
@router.get('/tradememo/result/stock/{stockcode}')
async def get_tradememo_result_stockcode(stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','ts_code',stockcode).to_dict(orient='records')
    tradememo_df = get_data_df_findby('tradememo','ts_code',stockcode)
    stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    df['profit'] = (df['close']-df['trigger_close'])/df['trigger_close']
    df = df.round(4)
    dict_result= df.to_dict(orient='records')
    return tmp.TemplateResponse('tradememo_result.html',
                                {'request':request,  # 一定要返回request
                                 'title':stockcode+'交易单',
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result                              
                                 })

@router.get('/tradememo/result/reason_trigger_trade_date_stockcode/{reason}/{trigger_trade_date}/{stockcode}')
async def get_tradememo_result_reason_trigger_trade_date_stockcode(reason:str,trigger_trade_date:str,stockcode:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','trade_date',tradedate).to_dict(orient='records')
    tradememo_df = get_data_df_findbythree('tradememo','reason',reason,'trigger_trade_date',trigger_trade_date,'ts_code',stockcode)
    price_buy = tradememo_df['price_buy'][0]
    stock_dailybasic_lastday_df = get_data_df_findby('stocks_dailybasic_lastday','ts_code',stockcode)
    today_close = stock_dailybasic_lastday_df['close'][0]
    #最新收益率
    today_profit = round((today_close-price_buy)/price_buy,3)
    #获取个股从触发交易日到当前交易日的日线行情数据
    df = get_data_df_findbytradedaterange('daily_qfq_macd_'+stockcode,stockcode,trigger_trade_date,DATE_TODAY_STR)
    df['profit'] = (df['close']-price_buy)/price_buy
    df['profit_high'] = (df['high']-price_buy)/price_buy
    df['profit_low'] = (df['low']-price_buy)/price_buy
    df = df.round(3)
    #涨天数
    profit_up_df = df[df['profit'] > 0]
    #跌天数
    profit_down_df = df[df['profit'] < 0]
    #平均收益率
    data = df[df['profit'] != 0]
    profit_serise = data['profit'].dropna(axis=0,how='any')
    profit_avg = profit_serise.mean()
    dict_result= df.to_dict(orient='records')
    #股价蜡烛图
    p1 = draw_tradememo_candlestick(df,trigger_trade_date,price_buy)
    script1,div1 = components(p1)
    #收益率走势折线图
    p2 = draw_line_param_zeroline(df,'profit','red')
    script2,div2 = components(p2)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    return tmp.TemplateResponse('tradememo_stock_profit.html',
                                {'request':request,  # 一定要返回request
                                 'reason':reason,
                                 'stockcode':stockcode,
                                 'trigger_trade_date':trigger_trade_date,
                                 'price_buy':price_buy,
                                 'profit_up_count':len(profit_up_df),
                                 'profit_down_count':len(profit_down_df),
                                 'today_profit':today_profit,
                                 'profit_avg':profit_avg,                                 
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result,
                                 "p_script1":script1,
                                 "p_div1":div1,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "js_res":js_res,
                                 "css_res":css_res                                
                                 })

@router.get('/tradememo/profit/reason_trigger_trade_date/{reason}/{trigger_trade_date}')
async def get_tradememo_profit_reason_trigger_trade_date(reason:str,trigger_trade_date:str,request:Request):  # async加了就支持异步  把Request赋值给request
    #dict_result= get_data_df_findby('tradememo','trade_date',tradedate).to_dict(orient='records')
    tradememo_df = get_data_df_findbyboth('tradememo','reason',reason,'trigger_trade_date',trigger_trade_date)
    #stocks_dailybasic_lastday_df = get_data_df('stocks_dailybasic_lastday')
    #df = pd.merge(tradememo_df, stocks_dailybasic_lastday_df, how='left', on='ts_code')
    stockcode_list = tradememo_df['ts_code'].tolist()
    df_trade_date = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=trigger_trade_date, end_date=get_day_time(0))
    lasttradedate_list = df_trade_date['cal_date'].tolist()
    result_df = pd.DataFrame()
    for trade_date in lasttradedate_list:
        result_dict = {}
        df_tradedate_stocks = pd.DataFrame()
        for stockcode in stockcode_list:
            df_stock_tradedate = get_data_df_findby('daily_qfq_macd_'+stockcode,'trade_date',trade_date)
            if (df_stock_tradedate.empty):
                continue
            else:
                df_tradedate_stocks = df_tradedate_stocks.append(df_stock_tradedate, ignore_index=True)
        if (df_tradedate_stocks.empty):
            continue
        else:
            df = pd.merge(tradememo_df, df_tradedate_stocks, how='left', on='ts_code')
            #df = pd.DataFrame()
            #df = pd.merge(tradememo_df,df_tradedate_stocks,how='inner')
            #print ('3',df['close'].tolist())
            df['profit'] = (df['close']-df['price_buy'])/df['price_buy']
            df['profit_high'] = (df['high']-df['price_buy'])/df['price_buy']
            df['profit_low'] = (df['low']-df['price_buy'])/df['price_buy']
            #df = df.round(4)
            data = df[df['profit'] != 0]
            profit_serise = data['profit'].dropna(axis=0,how='any')
            profit_avg = round(profit_serise.mean(),3)
            profit_high_avg = round(df['profit_high'].mean(),3)
            profit_low_avg = round(df['profit_low'].mean(),3)
            result_dict['reason'] = reason
            result_dict['trigger_trade_date'] = trigger_trade_date
            result_dict['trade_date'] = trade_date
            result_dict['profit_avg'] = profit_avg
            result_dict['profit_high_avg'] = profit_high_avg
            result_dict['profit_low_avg'] = profit_low_avg
            result_df = result_df.append(result_dict,ignore_index=True)
    dict_result= result_df.to_dict(orient='records')
    #收益率走势折线图
    p2 = draw_line_param3_zeroline(result_df,'profit_avg','red','profit_high_avg','blue','profit_low_avg','green')
    script2,div2 = components(p2)
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    return tmp.TemplateResponse('tradememo_reason_tradedate_profit.html',
                                {'request':request,  # 一定要返回request
                                 'trigger_trade_date':trigger_trade_date,
                                 'reason':reason,                                
                                 'resultcount':len(dict_result),  # 额外的参数可有可无
                                 'results':dict_result,
                                 "p_script2":script2,
                                 "p_div2":div2,
                                 "js_res":js_res,
                                 "css_res":css_res   
                                 })
    