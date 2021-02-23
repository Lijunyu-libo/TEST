# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 10:31:10 2020
策略执行基本模块
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

import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
from starlette.requests import Request
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
from datetime import datetime
# MONGODB CONNECT
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#import stockbasket
#import moneyflow
#import moneyflowtoptogether

from strategies import stockbasket
from strategies import moneyflow
from strategies import moneyflowtoptogether
from strategies import limit
from strategies import ma
from strategies import mastickup2
from strategies import dailybasic
from strategies import volume
from strategies import low
from strategies import caltools
from strategies import boll
#获取目标股票篮子
#exchange 交易所 SSE上交所 SZSE深交所 
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    #data = data[~ data['name'].str.contains('退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#print (get_stockbasket('SZSE','中小板'))
#获取库数据函数 参数
def get_data_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df
#获取指数函数 参数
def get_data(col):
    mycollection=mydb[col]
    rs = mycollection.find()
    rs_json = []
    for i in rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json

#策略执行模块
def run_strategy(df,strategy):
    result = strategy(df)
    return result

#带参数策略执行模块
def run_strategy_n(df,n,strategy):
    result = strategy(df,n)
    return result
#df_result = run_strategy(get_stockbasket('SZSE','中小板'),moneyflow.get_moneyflow_last)
#收盘指数折线图函数 
def draw_line_param_avgline(indexcode,param,colorname,avgparam,n,avgcolorname):
    df_index = get_data_df(indexcode)[0:30]
    close_array = np.array(df_index[param])
    avgline = round(np.array(df_index[avgparam][0:n]).mean(),2)
    df_index['trade_date']=df_index['trade_date'].apply(str)
    df_index['trade_date']=df_index['trade_date'].apply(parse)
    datetime_array = np.array(df_index['trade_date'], dtype=np.datetime64)
    p = figure(width=400,height=200,x_axis_type="datetime")
    p.line(datetime_array,close_array,color=colorname,legend=param)
    p.line(datetime_array,avgline,color=avgcolorname,legend=avgparam+str(n)+'='+str(avgline))
    p.legend.location = "top_left"
    return p
#策略结果输出模块
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')
@router.get('/strategy/mainpanel')
async def get_strategy_mainpanel(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('strategy_mainpanel.html',
                                {'request':request  # 一定要返回request                              
                                 })
    
@router.get('/strategy/moneyflow_last_netmfamount_top10/')
async def get_moneyflow_last_netmfamount_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result_net_top = run_strategy(get_stockbasket('',''),moneyflow.get_moneyflow_last_netmfamount_top10)
    dict_result= df_result_net_top.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_moneyflow.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'资金净流入策略',
                                 'strategyintro':'最近交易日资金净流入前十集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
@router.get('/strategy/moneyflow_last_buyelgamount_top10/')
async def get_moneyflow_last_buyelgamount_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result_buyelg_top = run_strategy(get_stockbasket('',''),moneyflow.get_moneyflow_last_buyelgamount_top10)
    dict_result= df_result_buyelg_top.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_moneyflow.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'特大单买入策略',
                                 'strategyintro':'最近交易日特大单前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
@router.get('/strategy/moneyflow_last_buylgamount_top10/')
async def get_moneyflow_last_buylgamount_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result_buylg_top = run_strategy(get_stockbasket('',''),moneyflow.get_moneyflow_last_buylgamount_top10)
    dict_result= df_result_buylg_top.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_moneyflow.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'大单买入策略',
                                 'strategyintro':'最近交易日大单前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
@router.get('/strategy/moneyflowtoptogether/')
async def get_moneyflowtoptogether(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result_toptogether = run_strategy(get_stockbasket('',''),moneyflowtoptogether.moneytoptogether)
    dict_result= df_result_toptogether.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_moneyflow.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'资金流入组合策略',
                                 'strategyintro':'最近交易日资金净流入前十、大单买入前十、特大单前十股票交集结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })


#涨停板策略    
@router.get('/strategy/limit_last_strth_top10/')
async def get_limit_last_strth_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),limit.get_limit_last_strth_top10)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'涨停强度策略',
                                 'strategyintro':'最近交易日涨停股票中强度前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/limit_last_fdamount_top10/')
async def get_limit_last_fdamount_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),limit.get_limit_last_fdamount_top10)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日涨停封单金额TOP10',
                                 'strategyintro':'最近交易日涨停股票中封单金额前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/limit_last_fcratio_top10/')
async def get_limit_last_fcratio_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),limit.get_limit_last_fcratio_top10)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日涨停封单金额/当日成交金额TOP10',
                                 'strategyintro':'最近交易日涨停股票中封单金额/当日成交金额前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
    
@router.get('/strategy/limit_last_flratio_top10/')
async def get_limit_last_flratio_top10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),limit.get_limit_last_flratio_top10)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日涨停封单手数/流通股本数TOP10',
                                 'strategyintro':'最近交易日涨停股票中封单手数/流通股本数前十股票集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/limit_last_lb1/')
async def get_limit_last_lb1(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),limit.get_limit_lb1)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日首板涨停个股',
                                 'strategyintro':'按照最近交易日涨停股票中 30个交易日内，出现首个涨停个股列表集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/limit_last_lb/{n}')
async def get_limit_last_lb(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy_n(get_stockbasket('',''),n,limit.get_limit_lb)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_limit.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日连续'+str(n)+'板涨停个股',
                                 'strategyintro':'按照最近交易日涨停股票中 30个交易日内，连续出现'+str(n)+'个涨停个股列表集合结果',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
#MA 
@router.get('/cal/strategy/ma_stick/')
async def cal_mastick(request:Request):
    df_result = run_strategy(get_stockbasket('',''),ma.get_ma_stick)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })  
@router.get('/strategy/ma_stick/')
async def get_ma_stick(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rundate = datetime.now().strftime('%Y%m%d')
    dict_result = get_data('ma_stick'+rundate)
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近N交易日个股日线MA粘合策略',
                                 'strategyintro':'按照最近N交易日，个股MA5,MA10,MA20,MA30均线走势，计算粘合系数，粘合系数阈值为 0.01。判断日MA均线间粘合系数小于阈值，输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
#MA 
@router.get('/cal/strategy/ma_ma5upma10/')
async def cal_ma5upma10(request:Request):
    df_result = run_strategy(get_stockbasket('',''),ma.get_ma_ma5upma10)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })  
@router.get('/strategy/ma_ma5upma10/')
async def get_ma_ma5upma10(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rundate = datetime.now().strftime('%Y%m%d')
    dict_result = get_data('ma_ma5upma10'+rundate)
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股日线MA5上穿MA10策略',
                                 'strategyintro':'按照最近交易日，个股MA5,MA10均线走势，计算粘合系数，上穿系数阈值为 0。判断前一交易日MA5值减去MA10小于阈值，当前交易日MA5值减去MA10大于阈值，输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })    
#MA
@router.get('/cal/strategy/mastickup/')
async def cal_mastickup(request:Request):
    df_result = run_strategy(get_stockbasket('',''),mastickup2.get_ma_stick_up)
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })    
@router.get('/strategy/mastickup/')
async def get_mastickup(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rundate = datetime.now().strftime('%Y%m%d')
    dict_result = get_data('mastickup'+rundate)
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股日线MA5上穿MA10策略',
                                 'strategyintro':'按照最近N交易日，个股MA5,MA10,MA20,MA30均线走势，计算粘合系数，粘合系数阈值为 0.01。判断日MA均线间粘合系数小于阈值，输出为符合策略个股。按照最近N交易日，个股MA5,MA10,MA20,MA30均线走势，计算粘合系数，粘合系数阈值为 0.01。判断日MA均线间粘合系数小于阈值，输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
#dailybasic   
@router.get('/strategy/dailybasic_last_turnover_top100/')
async def get_dailybasic_last_turnover_top100(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),dailybasic.get_dailybasic_last_turnover_top100)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股换手率TOP10策略',
                                 'strategyintro':'按照最近交易日，个股每日交易信息中换手率进行降序排列，并取前10名个股输出，同时叠加个股基本信息。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/dailybasic_last_volumeratio_top100/')
async def get_dailybasic_last_volumeratio_top100(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),dailybasic.get_dailybasic_last_volumeratio_top100)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股量比TOP10策略',
                                 'strategyintro':'按照最近交易日，个股每日交易信息中量比进行降序排列，并取前10名个股输出，同时叠加个股基本信息。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
@router.get('/strategy/dailybasic_last_turnover_between/')
async def get_dailybasic_last_turnover_between(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),dailybasic.get_dailybasic_last_turnover_between)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日位于设定区间换手率个股策略',
                                 'strategyintro':'按照最近交易日，每日交易信息中位于设定区间换手率个股，并按照换手率进行降序排列，并取前10名个股输出，同时叠加个股基本信息。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/dailybasic_last_volumeratio_between/')
async def get_dailybasic_last_volumeratio_between(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),dailybasic.get_dailybasic_last_volumeratio_between)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日位于设定区间量比个股策略',
                                 'strategyintro':'按照最近交易日，每日交易信息中位于设定区间量比个股。并按照量比进行降序排列，并取前10名个股输出，同时叠加个股基本信息。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })
    
@router.get('/strategy/last/weekly_low_n/')
async def get_weekly_low_n(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),low.get_weekly_low_n)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股周线低位运行策略',
                                 'strategyintro':'按照最近交易日，股价运行于低位一定时间，显示一周内多空对比格局。使用周K线反映中长期趋势，一般资金难以控制。输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/last/daily_low_n/')
async def get_daily_low_n(request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy(get_stockbasket('',''),low.get_daily_low_n)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'最近交易日个股日线低位运行策略',
                                 'strategyintro':'按照最近交易日，股价运行于低位n交易日，显示一日内多空对比格局。使用日K线反映短期趋势。输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 })

@router.get('/strategy/last/weekly_strongshort_long/{n}/')
async def get_weekly_strongshort_last_long(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy_n(get_stockbasket('',''),n,low.get_weekly_strongshort_long)    
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'周线低位运行'+str(n)+'周后，长阳线突破策略',
                                 'strategyintro':'股价运行于低位一定时间，显示一周内多空对比格局。使用周K线反映中长期趋势，一般资金难以控制。输出为符合策略个股。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 }) 
@router.get('/strategy/last/daily_vol_low_round/{n}/')
async def get_daily_vol_low_round(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy_n(get_stockbasket('',''),n,volume.get_daily_vol_round_low)    
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'交易日'+str(n)+'日后，成交量圆弧底形态策略',
                                 'strategyintro':'显示成交量在若干交易日减少，反映成交活跃程度降低；最近交易日成交量放量；疑似洗盘结束的信号，主力拉升的开始。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result                              
                                 }) 
@router.get('/strategy/last/daily_vol_close_low_round/{n}/')
async def get_daily_vol_close_low_round(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy_n(get_stockbasket('',''),n,volume.get_daily_vol_close_round_low)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1_dict={}
    p2_dict={}
    result_stocks_list = df_result['ts_code'].tolist()
    for i in result_stocks_list:
        p1 = draw_line_param_avgline('daily_'+i,'close','red','close',n,'black')
        script1,div1 = components(p1)
        p1_dict[i] = (script1,div1)
        p2 = draw_line_param_avgline('daily_'+i,'vol','green','vol',n,'black')
        script2,div2 = components(p2)
        p2_dict[i] = (script2,div2)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result_p2.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'交易日'+str(n)+'日后，成交量股价同步圆弧底形态策略',
                                 'strategyintro':'显示成交量在若干交易日减少，股价下跌，反映成交活跃程度降低；最近交易日成交量放量，股价回升；疑似洗盘结束的信号，主力拉升的开始。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result,
                                 "p1_dict":p1_dict,
                                 "p2_dict":p2_dict,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 }) 
    
#交易日均线上升成交量股价同步圆弧底形态策略
@router.get('/strategy/last/daily_ma_up_vol_close_low_round/{n}/')
async def get_daily_ma_up_vol_close_low_round(n:int,request:Request):  # async加了就支持异步  把Request赋值给request
    runtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_result = run_strategy_n(get_stockbasket('',''),n,volume.get_daily_ma_up_vol_close_round_low)    
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    p1_dict={}
    p2_dict={}
    result_stocks_list = df_result['ts_code'].tolist()
    for i in result_stocks_list:
        p1 = draw_line_param_avgline('daily_'+i,'close','red','close',n,'black')
        script1,div1 = components(p1)
        p1_dict[i] = (script1,div1)
        p2 = draw_line_param_avgline('daily_'+i,'vol','green','vol',n,'black')
        script2,div2 = components(p2)
        p2_dict[i] = (script2,div2)
    dict_result= df_result.to_dict(orient='records')
    return tmp.TemplateResponse('strategy_result_p2.html',
                                {'request':request,  # 一定要返回request
                                 'runtime':runtime,
                                 'strategyname':'交易日'+str(n)+'日后，均线上升,成交量股价同步圆弧底形态策略',
                                 'strategyintro':'均线MA20，MA30上升为前提,显示成交量在若干交易日减少，股价下跌，反映成交活跃程度降低；最近交易日成交量放量，股价回升；疑似洗盘结束的信号，主力拉升的开始。',
                                 'stockscount':len(dict_result),  # 额外的参数可有可无
                                 'stocks':dict_result,
                                 "p1_dict":p1_dict,
                                 "p2_dict":p2_dict,
                                 "js_res":js_res,
                                 "css_res":css_res
                                 }) 