# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 10:51:28 2020
#行业分析模块
@author: Boris
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

#from api import caltools
import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')

#查询库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取条件集合函数 参数 col param 返回df
def get_col_param_df(col,param,value):
    mycollection=mydb[col]
    query = {param:value}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取数据函数 排序
def get_col_sort_df(collection,colum,asc,count):
    mycollection=mydb[collection]
    rs_col = mycollection.find().limit(count).sort([(colum,asc)])
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取数据函数 排序
def get_col_sort2_df(collection,colum1,asc1,colum2,asc2,count):
    mycollection=mydb[collection]
    rs_col = mycollection.find().limit(count).sort([(colum1,asc1),(colum2,asc2)])
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#获取申万行业列表
def get_data_sw_level(levelname):
    df_swl1 = get_col_df(levelname)
    return df_swl1.to_dict('records')

#获取概念分类列表
def get_data_concept_list():
    df = get_col_df('concept_list')
    df['concept_name'] = df['name']
    df['concept_code'] = df['code']
    return df.to_dict('records')


#获取证监会行业分类列表
def get_data_stocksbasic_industry_list():
    df_stocksbasic = get_col_df('stocksbasic_industry_list')
    #df_stocksbasic['stockslist_count'] = df_stocksbasic['stockslist_count'].round(0)
    return df_stocksbasic.to_dict('records')


#获取申万行业分析结果
def get_analysis_sw_level(levelname):
    df_swl1 = get_col_df(levelname)
    return df_swl1.to_dict('records')

#获取概念分类分析结果
def get_analysis_concept():
    df = get_col_df('analysis_category_concept_list')
    return df.to_dict('records')    

#获取证监会行业分类分析结果
def get_analysis_stocksbasic_industry():
    df_stocksbasic = get_col_df('analysis_category_stocksbasic_industry_list')
    return df_stocksbasic.to_dict('records')
#获取包含股票代码
def get_data_stocks(col,param,indexcode):
    mycollection=mydb[col]
    query = {param:indexcode}
    rs_col = mycollection.find(query)
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    stockslist = df_col['stockslist'][0]
    df_result = pd.DataFrame()
    df_result['ts_code'] = pd.Series(stockslist)
    df_stocks_dailybasic_lastday = get_col_df('stocks_dailybasic_lastday')
    df_result = pd.merge(df_result, df_stocks_dailybasic_lastday, how='left', on='ts_code')
    df_result['circ_mv'] = (df_result['circ_mv']/10000).round(1)
    return df_result
#df = get_data_stocks('concept_list','code','TS0')

def get_stocks_analysis_up_vc():
    df_industry = get_col_df('analysis_category_stocksbasic_industry_list')
    #df_industry = get_col_df('analysis_category_concept_list')
    #df_industry = get_col_df('analysis_category_swl3')
    #通用条件
    df_analysis_result = df_industry[(df_industry['stocks_up_count_ratio']==1) & (df_industry['stocks_limit_count']>=1) & (df_industry['stocks_vol_up_count_ratio']>=0.6) & (df_industry['stocks_vol_up_100_count_ratio']>=0.25)]
    #严谨条件
    #df_analysis_result = df_industry[(df_industry['stocks_up_count_ratio']==1) & (df_industry['stocks_limit_count']>=1) & (df_industry['stocks_vol_up_count_ratio']>=0.95) & (df_industry['stocks_vol_up_100_count_ratio']>=0.6)]
    print (df_analysis_result['industry_name'].tolist())
    list_result = []
    for item in df_analysis_result['stockslist']:
        #print (item,len(item))
        list_result.extend(item)
    print (list_result,len(list_result))
    df_daily_lastday = pro.daily(trade_date='20201126')
    df_today = pd.DataFrame()
    df_today['ts_code'] = df_daily_lastday['ts_code']
    df_today['pct_chg_today'] = df_daily_lastday['pct_chg']
    df_result = df_today[df_today.ts_code.isin(list_result)]    
    print (df_result['pct_chg_today'].mean(),df_result['pct_chg_today'].max(),df_result['pct_chg_today'].min())
    #换手率因子
    df_dailybasic = pro.daily_basic(ts_code='', trade_date='20201127', fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')
    #df_result = pd.merge(df_result, df_dailybasic, how='left', on='ts_code')
    #df_result = df_result[df_result['turnover_rate_f']>5]
    #print (df_result['pct_chg'].mean())
    #指标因子
    df_result = pd.DataFrame()
    for stock in list_result:
        df_stock_result = get_col_df('daily_qfq_macd_'+stock).head(1)
        result_dict = df_stock_result.to_dict('records')
        df_result = df_result.append(result_dict,ignore_index=True)
    df_result = pd.merge(df_result, df_today, how='left', on='ts_code')
    df_result = df_result[(df_result['DIF']>df_result['DEA']) & (df_result['DEA']>=0) ]
    df_result = pd.merge(df_result, df_dailybasic, how='left', on='ts_code')
    df_result = df_result[df_result['turnover_rate_f']>5]
    #print (df_result['pct_chg_today'].mean())
    return df_result
#df = get_stocks_analysis_up_vc()    

#获取日内实时行业热点分析
def get_realtime_analysis_category_industry():
    #获取实时数据
    df_realtime_daily = get_col_df('realtime_daily_data')
    #数据处理
    #增加基础信息
    #df_stocksbasic = get_col_df('stocksbasic')
    #df_realtime_daily = pd.merge(df_realtime_daily, df_stocksbasic, how='left', on='ts_code')
    #计算涨跌百分比
    df_realtime_daily['realtime_pct_chg'] = round((df_realtime_daily['realtime_close']-df_realtime_daily['realtime_open'])*100/df_realtime_daily['realtime_open'],2)
    #增加昨日行情信息
    df_dailybasic_lastday = get_col_df('stocks_dailybasic_lastday')
    df_realtime_daily = pd.merge(df_realtime_daily, df_dailybasic_lastday, how='left', on='ts_code')
    #计算实时量比    
    #计算实时换手率
    #分组数据处理    
    df_groupby_industry = df_realtime_daily.groupby('industry')
    df_groupby_industry_result = pd.DataFrame()
    index_code = 1
    for name,group in df_groupby_industry:        
        result_dict = {}
        #GET GROUP
        df_group = pd.DataFrame(group)
        #涨跌个股数量统计
        stocks_count = len(df_group['ts_code'])
        stocks_up_count = len(df_group[df_group['realtime_pct_chg']>0])        
        #最大涨幅
        stocks_pct_chg_max = round(df_group['realtime_pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(df_group['realtime_pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(df_group['realtime_pct_chg'].mean(),2)
        #涨停个股数量统计
        stocks_limit_count = len(df_group[df_group['realtime_pct_chg']>9.8])
        #stocks_down_count = len(df_group[df_group['realtime_pct_chg']<=0])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)   
        result_dict['index_code'] = str(index_code)
        result_dict['industry_name'] = name
        result_dict['realtime_trade_date'] = df_group['realtime_trade_date'].head(1).iloc[0]
        result_dict['realtime_trade_time'] = df_group['realtime_trade_time'].head(1).iloc[0]
        result_dict['stockslist_count'] = str(len(df_group))
        result_dict['stockslist'] = df_group['ts_code'].tolist()
        result_dict['stocks_amount_total'] = df_group['realtime_amount'].sum()
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg        
        result_dict['stocks_limit_count'] = stocks_limit_count     
        df_groupby_industry_result = df_groupby_industry_result.append(result_dict,ignore_index=True)
        #print (index_code,len(df_groupby_industry_result))
        index_code += 1
    return df_groupby_industry_result

#df =   get_realtime_analysis_category_industry()  

#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')

#主面板

#行业

#三大产业分类数据
@router.get('/industry/three')
async def get_three(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    industry_dict = get_col_df('industry_three').to_dict('records')
    return tmp.TemplateResponse('industry_three.html',
                                {'request':request,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })

#行业分类数据
@router.get('/industry/category')
async def get_category(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    swl1_dict = get_data_sw_level('swl1')
    swl2_dict = get_data_sw_level('swl2')
    swl3_dict = get_data_sw_level('swl3')
    concept_dict = get_data_concept_list()
    industry_dict = get_data_stocksbasic_industry_list()
    return tmp.TemplateResponse('industry_category.html',
                                {'request':request,
                                 'swl1_dict':swl1_dict,
                                 'swl2_dict':swl2_dict,
                                 'swl3_dict':swl3_dict,
                                 'concept_dict':concept_dict,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })
    
@router.get('/industry/category/{col}/{param}/{indexcode}/stocks/')
async def get_category_stocks(request:Request,col:str,param:str,indexcode:str):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    category_data = get_col_param_df(col,param,indexcode).to_dict('records')
    stocks_dict = get_data_stocks(col,param,indexcode).to_dict('records')
    return tmp.TemplateResponse('industry_stocks.html',
                                {'request':request,
                                 "category":category_data,
                                 "stocks":stocks_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 }) 
    
#行业分类分析数据
@router.get('/industry/analysis/category')
async def get_analysis_category(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    swl1_dict = get_data_sw_level('analysis_category_swl1')
    swl2_dict = get_data_sw_level('analysis_category_swl2')
    swl3_dict = get_data_sw_level('analysis_category_swl3')
    concept_dict = get_analysis_concept()
    industry_dict = get_analysis_stocksbasic_industry()
    return tmp.TemplateResponse('industry_analysis_category.html',
                                {'request':request,
                                 'swl1_dict':swl1_dict,
                                 'swl2_dict':swl2_dict,
                                 'swl3_dict':swl3_dict,
                                 'concept_dict':concept_dict,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })    
    
#行业看多股价分析 industry/analysis/up/close/
@router.get('/industry/analysis/up/close/')
async def get_analysis_up_close(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    swl1_dict = get_col_sort2_df('analysis_category_swl1','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    swl2_dict = get_col_sort2_df('analysis_category_swl2','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    swl3_dict = get_col_sort2_df('analysis_category_swl3','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    concept_dict = get_col_sort2_df('analysis_category_concept_list','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    industry_dict = get_col_sort2_df('analysis_category_stocksbasic_industry_list','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    return tmp.TemplateResponse('industry_analysis_up_close.html',
                                {'request':request,
                                 'swl1_dict':swl1_dict,
                                 'swl2_dict':swl2_dict,
                                 'swl3_dict':swl3_dict,
                                 'concept_dict':concept_dict,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })
    
#行业看多成交量分析 industry/analysis/up/vol/
@router.get('/industry/analysis/up/vol/')
async def get_analysis_up_vol(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    swl1_dict = get_col_sort_df('analysis_category_swl1','stocks_vol_up_count_ratio',-1,9999).to_dict('records')
    swl2_dict = get_col_sort_df('analysis_category_swl2','stocks_vol_up_count_ratio',-1,9999).to_dict('records')
    swl3_dict = get_col_sort_df('analysis_category_swl3','stocks_vol_up_count_ratio',-1,9999).to_dict('records')
    concept_dict = get_col_sort_df('analysis_category_concept_list','stocks_vol_up_count_ratio',-1,9999).to_dict('records')
    industry_dict = get_col_sort_df('analysis_category_stocksbasic_industry_list','stocks_vol_up_count_ratio',-1,9999).to_dict('records')
    return tmp.TemplateResponse('industry_analysis_up_vol.html',
                                {'request':request,
                                 'swl1_dict':swl1_dict,
                                 'swl2_dict':swl2_dict,
                                 'swl3_dict':swl3_dict,
                                 'concept_dict':concept_dict,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })

#行业看多量价三因子分析 industry/analysis/up/vol/
@router.get('/industry/analysis/up/vc/3/')
async def get_analysis_up_vol_close_3(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    swl1_dict = get_col_sort2_df('analysis_category_swl1','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    swl2_dict = get_col_sort2_df('analysis_category_swl2','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    swl3_dict = get_col_sort2_df('analysis_category_swl3','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    concept_dict = get_col_sort2_df('analysis_category_concept_list','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    industry_dict = get_col_sort2_df('analysis_category_stocksbasic_industry_list','stocks_up_count_ratio',-1,'stocks_limit_count',-1,9999).to_dict('records')
    return tmp.TemplateResponse('industry_analysis_up_vol_close.html',
                                {'request':request,
                                 'swl1_dict':swl1_dict,
                                 'swl2_dict':swl2_dict,
                                 'swl3_dict':swl3_dict,
                                 'concept_dict':concept_dict,
                                 'industry_dict':industry_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })

#实时行业热点分析
@router.get('/industry/analysis/realtime/')
async def get_analysis_industry_realtime(request:Request):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #industry_dict = get_realtime_analysis_category_industry().to_dict('records')
    industry_df = get_realtime_analysis_category_industry()
    industry_df_sort = industry_df.sort_values(by="stocks_up_count_ratio",axis=0,ascending=False)
    industry_dict_sort = industry_df_sort.to_dict('records')
    return tmp.TemplateResponse('industry_analysis_realtime.html',
                                {'request':request,
                                 'industry_dict':industry_dict_sort,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })
#实时行业个股行情
@router.get('/realtime/industry/{industryname}/')
async def get_realtime_industry_indexcode(request:Request,industryname:str):  # async加了就支持异步  把Request赋值给request
    js_res = INLINE.render_js()
    css_res = INLINE.render_css()
    #获取实时行情
    df_realtime_daily = get_col_df('realtime_daily_data')
    #计算涨跌百分比
    df_realtime_daily['realtime_pct_chg'] = round((df_realtime_daily['realtime_close']-df_realtime_daily['realtime_open'])*100/df_realtime_daily['realtime_open'],2)
    #增加昨日行情及行业基本信息信息
    df_dailybasic_lastday = get_col_df('stocks_dailybasic_lastday')
    df_realtime_daily = pd.merge(df_realtime_daily, df_dailybasic_lastday, how='left', on='ts_code')    
    #筛选当前industryname
    df_realtime_daily = df_realtime_daily[df_realtime_daily['industry'] == industryname]
    realtime_daily_dict = df_realtime_daily.to_dict('records')
    return tmp.TemplateResponse('realtime_stocks_daily.html',
                                {'request':request,
                                 'industryname':industryname,
                                 'stocks_daily_indexcode':realtime_daily_dict,
                                 "js_res":js_res,
                                 "css_res":css_res  
                                 })