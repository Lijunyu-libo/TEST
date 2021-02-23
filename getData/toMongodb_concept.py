# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取月线数据并入库
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time
from starlette.requests import Request
#from fastapi import FastAPI
from fastapi import APIRouter
router = APIRouter()
from starlette.templating import Jinja2Templates
# MONGODB CONNECT
import tushare as ts
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
today=time.strftime('%Y%m%d',)
#入库
from pymongo import MongoClient
#print (os.getcwd())
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col



#获取当前分类的成份股
def getstocks(ccode):
    df_stocks = pro.concept_detail(id=ccode)
    dict_stocks = df_stocks.to_dict(orient='records')
    #print (type(dict_stocks))
    stocks_items=[]
    for stock in dict_stocks:
        #print (type(stock))
        stockname = stock['name']
        stockcode = stock['ts_code']
        conceptname = stock['concept_name']
        #拼装json
        data = {"stockcode":stockcode,"stockname":stockname,"conceptname":conceptname}
        stocks_items.append(data)   
    return stocks_items
#stocks=getstocks('TS344')
#print (stocks)

#获取ts概念分类
'''
def get_concept():
    df = pro.concept()
    #print (df)
    conceptstockslist = []
    for i in df['code']:
        stockslist = getstocks(i)
        conceptstockslist.append(stockslist)
    df['stocks'] = conceptstockslist
    print (df)
    df.to_csv('./data/concept/'+'concept.csv')
'''



def get_concept():
    #获取概念列表
    df_concept = pro.concept()
    mycollection=mydb['concept_list']
    mycollection.remove()
    records = json.loads(df_concept.T.to_json()).values()
    mycollection.insert(records)
    #获取基本信息
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_stockbasic['stockname'] = df_stockbasic['name']
    #获取每日行情
    df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
    lasttradeday = df_tradedate['cal_date'].tail(1).iloc[0]
    df_daily = pro.daily(trade_date=lasttradeday)
    df_daily['amount'] = df_daily['amount']/10000
    #获取每日指标
    df_daily_basic = pro.daily_basic(ts_code='', trade_date=lasttradeday, fields='ts_code,turnover_rate,volume_ratio,pe_ttm,pb,circ_mv')
    df_daily_basic['circ_mv'] = df_daily_basic['circ_mv']/10000
    for i in df_concept['code']:
        #获取概念所有个股
         df_conceptstocks = pro.concept_detail(id=i)
         df_temp = pd.merge(df_conceptstocks, df_stockbasic, how='left', on='ts_code')
         df = pd.merge(df_temp, df_daily_basic, how='left', on='ts_code')
         df2 = pd.merge(df, df_daily, how='left', on='ts_code')
         #df.to_csv('./data/concept/'+i+'_concept.csv')
         mycollection=mydb['concept_'+i]
         mycollection.remove()
         records = json.loads(df2.T.to_json()).values()
         mycollection.insert(records)
         print (i)


def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open('./data/concept/'+filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

def get_data_concept():
    #获取基本信息
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,area,industry')
    df_stockbasic = df_stockbasic.rename(columns={'name':'stockname'})
    df_concept_list = get_col_df('concept_list')
    df_data_concept = pd.DataFrame()
    for index, row in df_concept_list.iterrows():
        df_stockbasic_concept_stockslist = pd.DataFrame()
        concept_name = row['concept_name']
        concept_code = row['concept_code']
        concept_stockslist = row['stockslist']
        df_stockbasic_concept_stockslist = df_stockbasic[df_stockbasic.ts_code.isin(concept_stockslist)]
        df_stockbasic_concept_stockslist['concept_name'] = concept_name
        df_stockbasic_concept_stockslist['concept_code'] = concept_code
        print (index,concept_name,concept_code,len(df_stockbasic_concept_stockslist))
        df_data_concept = df_data_concept.append(df_stockbasic_concept_stockslist,ignore_index=True)
    return df_data_concept
#df = get_data_concept()


#获取单日内概念热点分析数据
def get_daily_analysis_category_concept(tradedate):
    #获取日线数据
    df_daily = pro.daily(trade_date = tradedate)
    df_concept_list = get_col_df('concept_list')
    df_concept_result = pd.DataFrame()
    for index, row in df_concept_list.iterrows():
        concept_name = row['concept_name']
        concept_code = row['concept_code']
        concept_stockslist = row['stockslist']      
        result_dict = {}
        df_group = pd.DataFrame()
        #GET GROUP
        df_group = df_daily[df_daily.ts_code.isin(concept_stockslist)]
        #涨跌个股数量统计
        stocks_count = len(df_group['ts_code'])
        stocks_up_count = len(df_group[df_group['pct_chg']>=0])
        stocks_down_count = len(df_group[df_group['pct_chg']<0])        
        #最大涨幅
        stocks_pct_chg_max = round(df_group['pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(df_group['pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(df_group['pct_chg'].mean(),2)
        #涨停个股数量统计
        stocks_limit_count = len(df_group[df_group['pct_chg']>9.8])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)   
        result_dict['concept_code'] = concept_code
        result_dict['concept_name'] = concept_name
        result_dict['trade_date'] = df_group['trade_date'].head(1).iloc[0]
        result_dict['stockslist_count'] = str(len(df_group))
        result_dict['stockslist'] = df_group['ts_code'].tolist()
        result_dict['stocks_amount_total'] = df_group['amount'].sum()
        result_dict['stocks_vol_total'] = df_group['vol'].sum()
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_down_count'] = stocks_down_count
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg        
        result_dict['stocks_limit_count'] = stocks_limit_count     
        df_concept_result = df_concept_result.append(result_dict,ignore_index=True)
    return df_concept_result

#保存概念分类分析数据入库
def save_daily_analysis_category_concept_tradedatelist(startdate,enddate):
    df_tradedatelist = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=startdate, end_date=enddate)
    tradedatelist = df_tradedatelist['cal_date'].tolist()
    #定义文档名称
    mycol = mydb['daily_analysis_category_concept']
    mycol.remove()
    for i in tradedatelist:    
        df =   get_daily_analysis_category_concept(i)
        mycol.insert_many(df.to_dict('records'))
        print (i,df['trade_date'][0],'daily_analysis_category_concept:'+str(len(df)))

#save_daily_analysis_category_concept_tradedatelist(20200101,20201231)

tmp = Jinja2Templates(directory='./api/templates')
@router.get('/update/concept/')
async def get_indexs(request:Request):
    get_concept()
    #toMongodb('concept','concept')
    return tmp.TemplateResponse('update_data.html',
                                {'request':request
                                 })