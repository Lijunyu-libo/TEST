# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 12:47:22 2020
申万指数TOP5策略
@author: iFunk
"""
import pandas as pd
import time
import inspect
#查询库
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#获取正在运行函数(或方法)名称
def get__function_name():

    return inspect.stack()[1][3]
#获取库数据函数 返回df
def get_data_df(col):
    col=mydb[col]
    rs = col.find()
     # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(rs))
    return df

def get_sw_indexs_up_stocks_top_n(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame() 
    indexs_sw_last = get_data_df('swindexs_daily_last')
    pct_change_asc = indexs_sw_last.sort_values(by="pct_change",ascending=False)
    pct_change_asc.reset_index(drop=True, inplace=True)
    pct_change_asc_top10 = pct_change_asc.head(n)
    indexs_sw_top10_indexcode_list = pct_change_asc_top10['ts_code'].tolist()
    print (indexs_sw_top10_indexcode_list)
    result_df = pd.DataFrame()
    for i in indexs_sw_top10_indexcode_list:
        index_stocks = get_data_df('stocks_'+i)
        stocks_pct_change_asc = index_stocks.sort_values(by="pct_chg",ascending=False)
        stocks_pct_change_asc.reset_index(drop=True, inplace=True)
        stocks_pct_change_asc_top3 = stocks_pct_change_asc.head(3)
        result_df = pd.concat([result_df,stocks_pct_change_asc_top3],axis=0,join='outer')
        print (i,len(stocks_pct_change_asc_top3))
    result_df_pct_change_asc = result_df.sort_values(by="pct_chg",ascending=False)
    result_df_pct_change_asc.reset_index(drop=True, inplace=True)
    result_df_pct_change_asc = result_df_pct_change_asc.drop_duplicates(['ts_code'])
    result_df_pct_change_asc_top10 = result_df_pct_change_asc.head(n)
    result_df = pd.DataFrame()
    result_df['ts_code'] = result_df_pct_change_asc_top10['ts_code']
    result_df['trigger_trade_date'] = result_df_pct_change_asc_top10['trade_date']
    result_df['trigger_close'] = result_df_pct_change_asc_top10['close']
    result_df['reason'] = FNAME+str(n)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df 
