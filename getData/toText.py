# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 16:22:26 2020
#写入list dict 到文本文件
@author: iFunk
"""
import pandas as pd
import numpy as np
import math
import inspect
import time
import os
import datetime
import tushare as ts
import caltools
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

DATESTAMP = time.strftime("%Y%m%d",)

def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#获取stockcode条件集合函数 参数 col 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

#LIST TO TEXT
def set_list_txt(data_list):
    with open('..\\data\\test_'+DATESTAMP+'.txt','w') as f:
        f.write(str(data_list))    
#set_list_txt()

#TEXT TO LIST
def get_txt_list():
    with open('..\\data\\test_'+DATESTAMP+'.txt','r') as f:
        text_data = f.read()
        text_list = eval(text_data)
    for item in text_list:
        print (item)
        
#get_txt_list()

#最近交易日个股日MACD金叉策略
def get_stocks_daily_macd_cross():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()      
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}  
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#0轴上方金叉
def get_stocks_daily_macd_cross_abovezreo():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_1 > 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#最近交易日个股日boll线收敛策略
def get_stocks_daily_boll_narrow():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()      
    df_stocks = get_stockbasket('','')
    #testlist = ['000001.SZ','600000.SH']
    for stockcode in df_stocks['ts_code']:
        result_dict = {}  
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<20):
            continue
        else:
            df_boll = caltools.get_boll(df_qfq)
            df_qfq['BOLL_UPPER'] = df_boll['upper']
            df_qfq['BOLL_MIDDLE'] = df_boll['middle']
            df_qfq['BOLL_LOWER'] = df_boll['lower']
            BOLL_UPPER_0 = round(df_qfq['BOLL_UPPER'][0],2)
            BOLL_MIDDLE_0 = round(df_qfq['BOLL_MIDDLE'][0],2)
            BOLL_LOWER_0 = round(df_qfq['BOLL_LOWER'][0],2)
            #print ('SCAN',stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],BOLL_UPPER_0,BOLL_MIDDLE_0,BOLL_LOWER_0)
            if (BOLL_UPPER_0 > 0 and BOLL_LOWER_0 >0):                
                BOLL_UPPER_LOWER_DISTENCE = round(math.sqrt(BOLL_UPPER_0*BOLL_LOWER_0)/BOLL_UPPER_0,4)           
            else:
                BOLL_UPPER_LOWER_DISTENCE > 1.0
            if (BOLL_UPPER_LOWER_DISTENCE < 1.0 and BOLL_UPPER_LOWER_DISTENCE > 0.97):                
                print ('SELECT',stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],BOLL_UPPER_0,BOLL_MIDDLE_0,BOLL_LOWER_0,BOLL_UPPER_LOWER_DISTENCE)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

def get_stocks_daily_mtm_cross_abovezero():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            MTM_1 = round(df_qfq['MTM'][1],4)
            MAMTM_1 = round(df_qfq['MAMTM'][1],4)
            MTM_0 = round(df_qfq['MTM'][0],4)
            MAMTM_0 = round(df_qfq['MAMTM'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (MTM_1 < MAMTM_1 and MTM_0 > MAMTM_0 and MTM_0 > 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],MTM_1,MAMTM_1,MTM_0,MAMTM_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#0轴上方金叉
def get_stocks_daily_macd_cross_abovezreo_mtm_cross_abovezero():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            MTM_1 = round(df_qfq['MTM'][1],4)
            MAMTM_1 = round(df_qfq['MAMTM'][1],4)
            MTM_0 = round(df_qfq['MTM'][0],4)
            MAMTM_0 = round(df_qfq['MAMTM'][0],4)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_1 > 0):
                if (MTM_1 < MAMTM_1 and MTM_0 > MAMTM_0 and MTM_0 > 0):
                    print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0,MTM_0)
                    result_dict['ts_code'] = stockcode
                    result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                    result_dict['trigger_close'] = df_qfq['close'][0]
                    result_dict['reason'] = FNAME
                    result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df


#0轴上方金叉 5日前MACD绿柱
def get_stocks_daily_macd_cross_abovezreo_greenbefore():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #MACD_2 = round(df_qfq['MACD'][2],4)
            #MACD_3 = round(df_qfq['MACD'][3],4)
            #MACD_4 = round(df_qfq['MACD'][4],4)
            MACD_1_6_MAX = df_qfq['MACD'][1:6].max()
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_1 > 0 and MACD_1_6_MAX < 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df


#0轴上方5日MACD绿柱逐渐缩小
def get_stocks_daily_macd_abovezreo_greenbefore():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<6):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            MACD_0 = round(df_qfq['MACD'][0],4)
            MACD_1 = round(df_qfq['MACD'][1],4)
            MACD_2 = round(df_qfq['MACD'][2],4)
            #MACD_3 = round(df_qfq['MACD'][3],4)
            #MACD_4 = round(df_qfq['MACD'][4],4)
            MACD_0_5_MAX = df_qfq['MACD'][0:5].max()
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            #0轴上方
            if (DIF_0 > 0 and  DEA_0 > 0 ):
                #至少连续5日绿柱
                if (MACD_0_5_MAX < 0):
                    #绿柱连续3日缩小
                    if (MACD_0 > MACD_1 and MACD_1 > MACD_2):
                        print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                        result_dict['trigger_close'] = df_qfq['close'][0]
                        result_dict['reason'] = FNAME
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

df = get_stocks_daily_macd_abovezreo_greenbefore()
#df = get_stocks_daily_mtm_cross_abovezero()
#df = get_stocks_daily_macd_cross_abovezreo_mtm_cross_abovezero()
list_stockcode = df['ts_code'].tolist()
print (len(list_stockcode))
set_list_txt(list_stockcode)