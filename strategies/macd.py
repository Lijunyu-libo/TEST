# -*- coding: utf-8 -*-
"""
Spyder Editor
计算所有股票MACD
This is a temporary script file.
"""
import sys
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import inspect
#import caltools
#import tradememo
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
import tushare as ts
from tushare.util import formula as fm
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')


def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#计算当前日期的前N天的时间戳
def get_day_time(n):
    the_date = datetime.datetime.now()
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str
#TRADEDATE
def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday

#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

#TUSHARE GET STOCK_DAILY BY CODE
def get_daily_code(stockcode):
    df = pro.daily(ts_code=stockcode)
    return df

#TUSHARE GET STOCK_DAILY BY tradedate
def get_daily_tradedate(tradedate):
    df = pro.daily(trade_date=tradedate)
    return df


#GET ALL STOCKS WEEKLY QFQ
def get_stocks_weekly_df():
    mycollection=mydb["stocks_weekly_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode
             
#获取stockcode条件集合函数 参数 stockcode
def get_data_stockcode(col,stockcode):
    mycollection=mydb[col]
    query = {'ts_code':stockcode}
    index_rs = mycollection.find(query)
    rs_json = []
    for i in index_rs:
        rs_json.append(i)
    #print (rs_json)
    return rs_json
#获取stockcode条件集合函数 参数 col 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

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


#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

def MACD_CN(close, fastperiod=12, slowperiod=26, signalperiod=9) :
        macdDIFF, macdDEA, macd = talib.MACDEXT(close, fastperiod=fastperiod, fastmatype=1, slowperiod=slowperiod, slowmatype=1, signalperiod=signalperiod, signalmatype=1)
        macd = macd * 2
        return macdDIFF, macdDEA, macd
    
def get_stocks_weekly_macd():
    result_list=[]
    result_dict={}
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = ts.pro_bar(ts_code=stockcode, freq='W', adj='qfq')
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            #按照日期升序df
            df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
            #重新设置索引
            df_qfq_asc.reset_index(drop=True, inplace=True)
            #初始化CDMA df
            df = pd.DataFrame()
            df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
            df['MACD'] = df['MACD'] * 2
            #按照index反序
            df = df.sort_index(ascending=False)
            #重新设置索引
            df.reset_index(drop=True, inplace=True)
            #赋值series给df_qfq
            df_qfq['DIF'] = df['DIF']
            df_qfq['DEA'] = df['DEA']
            df_qfq['MACD'] = df['MACD']
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_list.append(stockcode)
                result_dict['ts_code'] = stockcode
                result_dict['trade_date'] = df_qfq['trade_date'][0]
                result_dict['close'] = df_qfq['close'][0]
                result_dict['reason'] = str(sys._getframe().f_code.co_name)
    print (len(result_list))
    #策略结果自动入库
    #tradememo.autotrade(result_dict)

def get_stocks_daily_macd():
    result_list=[]
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            #按照日期升序df
            df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
            #重新设置索引
            df_qfq_asc.reset_index(drop=True, inplace=True)
            #初始化CDMA df
            df = pd.DataFrame()
            df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
            df['MACD'] = df['MACD'] * 2
            #按照index反序
            df = df.sort_index(ascending=False)
            #重新设置索引
            df.reset_index(drop=True, inplace=True)
            #赋值series给df_qfq
            df_qfq['DIF'] = df['DIF']
            df_qfq['DEA'] = df['DEA']
            df_qfq['MACD'] = df['MACD']
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DEA_0 < 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_list.append(stockcode)
    print (len(result_list))

#最近交易日个股周MACD金叉策略
def get_stocks_weekly_macd_cross():
    result_list=[]
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('weekly_qfq_macd_'+stockcode)
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
                result_list.append(stockcode)
    print (len(result_list))

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

#n周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_n_macd_cross_turnover(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('weekly_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n+2):
            continue
        else:
            df_qfq_n = df_qfq[n:]
            if (df_qfq_n is None or len(df_qfq_n)<2):
                continue
            else:
                DIF_1 = round(df_qfq['DIF'][n+1],4)
                DEA_1 = round(df_qfq['DEA'][n+1],4)
                DIF_0 = round(df_qfq['DIF'][n],4)
                DEA_0 = round(df_qfq['DEA'][n],4)
                #MACD_0 = round(df_qfq['MACD'][0],4)
                #MACD_1 = round(df_qfq['MACD'][1],4)
                #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
                if (DIF_1 < DEA_1 and DIF_0 > DEA_0):
                    vol_n_total = df_qfq['vol'][:n].sum()
                    df = pro.daily_basic(ts_code=stockcode)
                    total_share = df['total_share'][0]*100
                    total_turnover = round(vol_n_total/total_share,2)                   
                    cost_avg =round((df_qfq['close'][:n].max()+df_qfq['close'][:n].min())/2,2)
                    chg_pct=round(df['close'][0]/df_qfq['close'][n]-1,2)
                    profit_pct = round(df['close'][0]/cost_avg-1,2)
                    if (total_turnover>=300):
                        #print (stockcode,df_qfq['trade_date'][n],df_qfq['close'][n],DIF_1,DEA_1,DIF_0,DEA_0)
                        print (stockcode,df_qfq['trade_date'][n],'n周涨跌',chg_pct,'主力成本涨跌',profit_pct,df['close'][0],cost_avg,total_turnover)
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                        result_dict['trigger_close'] = df_qfq['close'][0]
                        result_dict['reason'] = FNAME+str(n)
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df
#get_stocks_weekly_n_macd_cross_turnover(7)


#n交易日前日0轴上MACD金叉，后n日量增价增策略
def get_stocks_daily_n_macd_cross_abovezero_n_vol_close_up(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {} 
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=n+2):
            continue
        else:
            df_qfq_n = df_qfq[n:]
            if (df_qfq_n is None or len(df_qfq_n)<=2):
                continue
            else:
                DIF_4 = round(df_qfq['DIF'][n+1],4)
                DEA_4 = round(df_qfq['DEA'][n+1],4)
                DIF_3 = round(df_qfq['DIF'][n],4)
                DEA_3 = round(df_qfq['DEA'][n],4)
                #MACD_0 = round(df_qfq['MACD'][0],4)
                #MACD_1 = round(df_qfq['MACD'][1],4)
                #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
                if (DIF_4 < DEA_4 and DIF_3 > DEA_3 and DIF_4>0):
                    if(df_qfq['vol'][0]>df_qfq['vol'][1]>df_qfq['vol'][2] and df_qfq['close'][0]>df_qfq['close'][1]>df_qfq['close'][2]):
                        print (stockcode,df_qfq['trade_date'][0])
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                        result_dict['trigger_close'] = df_qfq['close'][0]
                        result_dict['reason'] = FNAME+str(n)
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df  
#get_stocks_daily_n_macd_cross_abovezero_n_vol_close_up(3)    


#n交易日前日MACD金叉，后n日DIF增策略
def get_stocks_daily_n_macd_cross_n_dif_up(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {} 
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=n+2):
            continue
        else:
            df_qfq_n = df_qfq[n:]
            if (df_qfq_n is None or len(df_qfq_n)<=2):
                continue
            else:
                DIF_4 = round(df_qfq['DIF'][n+1],4)
                DEA_4 = round(df_qfq['DEA'][n+1],4)
                DIF_3 = round(df_qfq['DIF'][n],4)
                DEA_3 = round(df_qfq['DEA'][n],4)
                #MACD_0 = round(df_qfq['MACD'][0],4)
                #MACD_1 = round(df_qfq['MACD'][1],4)
                #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
                if (DIF_4 < DEA_4 and DIF_3 > DEA_3):
                    if(df_qfq['DIF'][0]>df_qfq['DIF'][1]>df_qfq['DIF'][2]):
                        if (df_qfq['DIF'][1]>0):
                            print (stockcode,df_qfq['trade_date'][0])
                            result_dict['ts_code'] = stockcode
                            result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                            result_dict['trigger_close'] = df_qfq['close'][0]
                            result_dict['reason'] = FNAME+str(n)
                            result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#当前交易日个股N日MACDDIFF极端低点策略
def get_stocks_daily_n_macd_diff_low(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {} 
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=n+2):
            continue
        else:
            df_qfq_n = df_qfq[n:]
            if (df_qfq_n is None or len(df_qfq_n)<=2):
                continue
            else:
                DIF_MIN = df_qfq['DIF'][0:n].min()
                DIF_MAX = df_qfq['DIF'][0:5].max()
                #if (df_qfq['DIF'][1]==DIF_MIN and df_qfq['DIF'][0]<0):
                if (df_qfq['DIF'][0]==DIF_MIN and df_qfq['DIF'][0]<0):
                    if(DIF_MAX<=0):
                        print (stockcode,df_qfq['trade_date'][0])
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                        result_dict['trigger_close'] = df_qfq['close'][0]
                        result_dict['reason'] = FNAME+str(n)
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df
#MACD主升浪组合策略
def get_macd_mainwave():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {} 
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<31):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #df_qfq = df_qfq[2:30]
            #DEA\DIF 近距离振荡
            '''
            DEA_AVG = df_qfq['DEA'].mean()
            DEA_MAX = df_qfq['DEA'].max()
            DEA_MIN = df_qfq['DEA'].min()
            DEA_MAX_R = DEA_MAX/DEA_AVG
            DEA_MIN_R = DEA_MIN/DEA_AVG
            df_qfq['DEA_DIF_AVG'] = (df_qfq['DEA']+df_qfq['DIF'])/2
            DEA_DIF_AVG = df_qfq['DEA_DIF_AVG'].mean()
            if (DEA_MIN_R<DEA_DIF_AVG<DEA_MAX_R):
                '''
            #df_qfq['DIF_DEA_R'] = (df_qfq['DIF']-df_qfq['DEA'])/df_qfq['DEA']
            #DIF_DEA_R_MAX = math.fabs(df_qfq['DIF_DEA_R'].max())
            #DIF_DEA_R_MIN = math.fabs(df_qfq['DIF_DEA_R'].min())
            #w = 0.3
            #if (DIF_DEA_R_MAX<w and DIF_DEA_R_MIN<w):
            #CCI > 100
            if (df_qfq['CCI'][0]>100):
            #0轴上方
                if(DIF_0>=0):
                    #金叉
                   if (DIF_1 < DEA_1 and DIF_0 > DEA_0):
                       #BOLL中轨上轨向上
                       if(df_qfq['upper'][0]>df_qfq['upper'][1] and df_qfq['middle'][0]>df_qfq['middle'][1]):
                           #喇叭口形态
                           if(df_qfq['upper'][0]>df_qfq['upper'][1] and df_qfq['lower'][0]<df_qfq['lower'][1]):   
                               print (df_qfq['trade_date'][0],stockcode)
                               result_dict['ts_code'] = stockcode
                               result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                               result_dict['trigger_close'] = df_qfq['close'][0]
                               result_dict['reason'] = FNAME
                               result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df            
#get_macd_mainwave()

#N交易日个股日MACD金叉策略
def get_stocks_n_daily_macd_cross(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()      
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict = {}  
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n+1):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][n],4)
            DEA_1 = round(df_qfq['DEA'][n],4)
            DIF_0 = round(df_qfq['DIF'][n-1],4)
            DEA_0 = round(df_qfq['DEA'][n-1],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0):
                print (stockcode,df_qfq['trade_date'][n],df_qfq['close'][n],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][n]
                result_dict['trigger_close'] = df_qfq['close'][n]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#N交易日个股MACD金叉后出现二次金叉策略
def get_stocks_daily_macd_doublecross():
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()      
    df_monitor = get_col_param_df('monitorpoll','reason','get_stocks_n_daily_macd_cross')
    monitor_stockcode_list = df_monitor['ts_code'].tolist()
    for stockcode in monitor_stockcode_list:
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
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_0 > 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df                


#0轴上方金叉 换手率大于5
def get_stocks_daily_macd_cross_abovezreo_turnover_above():
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
            MACD_3 = round(df_qfq['MACD'][3],4)
            MACD_4 = round(df_qfq['MACD'][4],4)
            MACD_5 = round(df_qfq['MACD'][5],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            DIF_MIN = df_qfq['DIF'][0:60].min()
            #if (df_qfq['DIF'][0]==DIF_MIN):
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_0 > 0 and math.fabs(MACD_0)>math.fabs(MACD_1*3) and MACD_1<0 and MACD_2<0 and MACD_3<0 and MACD_4<0 and MACD_5<0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME
                result_df = result_df.append(result_dict,ignore_index=True)
    #换手率因子
    df_dailybasic = pro.daily_basic(ts_code='', trade_date='20201119', fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')
    result_df = pd.merge(result_df, df_dailybasic, how='left', on='ts_code')
    result_df = result_df[result_df['volume_ratio']>3]
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df
#df = get_stocks_daily_macd_cross_abovezreo_turnover_above()


#get_stocks_daily_macd_doublecross()
#get_stocks_n_daily_macd_cross(10)
#get_stocks_daily_macd_cross()
#get_stocks_weekly_macd()
#get_stocks_daily_macd()
#df = get_df_stockcode('daily_qfq_macd_000004.SZ')
#df = pro.weekly(ts_code='000001.SZ')
#df = df.sort_values(by="trade_date",ascending=True)
'''
#qfq
#获取前复权数据，默认为日期降序
df_qfq = ts.pro_bar(ts_code='000001.SZ', freq='W', adj='qfq')
#按照日期升序df
df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
#重新设置索引
df_qfq_asc.reset_index(drop=True, inplace=True)
#初始化CDMA df
df = pd.DataFrame()
df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
df['MACD'] = df['MACD'] * 2
#按照index反序
df = df.sort_index(ascending=False)
#重新设置索引
df.reset_index(drop=True, inplace=True)
#赋值series给df_qfq
df_qfq['DIF'] = df['DIF']
df_qfq['DEA'] = df['DEA']
df_qfq['MACD'] = df['MACD']
DIF_1 = round(df_qfq['DIF'][1],4)
DEA_1 = round(df_qfq['DEA'][1],4)
DIF_0 = round(df_qfq['DIF'][0],4)
DEA_0 = round(df_qfq['DEA'][0],4)
MACD_0 = round(df_qfq['MACD'][0],4)
MACD_1 = round(df_qfq['MACD'][1],4)
print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
'''

'''
#macd=DIF，signal=DEA，hist=BAR

#df_macd = get_MACD(df)
#df_macd = cal_macd_system(df,12,26,9)
#macd=DIF，signal=DEA，hist=BAR
#df['DIF2'] = df_macd['DIF']
'''