# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020
#个股行情入库
@author: 李博
"""
import tushare as ts
import pandas as pd
import numpy as np
import talib
from getData import cal_index_basic
import datetime
import time
#获取日线前复权数据模块
#import toMongodb_stocks_daily
#获取最新每日指标数据模块
#import toMongodb_dailybasic_last
#获取个股资金流向
#import toMongodb_moneyflow
#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(30), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday
#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#TRADELIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    lasttradeday_list.reverse()
    return lasttradeday_list

#exchange SSE上交所 SZSE深交所
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#DAILY
def get_eachstock_daily_qfq_indexs():
    STARTTIME = time.time()
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #CHECK
        df_check = get_col_df('daily_qfq_macd_'+stockcode)
        if (df_check.empty):
            #定义文档名称
            mycol = mydb['daily_qfq_macd_'+stockcode]            
            mycol.remove()
            #获取前复权数据，默认为日期降序
            #df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
            #df_nfq = ts.pro_bar(ts_code=stockcode)
            i = 0
            while i < 3:
                try:
                    df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                    break
                except:
                    print ('PAUSE WAITING 60S TO RECONNECTION')
                    time.sleep(60)
                    i += 1            
            #不符合计算MACD条件
            if (df_qfq is None or len(df_qfq)<2):
                continue
            #计算指标
            else:            
                #MA
                df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                #MACD
                df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                #KDJ
                df_qfq = cal_index_basic.get_kdj(df_qfq)
                #BOLL
                df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                #CCI
                df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                #MTM
                df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                #OBV
                df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                #VRSI
                df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                
                #插入数据
                mycol.insert_many(df_qfq.to_dict('records'))
                result_list.append(stockcode)
                print (df_qfq['trade_date'][0],'daily_'+stockcode+': '+str(len(df_qfq)))        
        else:
            col_trade_date = df_check['trade_date'][0]
            current_trade_date =  get_lasttradedate(0)       
            if (col_trade_date == current_trade_date):
                print (col_trade_date,col_trade_date,stockcode,'DATA ALREADY')
            else:
                #定义文档名称
                mycol = mydb['daily_qfq_macd_'+stockcode]            
                mycol.remove()
                #获取前复权数据，默认为日期降序
                #df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                #df_nfq = ts.pro_bar(ts_code=stockcode)
                i = 0
                while i < 3:
                    try:
                        df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                        break
                    except:
                        print ('PAUSE WAITING 60S TO RECONNECTION')
                        time.sleep(60)
                        i += 1                  
                #不符合计算MACD条件
                if (df_qfq is None or len(df_qfq)<2):
                    continue
                #计算指标
                else:            
                    #MA
                    df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                    df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                    df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                    df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                    #MACD
                    df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                    #KDJ
                    df_qfq = cal_index_basic.get_kdj(df_qfq)
                    #BOLL
                    df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                    #CCI
                    df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                    #MTM
                    df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                    #OBV
                    df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                    #VRSI
                    df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                    
                    #插入数据
                    mycol.insert_many(df_qfq.to_dict('records'))
                    result_list.append(stockcode)
                    print (df_qfq['trade_date'][0],'daily_'+stockcode+': '+str(len(df_qfq)))
    ENDTIME = time.time()
    print (len(result_list),ENDTIME-STARTTIME)
    #return df_qfq

#WEEKLY
def get_eachstock_weekly_qfq_indexs():
    STARTTIME = time.time()
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #CHECK
        df_check = get_col_df('weekly_qfq_macd_'+stockcode)
        if (1):
        #if (df_check.empty):
            #定义文档名称
            mycol = mydb['weekly_qfq_macd_'+stockcode]            
            mycol.remove()
            #获取前复权数据，默认为日期降序
            #df_qfq = ts.pro_bar(ts_code=stockcode,freq='W',adj='qfq')
            #df_nfq = ts.pro_bar(ts_code=stockcode,freq='W')
            i = 0
            while i < 3:
                try:
                    df_qfq = ts.pro_bar(ts_code=stockcode, freq='W',adj='qfq')
                    break
                except:
                    print ('PAUSE WAITING 60S TO RECONNECTION')
                    time.sleep(60)
                    i += 1            
            #不符合计算MACD条件
            if (df_qfq is None or len(df_qfq)<2):
                continue
            #计算指标
            else:            
                #MA
                df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                #MACD
                df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                #KDJ
                df_qfq = cal_index_basic.get_kdj(df_qfq)
                #BOLL
                df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                #CCI
                df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                #MTM
                df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                #OBV
                df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                #VRSI
                df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                
                #插入数据
                mycol.insert_many(df_qfq.to_dict('records'))
                result_list.append(stockcode)
                print (df_qfq['trade_date'][0],'weekly_'+stockcode+': '+str(len(df_qfq)))        
        else:
            col_trade_date = df_check['trade_date'][0]
            current_trade_date =  get_lasttradedate(0)       
            if (col_trade_date == current_trade_date):
                print (col_trade_date,col_trade_date,stockcode,'DATA ALREADY')
            else:
                #定义文档名称
                mycol = mydb['weekly_qfq_macd_'+stockcode]            
                mycol.remove()
                #获取前复权数据，默认为日期降序
                #df_qfq = ts.pro_bar(ts_code=stockcode,freq='W',adj='qfq')
                #df_nfq = ts.pro_bar(ts_code=stockcode,freq='W')
                i = 0
                while i < 3:
                    try:
                        df_qfq = ts.pro_bar(ts_code=stockcode,freq='W',adj='qfq')
                        break
                    except:
                        print ('PAUSE WAITING 60S TO RECONNECTION')
                        time.sleep(60)
                        i += 1                  
                #不符合计算MACD条件
                if (df_qfq is None or len(df_qfq)<2):
                    continue
                #计算指标
                else:            
                    #MA
                    df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                    df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                    df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                    df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                    #MACD
                    df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                    #KDJ
                    df_qfq = cal_index_basic.get_kdj(df_qfq)
                    #BOLL
                    df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                    #CCI
                    df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                    #MTM
                    df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                    #OBV
                    df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                    #VRSI
                    df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                    
                    #插入数据
                    mycol.insert_many(df_qfq.to_dict('records'))
                    result_list.append(stockcode)
                    print (df_qfq['trade_date'][0],'weekly_'+stockcode+': '+str(len(df_qfq)))
    ENDTIME = time.time()
    print (len(result_list),ENDTIME-STARTTIME)
    #return df_qfq
    
#MONTHLY
def get_eachstock_monthly_qfq_indexs():
    STARTTIME = time.time()
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #CHECK
        df_check = get_col_df('monthly_qfq_macd_'+stockcode)
        if (df_check.empty):
            #定义文档名称
            mycol = mydb['monthly_qfq_macd_'+stockcode]            
            mycol.remove()
            #获取前复权数据，默认为日期降序
            #df_qfq = ts.pro_bar(ts_code=stockcode,freq='W',adj='qfq')
            #df_nfq = ts.pro_bar(ts_code=stockcode,freq='W')
            i = 0
            while i < 3:
                try:
                    df_qfq = ts.pro_bar(ts_code=stockcode, freq='M',adj='qfq')
                    break
                except:
                    print ('PAUSE WAITING 60S TO RECONNECTION')
                    time.sleep(60)
                    i += 1            
            #不符合计算MACD条件
            if (df_qfq is None or len(df_qfq)<2):
                continue
            #计算指标
            else:            
                #MA
                df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                #MACD
                df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                #KDJ
                df_qfq = cal_index_basic.get_kdj(df_qfq)
                #BOLL
                df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                #CCI
                df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                #MTM
                df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                #OBV
                df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                #VRSI
                df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                
                #插入数据
                mycol.insert_many(df_qfq.to_dict('records'))
                result_list.append(stockcode)
                print (df_qfq['trade_date'][0],'monthly_'+stockcode+': '+str(len(df_qfq)))        
        else:
            col_trade_date = df_check['trade_date'][0]
            current_trade_date =  get_lasttradedate(0)       
            if (col_trade_date == current_trade_date):
                print (col_trade_date,col_trade_date,stockcode,'DATA ALREADY')
            else:
                #定义文档名称
                mycol = mydb['monthly_qfq_macd_'+stockcode]            
                mycol.remove()
                #获取前复权数据，默认为日期降序
                #df_qfq = ts.pro_bar(ts_code=stockcode,freq='W',adj='qfq')
                #df_nfq = ts.pro_bar(ts_code=stockcode,freq='W')
                i = 0
                while i < 3:
                    try:
                        df_qfq = ts.pro_bar(ts_code=stockcode,freq='M',adj='qfq')
                        break
                    except:
                        print ('PAUSE WAITING 60S TO RECONNECTION')
                        time.sleep(60)
                        i += 1                  
                #不符合计算MACD条件
                if (df_qfq is None or len(df_qfq)<2):
                    continue
                #计算指标
                else:            
                    #MA
                    df_qfq['MA5'] = cal_index_basic.get_ma(df_qfq,5)
                    df_qfq['MA10'] = cal_index_basic.get_ma(df_qfq,10)
                    df_qfq['MA20'] = cal_index_basic.get_ma(df_qfq,20)
                    df_qfq['MA30'] = cal_index_basic.get_ma(df_qfq,30)
                    #MACD
                    df_qfq = cal_index_basic.get_macd_talib_macdext(df_qfq)
                    #KDJ
                    df_qfq = cal_index_basic.get_kdj(df_qfq)
                    #BOLL
                    df_qfq = cal_index_basic.get_boll_talib_bbands(df_qfq)
                    #CCI
                    df_qfq = cal_index_basic.get_cci_talib_cci(df_qfq)
                    #MTM
                    df_qfq = cal_index_basic.get_mtm_talib_mom(df_qfq)
                    #OBV
                    df_qfq = cal_index_basic.get_obv_talib_OBV(df_qfq)
                    #VRSI
                    df_qfq = cal_index_basic.get_vrsi_talib_RSI(df_qfq)                    
                    #插入数据
                    mycol.insert_many(df_qfq.to_dict('records'))
                    result_list.append(stockcode)
                    print (df_qfq['trade_date'][0],'monthly_'+stockcode+': '+str(len(df_qfq)))
    ENDTIME = time.time()
    print (len(result_list),ENDTIME-STARTTIME)
    #return df_qfq    