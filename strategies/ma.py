# -*- coding: utf-8 -*-
"""
Spyder Editor
计算所有股票MA
This is a temporary script file.
"""
import pandas as pd
import numpy as np
import datetime
import time
import talib
import math
import inspect
# MONGODB CONNECT
from pymongo import MongoClient
import json
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
mycollection=mydb["stocks_daily"]
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

#获取正在运行函数(或方法)名称
def get__function_name():
    return inspect.stack()[1][3]

#获取stockcode条件集合函数 参数 stockcode 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

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

#计算MA函数
def cal_ma(df,nday):
    temp_serise = df['close'].rolling(nday).mean()
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise
def cal_ma_ta(df,n):
    temp_serise = talib.MA(df['close'],timeperiod=n)
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise
#MA
def get_ma_n(stockcode):
    rs_stockcode = mycollection.find({'ts_code':stockcode})
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    if (df_stockcode.empty):
        print (stockcode)
    else:
        df_stockcode['MA5'] = cal_ma_ta(df_stockcode,5)
        df_stockcode['MA10'] = cal_ma_ta(df_stockcode,10)
        df_stockcode['MA20'] = cal_ma_ta(df_stockcode,20)
        df_stockcode['MA30'] = cal_ma_ta(df_stockcode,30)
    return df_stockcode

#GET ALL MA
def get_ma_all():
    mycollection=mydb["stocks_daily"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode
#粘合算法,返回粘合系数列表
def cal_stick(df,param1,param2,n):
    stick_list = []
    i = 0
    while i<n:
        w = math.fabs((df[param1][i]-df[param2][i])/df[param2][i])
        stick_list.append(w)
        i +=1
    #stick_series = pd.Series(stick_list)    
    return stick_list
#粘合MA20算法,返回粘合系数列表
def cal_stick_ma20(df,n):
    resultflag = False
    #以第0根MA20线开始计算，获取第0根MA20线开始到第n根MA20线
    ma20_max = max(df['MA20'][0:n])
    ma20_min = min(df['MA20'][0:n])
    ma20_avg = df['MA20'][0:n].mean()
    ma30_max = max(df['MA30'][0:n])
    ma30_min = min(df['MA30'][0:n])
    ma30_avg = df['MA30'][0:n].mean()
    #获取MA20最高振幅和最低振幅
    ma20_max_r = ma20_max/ma20_avg
    ma20_min_r = ma20_min/ma20_avg
    ma30_max_r = ma30_max/ma30_avg
    ma30_min_r = ma30_min/ma30_avg
    if (ma30_max_r < ma20_max_r and ma30_min_r < ma20_min_r):
        resultflag = True
    #返回多个结果
    return resultflag

#粘合MA20算法,返回粘合系数列表
def cal_stick_ma20_up(df,n):
    resultflag = False
    ma5_0 = df['MA5'][0]
    ma5_1 = df['MA5'][1]
    ma10_0 = df['MA10'][0]
    ma10_1 = df['MA10'][1]
    ma20_0 = df['MA20'][0]
    ma20_1 = df['MA20'][1]
    #以第0根MA20线开始计算，获取第0根MA20线开始到第n根MA20线
    ma20_max = max(df['MA20'][0:n])
    ma20_min = min(df['MA20'][0:n])
    ma20_avg = df['MA20'][0:n].mean()
    ma30_max = max(df['MA30'][0:n])
    ma30_min = min(df['MA30'][0:n])
    ma30_avg = df['MA30'][0:n].mean()
    #获取MA20最高振幅和最低振幅
    ma20_max_r = ma20_max/ma20_avg
    ma20_min_r = ma20_min/ma20_avg
    ma30_max_r = ma30_max/ma30_avg
    ma30_min_r = ma30_min/ma30_avg
    if (ma30_max_r < ma20_max_r and ma30_min_r < ma20_min_r):
        if (ma10_1<ma20_1 and ma10_0>ma20_0):
            resultflag = True
    #返回多个结果
    return resultflag

#MA走势算法
def cal_ma_up(df,n):
    resultflag = False
    ma5_0 = df['MA5'][0]
    ma5_1 = df['MA5'][1]
    ma10_0 = df['MA10'][0]
    ma10_1 = df['MA10'][1]
    ma20_0 = df['MA20'][0]
    ma20_1 = df['MA20'][1]
    close_0 = df['close'][0]
    close_1 = df['close'][1]
    ma5_avg = df['MA5'][0:n].mean()
    ma5_max = max(df['MA5'][0:n])
    if (ma20_1<ma20_0 and ma10_1<ma10_0):
        if (close_0<ma5_max):
            resultflag = True
    #返回多个结果
    return resultflag    
#UP算法,返回true/false
def cal_up(df,param1,param2,n,step):
    up_list = []
    i = 0
    while i<n:
        w = df[param1][i]-df[param2][i+step]
        up_list.append(w)
        i +=1
    #stick_series = pd.Series(stick_list)    
    return up_list

#MA粘合策略 粘合MA20算法
def get_ma_stick_ma20(df,n):
    result_list = []
    df_ma = get_ma_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n+30):
            #print (name)
            continue
        else:
            df_group['MA5'] = cal_ma_ta(df_group,5)
            df_group['MA10'] = cal_ma_ta(df_group,10)
            df_group['MA20'] = cal_ma_ta(df_group,20)
            df_group['MA30'] = cal_ma_ta(df_group,30)  
            resultflag = cal_ma_up(df_group,n)
            if (resultflag):
                result_list.append(name)
                print (name,df_group['trade_date'][0])
    print (len(result_list))
    
#MA粘合策略
def get_ma_stick(df):
    w = 0.01
    result_list = []
    df_ma = get_ma_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<40):
            print (name)
        else:
            print (name,len(df_group))
            df_group['MA5'] = cal_ma_ta(df_group,5)
            df_group['MA10'] = cal_ma_ta(df_group,10)
            df_group['MA20'] = cal_ma_ta(df_group,20)
            df_group['MA30'] = cal_ma_ta(df_group,30)  
        stick_w_5_10 = cal_stick(df_ma,'MA5','MA10',5)
        stick_w_10_20 = cal_stick(df_ma,'MA10','MA20',5)
        stick_w_20_30 = cal_stick(df_ma,'MA20','MA30',5)
        if (max(stick_w_5_10)<=w):
            if (max(stick_w_10_20)<=w):
                if (max(stick_w_20_30)<=w):
                    result_list.append(name)
                    print (name,' ', max(stick_w_5_10),' ',max(stick_w_10_20),' ',max(stick_w_20_30))
    df_daily = pro.daily(trade_date=get_lasttradedate(0))
    if (df_daily.empty):
        df_daily = pro.daily(trade_date=get_lasttradedate(1))
    df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    if (df_daily_basic.empty):
        df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    df_result = df_daily_basic[df_daily_basic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    rundate = datetime.datetime.now().strftime('%Y%m%d')
    mycollection=mydb['ma_stick'+rundate]
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)
    return result

def get_ma_ma5upma10(df):
    up = 0
    result_list = []
    df_ma = get_ma_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<40):
            print (name)
        else:
            print (name,len(df_group))
            df_group['MA5'] = cal_ma_ta(df_group,5)
            df_group['MA10'] = cal_ma_ta(df_group,10)
            up_w_5_10 = cal_up(df_group,'MA5','MA10',2,0)
            if (up_w_5_10[0]>up and up_w_5_10[1]<up):
                result_list.append(name)
                print ('OK: ',name,' ',up_w_5_10)
    df_daily = pro.daily(trade_date=get_lasttradedate(0))
    if (df_daily.empty):
        df_daily = pro.daily(trade_date=get_lasttradedate(1))
    df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    if (df_daily_basic.empty):
        df_daily_basic = pro.daily_basic(ts_code='', trade_date=get_lasttradedate(0))
    df_result = df_daily_basic[df_daily_basic.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    rundate = datetime.datetime.now().strftime('%Y%m%d')
    mycollection=mydb['ma_ma5upma10'+rundate]
    mycollection.remove()
    records = json.loads(result.T.to_json()).values()
    mycollection.insert(records)
    return result               


#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#df = get_stockbasket('','')
#get_ma_stick_ma20(df,10)
      
#N最近交易日MA10ma20ma30粘合MACDDIF0轴上方策略
def get_stocks_daily_n_ma_stick_macd_diff_abovezero(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    W_MIN = 0.997
    W_MAX = 1
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=60+n):
            continue
        else:
            df_qfq['MA5'] = cal_ma_ta(df_qfq,5)
            df_qfq['MA10'] = cal_ma_ta(df_qfq,10)
            df_qfq['MA20'] = cal_ma_ta(df_qfq,20)
            df_qfq['MA30'] = cal_ma_ta(df_qfq,30)
            df_qfq['MA60'] = cal_ma_ta(df_qfq,60)
            MA10_N = df_qfq['MA10'][n]
            MA20_N = df_qfq['MA20'][n]
            MA30_N = df_qfq['MA30'][n]
            MA60_N = df_qfq['MA60'][n]
            MA_10_20_N_SQRT = math.sqrt(MA10_N*MA20_N)
            MA_20_30_N_SQRT = math.sqrt(MA20_N*MA30_N)
            MA_30_60_N_SQRT = math.sqrt(MA30_N*MA60_N)
            W_10_20_N = MA_10_20_N_SQRT/MA30_N
            W_20_30_N = MA_20_30_N_SQRT/MA30_N
            W_30_60_N = MA_30_60_N_SQRT/MA30_N
            if (W_MAX>W_10_20_N>W_MIN and W_MAX>W_20_30_N>W_MIN):
            #if (W_MAX>W_20_30_N>W_MIN and W_MAX>W_30_60_N>W_MIN):
                #if (df_qfq['MA5'][n-1]>df_qfq['MA10'][n-1]):
                if (True):
                    DIF_1 = round(df_qfq['DIF'][n+1],4)
                    DEA_1 = round(df_qfq['DEA'][n+1],4)
                    DIF_0 = round(df_qfq['DIF'][n],4)
                    DEA_0 = round(df_qfq['DEA'][n],4)
                    #if(True):
                    #if (df_qfq['MACD'][n-2]>0 and df_qfq['MACD'][n]<df_qfq['MACD'][n-1]<df_qfq['MACD'][n-2]):
                    if (DIF_0 > 0):
                    #if(df_qfq['MA20'][0]>df_qfq['MA20'][1]>df_qfq['MA20'][2]):
                        print (stockcode,df_qfq['trade_date'][n],df_qfq['close'][n],W_10_20_N,W_20_30_N)
                        result_dict['ts_code'] = stockcode
                        result_dict['trigger_trade_date'] = df_qfq['trade_date'][n]
                        result_dict['trigger_close'] = df_qfq['close'][n]
                        result_dict['reason'] = FNAME
                        result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df
#get_stocks_daily_n_ma_stick(0)

#N最近交易日MA20上升MACD0轴上方金叉策略
def get_stocks_daily_n_ma_up_macd_cross_abovezero(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()    
    df_stocks = get_stockbasket('','')
    n_range = 3
    ma_up_list = []
    close_up_list = []
    for stockcode in df_stocks['ts_code']:
        result_dict = {}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<=60+n):
            continue
        else:
            df_qfq['MA5'] = cal_ma_ta(df_qfq,5)
            df_qfq['MA10'] = cal_ma_ta(df_qfq,10)
            df_qfq['MA16'] = cal_ma_ta(df_qfq,16)
            df_qfq['MA20'] = cal_ma_ta(df_qfq,20)
            df_qfq['MA30'] = cal_ma_ta(df_qfq,30)
            df_qfq['MA60'] = cal_ma_ta(df_qfq,60)
            df_qfq['MA120'] = cal_ma_ta(df_qfq,120)     
            if (df_qfq['MA20'][n]==df_qfq['MA20'][n:n+n_range].max()):
                if (df_qfq['DIF'][n]>=0):
                #if (df_qfq['close'][n+1]<df_qfq['MA16'][n+1] and df_qfq['close'][n]>df_qfq['MA16'][n]):
                    if (df_qfq['DIF'][n]>df_qfq['DEA'][n] and df_qfq['DIF'][n+1]<df_qfq['DEA'][n+1]):
                        ma_up_list.append(stockcode)
                        if (df_qfq['high'][0:n-1].max()>df_qfq['close'][n]):
                            profit_pct = round(df_qfq['high'][0:n-1].max()/df_qfq['close'][n],2)
                            close_up_list.append(profit_pct)
                            print (stockcode,df_qfq['trade_date'][n],df_qfq['trade_date'][0],profit_pct)
                            result_dict['ts_code'] = stockcode
                            result_dict['trigger_trade_date'] = df_qfq['trade_date'][n]
                            result_dict['trigger_close'] = df_qfq['close'][n]
                            result_dict['reason'] = FNAME
                            result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('PERCENT',len(ma_up_list),len(close_up_list),round(len(close_up_list)/len(ma_up_list),2),min(close_up_list),np.mean(close_up_list),max(close_up_list))
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df

#get_stocks_daily_n_ma_up(6)