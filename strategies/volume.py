# -*- coding: utf-8 -*-
"""
Spyder Editor
成交量策略
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
#client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["stocks_daily"]
import tushare as ts
ts.set_token('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
pro = ts.pro_api()

def get__function_name():
    '''获取正在运行函数(或方法)名称'''
    return inspect.stack()[1][3]
#获取col数据库函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode
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
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(0), end_date=get_day_time(n))
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

# VOL MA
def cal_ma_ta_vol(df,n):
    temp_serise = talib.MA(df['vol'],timeperiod=n)
    temp_serise.dropna(inplace=True)
    ma_serise = temp_serise.reset_index(drop=True)
    return ma_serise

# param MA
def cal_ma_ta_param(df,param,n):
    temp_serise = talib.MA(df[param],timeperiod=n)
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

#GET ALL DAILY
def get_daily_all():
    mycollection=mydb["stocks_daily_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode

#GET DAILYBASIC
def get_dailybasic():
    mycollection=mydb["dailybasic_last"]
    rs_dailybasic = mycollection.find()
    list_dailybasic = list(rs_dailybasic)
    #将查询结果转换为Df
    df_dailybasic = pd.DataFrame(list_dailybasic)
    return df_dailybasic

#GET ALL WEEKLY
def get_weekly_all():
    mycollection=mydb["stocks_weekly_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
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

#低位算法,返回True/False
def cal_low(df,param1,n,step):
    lowflag = False
    n_max = max(df[param1][0:n])
    n_1 = df.at[n+step,param1]
    if (n_max < n_1):
        lowflag = True   
    return lowflag

#圆底算法1,返回True/False
def cal_round_low(df,param1,n,step):
    roundlowflag = False
    #获取n日最高点
    n_max = max(df[param1][1:n])
    n_avg = df[param1][1:n].mean()
    n_naddstep = df.at[n+step,param1]
    n_0 = df.at[0,param1]
    #判断n日未超过前期高点
    if (n_max < n_naddstep):
        #判断最近交易日高于前期平均值
        if(n_0>n_avg):
            roundlowflag = True   
    return roundlowflag

#圆底算法2，连续3日增加,返回True/False
def cal_round_low2(df,param1,n,step):
    roundlowflag = False
    #获取n日最高点
    n_max = max(df[param1][1:n])
    n_naddstep = df.at[n+step,param1]
    n_0 = df.at[0,param1]
    n_1 = df.at[1,param1]
    n_2 = df.at[2,param1]
    #判断n日未超过前期高点
    if (n_max < n_naddstep):
        #判断最近交易日高于前期平均值
        if(n_0>n_1 and n_1>n_2):
            roundlowflag = True   
    return roundlowflag
    
#交易日成交量圆弧底形态策略
def get_daily_vol_round_low(df,n):
    df_dailybasic_last = get_dailybasic()
    result_list = []
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n+2):
            continue
        else:
            resultflag = cal_round_low2(df_group,'vol',n,1)
            if (resultflag):
                print (name,df_group['trade_date'][0])
                result_list.append(name)
    print (len(result_list))
    df_result = df_dailybasic_last[df_dailybasic_last.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    return result

#交易日成交量股价同步圆弧底形态策略
def get_daily_vol_close_round_low(df,n):
    df_dailybasic_last = get_dailybasic()
    result_list = []
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n+2):
            continue
        else:
            resultflag_vol = cal_round_low2(df_group,'vol',n,1)
            resultflag_close = cal_round_low2(df_group,'close',n,1)
            if (resultflag_vol and resultflag_close):
                print (name,df_group['trade_date'][0],df_group['close'][0],df_group['vol'][0])
                result_list.append(name)
    df_result = df_dailybasic_last[df_dailybasic_last.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    print (len(result_list))
    return result

#交易日均线上升成交量股价同步圆弧底形态策略
def get_daily_ma_up_vol_close_round_low(df,n):
    df_dailybasic_last = get_dailybasic()
    result_list = []
    df_ma = get_daily_all()
    df_ma_gb_stockcode = df_ma.groupby('ts_code')
    for name,group in df_ma_gb_stockcode:
        df_group=pd.DataFrame(group)
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group.reset_index(drop=True, inplace=True)
        if (df_group.empty or len(df_group)<n+2 or len(df_group)<34):
            continue
        else:
            df_group['MA5'] = cal_ma_ta(df_group,5)
            df_group['MA10'] = cal_ma_ta(df_group,10)
            df_group['MA20'] = cal_ma_ta(df_group,20)
            df_group['MA30'] = cal_ma_ta(df_group,30)
            if (df_group['MA30'][0]>max(df_group['MA30'][1:3]) and df_group['MA20'][0]>max(df_group['MA20'][1:3])):
                resultflag_vol = cal_round_low2(df_group,'vol',n,1)
                resultflag_close = cal_round_low2(df_group,'close',n,1)
                if (resultflag_vol and resultflag_close):
                    print (name,df_group['trade_date'][0],df_group['close'][0],df_group['vol'][0])
                    result_list.append(name)
    df_result = df_dailybasic_last[df_dailybasic_last.ts_code.isin(result_list)]
    result = pd.merge(df, df_result, how='right', on=['ts_code'])
    print (len(result_list))
    return result
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

def get_stockbasket_nochuang(exchange):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    data = data[~ data['market'].str.contains('科创板|创业板')]
    data['ts_name'] = data['name']
    return data

#n日成交量最低位阶段性低点策略
def get_stocks_daily_n_vol_low(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict={}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n):
            continue
        else:
            vol_n_min = df_qfq['vol'][0:n].min()
            if(df_qfq['vol'][0]==vol_n_min):
                print (stockcode,df_qfq['trade_date'][0])
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME+str(n)
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df  
#df = get_stocks_daily_n_vol_low(20)

#最近40交易日盘整后成交量天量股价拉升换手高策略
def get_stocks_daily_n_close_low_vol_close_up_max(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()  
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict={}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n):
            continue
        else:
            close_min_n_1 = df_qfq['close'][1:n].min()
            close_avg_n_1 = df_qfq['close'][1:n].mean()
            close_max_n = df_qfq['close'][0:n].max()
            vol_max_n = df_qfq['vol'][0:n].max()
            if(df_qfq['vol'][0]==vol_max_n and df_qfq['close'][0]==close_max_n):
                if(close_avg_n_1>df_qfq['close'][1]>close_min_n_1):
                    print (stockcode,df_qfq['trade_date'][0])
                    result_dict['ts_code'] = stockcode
                    result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                    result_dict['trigger_close'] = df_qfq['close'][0]
                    result_dict['reason'] = FNAME+str(n)
                    result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df    
#get_stocks_daily_n_close_low_vol_close_up(40)

#最近N交易日天量天价高换手策略
def get_stocks_daily_n_turnover_up_vol_close_up(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame() 
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict={}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n):
            continue
        else:
            close_max_n = df_qfq['close'][0:n].max()
            vol_max_n = df_qfq['vol'][0:n].max()
            if(df_qfq['vol'][0]==vol_max_n and df_qfq['close'][0]==close_max_n):
                df = pro.daily_basic(ts_code=stockcode)
                turnover_n = df['turnover_rate_f'][0]
                chg_pct_n = df_qfq['pct_chg'][0]
                if(turnover_n>20 and chg_pct_n>3):
                    print (stockcode,df_qfq['trade_date'][0],turnover_n,chg_pct_n)
                    result_dict['ts_code'] = stockcode
                    result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                    result_dict['trigger_close'] = df_qfq['close'][0]
                    result_dict['reason'] = FNAME+str(n)
                    result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df   
#get_stocks_daily_n_turnover_up_vol_close_up(10)

#n日成交量最低位阶段性低点策略
def get_stocks_daily_n_vol_close_low(sn,vn,cn):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict={}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<sn+vn):
            continue
        else:
            vol_n_min = df_qfq['vol'][sn:sn+vn].min()
            close_n_min = df_qfq['close'][sn:sn+cn].min()
            if(df_qfq['vol'][sn]==vol_n_min and df_qfq['close'][sn]==close_n_min):
                close_diff_ratio = round((df_qfq['close'][0]-df_qfq['close'][sn])/df_qfq['close'][sn],3)
                #print (stockcode,df_qfq['trade_date'][sn],df_qfq['trade_date'][0],df_qfq['close'][sn],df_qfq['close'][0])
                result_dict['close_diff_ratio'] = close_diff_ratio 
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME+str(vn)
                result_df = result_df.append(result_dict,ignore_index=True)
    #print ('RESULT',result_df['close_diff_ratio'].mean())
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #print ('END',FNAME,len(result_df),TIMENOW)
    return result_df  
#df = get_stocks_daily_n_vol_close_low(1,20,80)
def testdemo(n,vn,cn):
    i=1
    while i<n:
        print (i,vn,cn)
        df = get_stocks_daily_n_vol_close_low(i,vn,cn)
        close_up_ratio = round(len(df[df['close_diff_ratio']>0])/len(df),2)        
        print ('涨跌统计：',close_up_ratio,'  涨跌幅统计：',round(df['close_diff_ratio'].mean(),3),round(df['close_diff_ratio'].max(),3),round(df['close_diff_ratio'].min(),3))
        i+=1
#testdemo(30,20,80)
#testdemo(30,30,80)
#testdemo(30,40,80)
#testdemo(30,50,80)
#testdemo(30,60,80)
#testdemo(30,70,80)
#testdemo(30,80,80)

#n周成交量最低位阶段性低点策略
def get_stocks_weekly_n_vol_close_low(n):
    FNAME = get__function_name()
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('START',FNAME,TIMENOW)
    result_df=pd.DataFrame()
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        result_dict={}
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('weekly_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<n):
            continue
        else:
            vol_n_min = df_qfq['vol'][0:n].min()
            close_n_min = df_qfq['close'][0:10].min()
            if(df_qfq['vol'][0]==vol_n_min and df_qfq['close'][0]==close_n_min):
                print (stockcode,df_qfq['trade_date'][0])
                result_dict['ts_code'] = stockcode
                result_dict['trigger_trade_date'] = df_qfq['trade_date'][0]
                result_dict['trigger_close'] = df_qfq['close'][0]
                result_dict['reason'] = FNAME+str(n)
                result_df = result_df.append(result_dict,ignore_index=True)
    TIMENOW = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print ('END',FNAME,len(result_df),TIMENOW)
    return result_df  
#df = get_stocks_weekly_n_vol_close_low(10)
#df = get_stockbasket('SSE','主板')
#get_daily_vol_close_round_low(df,5)      

#每笔均量计算DEMO
def get_amount_vol_demo(stockcode):
    df = get_df_stockcode('daily_qfq_macd_'+stockcode)
    df['amount_vol'] = round(df['amount']/df['vol'],3)
    return df
#df = get_amount_vol_demo('000001.SZ')