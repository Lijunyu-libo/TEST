# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 08:44:35 2020
回测框架DEMO
@author: iFunk
"""
import os
import pandas as pd
import json
import datetime
import time
import numpy as np
# MONGODB CONNECT
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["dailytest"]
#TUSHARE
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取历史交易日
trade_cal = pro.trade_cal(is_open='1',fileds='cal_date',start_date=get_day_time(180), end_date=get_day_time(1))
trade_cal.to_csv('trade_cal_'+today+'.csv')
df_trade_cal=pd.read_csv('trade_cal_'+today+'.csv')   
#print (df_trade_cal)
CASH=500
START_DATE=20191231
END_DATE=20200709



class Context:
    def __init__(self,cash,start_date,end_date):
        self.cash=cash
        self.start_date=start_date
        self.end_date=end_date
        self.positions={}
        self.daterange=df_trade_cal[(df_trade_cal['cal_date']>=start_date)&(df_trade_cal['cal_date']<=end_date)]['cal_date'].values
#print (Context(100,20191231,20200709).daterange)
ctx=Context(CASH,START_DATE,END_DATE)
#ORDER
def order(st_code,order_date,order_price,share):
    dict={}
    dict['order_date']=order_date
    dict['order_price']=order_price
    dict['share']=share
    ctx.positions[st_code]=dict


#获取回测时间段交易日数据
#获取数据集合
query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}}]} 
#query = { "$and": [{"trade_date":{"$gte":get_day_time(60)}},{"trade_date":{"$lte":get_day_time(1)}}]}
rs_daterange = mycollection.find(query)
#将查询结果转换为Df
df = pd.DataFrame(list(rs_daterange))

#策略函数
#计算前日线均值MA
def ma(closearray,nday):
    n=0
    mean_arr=[]
    #取n组数据
    while n<5:
        result_close = closearray[0+n:nday+n]
        mean_arr.append(np.mean(result_close))
        n+=1
    return mean_arr

#封装df_ma
df_ma = pd.DataFrame(columns=('ts_code','trade_date','close','preclose', 'ma5', 'ma10','ma20','ma30'))
for name,group in df.groupby(['ts_code']):
    close_arr=group['close'].tolist()
    preclose_arr=group['pre_close'].tolist()
    tradedate_arr = group['trade_date'].tolist()
    close_arr.reverse()
    preclose_arr.reverse()
    tradedate_arr.reverse()
    ma5=ma(close_arr,5)
    ma10=ma(close_arr,10)
    ma20=ma(close_arr,20)
    ma30=ma(close_arr,30)
    df_ma = df_ma.append([{'ts_code':name,'trade_date':tradedate_arr[0],'close':close_arr[0],'preclose':preclose_arr[0],'ma5':ma5,'ma10':ma10,'ma20':ma20,'ma30':ma30}], ignore_index=False)

#输出策略结果
# N(5,10,20,30)日均线值 连续T日持续上扬 日线T日刚突破5日线
n=0
ratioarr1 = []
ratioarr2 = []
ratioarr3 = []
ratioarr4 = []
for row in df_ma.iterrows():
    #print (row[1]['ma5'])
    if (row[1]['ma5'][0]>row[1]['ma5'][1]>row[1]['ma5'][2]>row[1]['ma5'][3]>row[1]['ma5'][4]):
        if (row[1]['ma10'][0]>row[1]['ma10'][1]>row[1]['ma10'][2]>row[1]['ma10'][3]>row[1]['ma10'][4]):
            if (row[1]['ma20'][0]>row[1]['ma20'][1]>row[1]['ma20'][2]>row[1]['ma20'][3]>row[1]['ma20'][4]):
                if (row[1]['ma30'][0]>row[1]['ma30'][1]>row[1]['ma30'][2]>row[1]['ma30'][3]>row[1]['ma30'][4]):
                    if(row[1]['close']>row[1]['ma5'][0] and row[1]['preclose']<row[1]['ma5'][0]):
                        print (n,row[1]['ts_code'],row[1]['trade_date'],row[1]['preclose'],row[1]['close'],row[1]['ma5'][0]/row[1]['ma5'][4])
                        ratioarr1.append(row[1]['ma5'][0]/row[1]['ma5'][1])
                        ratioarr2.append(row[1]['ma10'][0]/row[1]['ma10'][4])
                        ratioarr3.append(row[1]['ma20'][0]/row[1]['ma20'][1])
                        ratioarr4.append(row[1]['ma30'][0]/row[1]['ma30'][1])
                        n=n+1
print(np.mean(ratioarr1),np.mean(ratioarr2),np.mean(ratioarr3),np.mean(ratioarr4))


