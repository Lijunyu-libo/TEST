# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
获取PEG
@author: 李博
"""
import os
import pandas as pd
import json
import datetime
import time

# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
today=time.strftime('%Y%m%d',)
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

#获取最新交易日
df = pro.trade_cal(exchange='SSE', is_open='1',start_date=get_day_time(5), end_date=get_day_time(1))
lasttradedate = df['cal_date'].tail(1).iloc[0]

#获取PE值
df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry')
df_dailybasic_temp = pro.daily_basic(ts_code='', trade_date=lasttradedate,fields='ts_code,pe,pe_ttm')
df_dailybasic = df_dailybasic_temp.dropna(axis=0, how='any')  # 去掉所有有空值的行
df_pe = pd.merge(df_dailybasic,df_stockbasic, how='left', on='ts_code')

#获取某季度所有股票利润表EPS函数
def get_period_income(enddate):    
    df_income =  pd.DataFrame()
    df_income = pro.income_vip(period=enddate, fields='ts_code,ann_date,f_ann_date,end_date,basic_eps')
    #df_income.to_csv(enddate+'_income.csv')
    #print (df_income)
    return df_income
periods = ['20200331','20200630']
#periods = ['20191231','20200331']

def get_g(periods):
    #获取EPS
    df_eps_all = pd.DataFrame()
    
    for period in periods:
        df_temp = get_period_income(period)    
        df_eps_all = df_eps_all.append(df_temp,ignore_index=True)
    df_eps_dup = df_eps_all.drop_duplicates(subset = ['ts_code','f_ann_date','end_date'])
    
    #获取G
    df_eps = pd.DataFrame()
    for name, group in df_eps_dup.groupby('ts_code'):
        enddate_arr=group['end_date'].tolist()
        eps_arr=group['basic_eps'].tolist()
        if(len(enddate_arr)>1):
            g = (eps_arr[1]-eps_arr[0])/abs(eps_arr[0])     
            df_eps_temp = df_eps.append([{'ts_code':name,'end_date_pre':enddate_arr[0],'eps_pre':eps_arr[0],'end_date':enddate_arr[1],'eps':eps_arr[1],'G':g}], ignore_index=False)
            df_eps = df_eps_temp.dropna(axis=0, how='any')
    #计算PEG 市盈率/收益增长率
    df_PEG =  pd.merge(df_eps,df_pe, how='left', on='ts_code')
    df_PEG = df_PEG.dropna(axis=0, how='any')
    df_PEG['PEG']=df_PEG[['pe_ttm','G']].apply(lambda x:x["pe_ttm"]/(x["G"]*100) ,axis=1)
    df_PEG.to_csv(periods[1]+'_peg.csv') 

get_g(['20200331','20200630'])
get_g(['20191231','20200331'])
#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv','r',encoding='UTF-8') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#执行入库
toMongodb('20200630_peg','20200630_peg')
toMongodb('20200331_peg','20200331_peg')