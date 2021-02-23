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

# MONGODB CONNECT
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

#获取个股利润表数据函数
def get_stock_income(stockcode,startdate,enddate):    
    df_income = pro.income_vip(ts_code=stockcode, start_date=startdate, end_date=enddate, fields='ts_code,ann_date,f_ann_date,end_date,basic_eps,revenue,total_cogs,sell_exp,admin_exp,fin_exp,total_profit,n_income')
    #获取利润表数据
    #df = pro.income_vip(start_date='20180101', end_date='20200731',fields='ts_code,f_ann_date,end_date,basic_eps,revenue,total_cogs,sell_exp,admin_exp,fin_exp,total_profit,n_income')
    #df_income = df.drop_duplicates()#去除重复行
    df_income.to_csv(stockcode+'_income.csv')
    return df_income
'''
stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
stocks_dict =stocks.to_dict(orient='records')
#获取所有个利润表数据
for i in stocks_dict:
    df_income = get_stock_income(i['ts_code'],'20180101','20200731')
    print (df_income)
'''
#获取某季度所有股票利润表函数
def get_period_income(enddate):    
    df_income =  pd.DataFrame()
    df_income = pro.income_vip(period=enddate, fields='ts_code,ann_date,f_ann_date,end_date,basic_eps,revenue,total_cogs,sell_exp,admin_exp,fin_exp,total_profit,n_income')
    df_income.to_csv(enddate+'_income.csv')
    print (df_income)
    #return df_income

#get_period_income('20181231')

periods = ['20181231','20190331','20190630','20190930','20191231','20200331','20200630']
for period in periods:
    get_period_income(period)    

#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]

def toMongodb(collectionname,filename):
    mycollection=mydb[collectionname]
    mycollection.remove()
    path_df=open(filename+'.csv') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert(records)

#执行入库
for period in periods:
    toMongodb(period+'_income',period+'_income')

