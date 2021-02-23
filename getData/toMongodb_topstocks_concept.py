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
df = pro.concept()
#print (df)
conceptstockslist = []
for i in df['code']:
    stockslist = getstocks(i)
    conceptstockslist.append(stockslist)
df['stocks'] = conceptstockslist
print (df)
df.to_csv('concept.csv')
 

#入库
from pymongo import MongoClient
print (os.getcwd())
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["concept"]
mycollection.remove()
path_df=open('concept.csv','r',encoding='UTF-8') 
df_csv = pd.read_csv(path_df)
records = json.loads(df_csv.T.to_json()).values()
#print (records)
mycollection.insert(records)
