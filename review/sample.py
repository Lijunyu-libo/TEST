# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 10:34:29 2020
样本数据扫描模块 sample
@author: iFunk
"""
import pandas as pd
import numpy as np
import datetime
from strategies import boll
# MONGODB CONNECT
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
#client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]

#GET ALL DAILY
def get_daily_all():
    mycollection=mydb["stocks_daily_qfq"]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    list_stockcode.reverse()
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    return df_stockcode

def get_sample_daterange(data,startdate,enddate):
    data_stockcode = data.groupby('ts_code')
    for name,group in data_stockcode:
        #GET GROUP
        df_group = pd.DataFrame(group)
        #df_group['trade_date'] = pd.to_datetime(df_group['trade_date'])
        df_group = df_group.sort_values(by="trade_date",ascending=False)
        df_group['trade_date'] = pd.DataFrame(df_group['trade_date'], dtype=np.datetime64)
        df_group = df_group.set_index(['trade_date'], drop=False, append=False, inplace=False, verify_integrity=False)
        if (df_group.empty or len(df_group)<0):
            #print (name,'','EMPTY OR <5')
            continue
        else:
            #if (df_group.loc[startdate] is not None or df_group.loc[enddate] is not None):
            df = df_group[enddate:startdate]
            print (name,df['trade_date'].tolist())
    '''
    keys是列标签或数组列表，
    drop：删除要用作新索引的列,布尔值默认为True，
    append：boolean是否将列附加到现有索引,默认为False，
    inplace修改DataFrame（不要创建新对象）默认为False，
    verify_integrity：检查新索引是否有重复项默认为False。
    '''
    return data

data = get_daily_all()
start_date = datetime.datetime.strptime('2020-08-01','%Y-%m-%d')
end_date = datetime.datetime.strptime('2020-09-01','%Y-%m-%d')
df = get_sample_daterange(data,start_date,end_date).head(1000)
