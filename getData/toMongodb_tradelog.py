# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 11:01:24 2021
#交割单入库模块
@author: libo
"""
import pandas as pd
import json
import datetime
import time

#入库
from pymongo import MongoClient
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]

def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

def toMongo_tradelog():
    mycollection=mydb['tradelog']
    #mycollection.drop()
    path_df=open('../data/trade_20201201_20210110.csv') 
    df_csv = pd.read_csv(path_df)
    records = json.loads(df_csv.T.to_json()).values()
    mycollection.insert_many(records)
    df_result = get_col_df('tradelog')
    return df_result
#df = toMongo_tradelog()

