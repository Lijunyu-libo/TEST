# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 10:40:23 2020
查询St_code条件下所有交易信息
@author: Borisli
"""
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
ST_CODE='603700.SH'
START_DATE=20200701
END_DATE=20200707
mydb=client["ptest"]
mycollection=mydb["dailytest"]
query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}},{'ts_code':ST_CODE}]} 
rs_daterange = mycollection.find(query)
print (rs_daterange.count())
for i in rs_daterange:
    print (i)