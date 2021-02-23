# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 10:40:23 2020
查询St_code条件下所有交易信息
@author: Borisli
"""
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
ST_CODE='688599.SH'
mydb=client["ptest"]
mycollection=mydb["dailytest"]
rs_stcode = mycollection.find({'ts_code':ST_CODE})
print (rs_stcode.count())
for i in rs_stcode:
    print (i)