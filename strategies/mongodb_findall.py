# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 10:40:23 2020
查询所有交易信息
@author: Borisli
"""
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["dailytest"]
rs = mycollection.find()
print (rs.count())
for i in rs:
    print (i)