# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 09:07:53 2020
Redis demo
@author: iFunk
"""
import redis   # 导入redis 模块

#r = redis.Redis(host='localhost', port=6379, decode_responses=True)
pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)  
r.set('000001.SZ', 66.66)  # 设置 name 对应的值
r.set('000002.SZ',7.77)
if (r.get('000003.SZ') !=None):
    print(r.get('000003.SZ'))  # 取出键 name 对应的值
print(r.get('000001.SZ'))  # 查看类型
