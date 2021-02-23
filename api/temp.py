# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020
测试路由文件
@author: iFunk
"""
import json
from io import StringIO
import demjson
from typing import Optional
import pandas as pd
from starlette.requests import Request
from fastapi import FastAPI
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#查询库

from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
#获取地区函数 参数
areacollection=mydb['stockbasic']
rs_area = areacollection.find()
#将查询结果转换为Df
df = pd.DataFrame(list(rs_area))
area_arr = []
for name,group in df.groupby(['area']):
    area_arr.append(name)
print (area_arr)

from fastapi import APIRouter
router = APIRouter()



#模板渲染    
#app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='templates')

@router.get('/stocks/area/')
async def get_tmp(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('area_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'count':len(area_arr),
                                 'areas':area_arr,
                                 'stockscount':0,  # 额外的参数可有可无
                                 'stocks':[]                                 
                                 })



# 获取index代码
@router.get('/index/{item_id}') 
async def get_item(item_id):
    return item_id

# 获取index代码
@router.get('/stocks/area/{areaname}') 
async def get_index_stocks(areaname,request:Request):
    query = { "area": areaname}
    df_stocks = areacollection.find(query)   
    rs_json = []
    for i in df_stocks:
        rs_json.append(i)
    #print (rs_json)
    return tmp.TemplateResponse('area_stocks.html',
                                {'request':request,  # 一定要返回request
                                 'count':len(area_arr),
                                 'areas':area_arr,
                                 'stockscount':len(rs_json),
                                 'stocks':rs_json,  
                                 'areaname':areaname
                                 })
'''
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)
    '''