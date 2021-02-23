# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: iFunk
"""

from typing import Optional

from starlette.requests import Request
from fastapi import FastAPI
from starlette.templating import Jinja2Templates
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["swl1"]
rs = mycollection.find()
#print (rs.count())
rs_json = []
for i in rs:
    rs_json.append(i)
    print (i)
app = FastAPI()

# 挂载模版文件夹
tmp = Jinja2Templates(directory='templates')

@app.get("/hello,world")
def read_root():
    return {"Hello": "World"}

@app.get('/')
async def get_tmp(request:Request):  # async加了就支持异步  把Request赋值给request
    return tmp.TemplateResponse('index.html',
                                {'request':request,  # 一定要返回request
                                 'args':len(rs_json),  # 额外的参数可有可无
                                 'kw':rs_json})

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)