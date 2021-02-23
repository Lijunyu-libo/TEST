# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: iFunk
"""

from typing import Optional

from starlette.requests import Request
from fastapi import FastAPI
from starlette.templating import Jinja2Templates

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
                                 'args':'hello world',  # 额外的参数可有可无
                                 'kw':'88888888'
                                 }
                                )

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    return {"item_id": item_id, "q": q}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)