# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 21:56:55 2020

@author: Libo
"""

from typing import Optional
from starlette.requests import Request
from fastapi import Depends, FastAPI, Header, HTTPException
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles
#系统主面板
from api import mainpanel
#数据量化主面板
from api import data_mainpanel
#指数日线分析
from api import index_daily
#市场分析
from api import market
#行业分析模块
from api import industry
#指数数据更新
from getData import toMongodb_daily_index
#SW指数日线分析
from api import indexs_sw_daily
#SW指数数据更新
from getData import toMongodb_sw_index
#概念数据更新
from getData import toMongodb_concept
#概念数据读取
from api import stocks_concept
#行业数据更新
#from getData import toMongodb_sw
#行业数据读取
from api import stocks_sw
#资金流向数据更新
from getData import toMongodb_moneyflow
#资金流向数据读取
from api import moneyflow
from api import stocks_moneyflow
#涨停数据更新
from getData import toMongodb_limit
#涨停数据读取
from api import stocks_limit
#业绩预告数据更新
from getData import toMongodb_fina_forecast
from api import stocks_forecast
#禁售股解禁数据更新
from getData import toMongodb_fina_float
from api import stocks_float
#最新日线数据更新
from getData import toMongodb_dailybasic_last
#个股日线数据更新
from getData import toMongodb_stocks_daily
from api import stock_daily
#个股每日基本信息
from api import stocks_daily
from api import stocks_dailybasic
#个股指标数据
from api import stock_index 
#下单模块
from api import order

#策略模块加载
#策略执行基本模块
from strategies import basicmodel
#from strategies import dispatch
#from strategies import stockbasket
#from strategies import moneyflow
#from strategies import moneyflowtoptogether

#模拟交易记录
from tradememo import tradememo_result

app = FastAPI()
# 挂载模版文件夹
tmp = Jinja2Templates(directory='./api/templates')
#配置静态文件目录
app.mount('/static', StaticFiles(directory='./api/templates/static'), name='static')

async def get_token_header(x_token: str = Header(...)):
    if x_token != "fake-super-secret-token":
        raise HTTPException(status_code=400, detail="X-Token header invalid")
#系统主面板
app.include_router(mainpanel.router)        
#指数数据
app.include_router(data_mainpanel.router)
#市场数据
app.include_router(market.router)
#市场数据
app.include_router(industry.router)
#指数数据
app.include_router(index_daily.router)
app.include_router(toMongodb_daily_index.router)
#SW指数数据
app.include_router(indexs_sw_daily.router)
app.include_router(toMongodb_sw_index.router)

#概念分类
app.include_router(stocks_concept.router)
app.include_router(toMongodb_concept.router)
#行业分类
app.include_router(stocks_sw.router)
#app.include_router(toMongodb_sw.router)
#资金流向
app.include_router(moneyflow.router)
app.include_router(stocks_moneyflow.router)
app.include_router(toMongodb_moneyflow.router)
#涨停
app.include_router(stocks_limit.router)
app.include_router(toMongodb_limit.router)
#业绩预告
app.include_router(stocks_forecast.router)
app.include_router(toMongodb_fina_forecast.router)
#解禁
app.include_router(stocks_float.router)
app.include_router(toMongodb_fina_float.router)
#个股日线
app.include_router(toMongodb_stocks_daily.router)
app.include_router(stock_daily.router)
app.include_router(stocks_daily.router)
#个股日基本交易信息
app.include_router(toMongodb_dailybasic_last.router)
app.include_router(stocks_dailybasic.router)
#个股指标信息
app.include_router(stock_index.router)
#策略运行结果
app.include_router(basicmodel.router)
#模拟交易结果
app.include_router(tradememo_result.router)
#下单信息
app.include_router(order.router)
'''
app.include_router(
    items.router,
    prefix="/items",
    tags=["items"],
    dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)
'''

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app=app,
                host="127.0.0.1",
                port=8000,
                workers=1)