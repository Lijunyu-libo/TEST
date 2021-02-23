# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 10:31:10 2020
策略执行基本模块
@author: iFunk
"""
import pandas as pd
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

#获取目标股票篮子
#exchange 交易所 SSE上交所 SZSE深交所 
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#print (get_stockbasket('SZSE','中小板'))
