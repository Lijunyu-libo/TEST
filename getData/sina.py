# -*- coding: utf-8 -*-
"""
Created on Mon Dec  7 10:22:32 2020

@author: iFunk
"""
import requests
import time
import pandas as pd
import numpy as np
import datetime

import tushare as ts
pro = ts.pro_api('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
'''
接口：
http://hq.sinajs.cn/list=sh601006
这个url会返回一串文本，例如：
var hq_str_sh601006="大秦铁路, 27.55, 27.25, 26.91, 27.55, 26.20, 26.91, 26.92, 
22114263, 589824680, 4695, 26.91, 57590, 26.90, 14700, 26.89, 14300,
26.88, 15100, 26.87, 3100, 26.92, 8900, 26.93, 14230, 26.94, 25150, 26.95, 15220, 26.96, 2008-01-11, 15:05:32";
这个字符串由许多数据拼接在一起，不同含义的数据用逗号隔开了，按照程序员的思路，顺序号从0开始。
0：”大秦铁路”，股票名字；
1：”27.55″，今日开盘价；
2：”27.25″，昨日收盘价；
3：”26.91″，当前价格；
4：”27.55″，今日最高价；
5：”26.20″，今日最低价；
6：”26.91″，竞买价，即“买一”报价；
7：”26.92″，竞卖价，即“卖一”报价；
8：”22114263″，成交的股票数，由于股票交易以一百股为基本单位，所以在使用时，通常把该值除以一百；
9：”589824680″，成交金额，单位为“元”，为了一目了然，通常以“万元”为成交金额的单位，所以通常把该值除以一万；
10：”4695″，“买一”申请4695股，即47手；
11：”26.91″，“买一”报价；
12：”57590″，“买二”
13：”26.90″，“买二”
14：”14700″，“买三”
15：”26.89″，“买三”
16：”14300″，“买四”
17：”26.88″，“买四”
18：”15100″，“买五”
19：”26.87″，“买五”
20：”3100″，“卖一”申报3100股，即31手；
21：”26.92″，“卖一”报价
(22, 23), (24, 25), (26,27), (28, 29)分别为“卖二”至“卖四的情况”
30：”2008-01-11″，日期；
31：”15:05:32″，时间；
'''


#exchange SSE上交所 SZSE深交所
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket_sinarule(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data['symbol'].tolist()
#df = get_stockbasket_sinarule('SSE','')
#调用新浪api
def get_sina_data(stockcode):
    content=requests.get('http://hq.sinajs.cn/?format=json&list=sh'+stockcode).text
    list = content.split(',')
    #print (content)
    #第四列为当前价格
    #print (list[3])
    return list
#stocklist = ['sz000001','sz000004','sz000001','sz000004','sz000001','sz000004','sz000001','sz000004','sz000001','sz000004']

def get_sina_data_list():
    stocklist = get_stockbasket_sinarule('SSE','')
    start = time.time()
    for i in stocklist:
        stock_data = get_sina_data(i)
        amount = round(float(stock_data[9])/10000,1)
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        #ratio = round(float(lastprice)/float(openprice)-1,2)
        #print (stock_data[31],i,openprice,lastprice,amount)
        if (lastprice_str == highprice_str):
            #if(ratio>0):
            print (stock_data[31],i,lastprice_str,highprice_str,amount)
    end = time.time()
    #输出时间
    print (len(stocklist),end-start)

get_sina_data_list()