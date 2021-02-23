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
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()

from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
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

def get_stockbasket_sinarule_exchange(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    if (exchange == 'SSE'):
        data['sina'] = 'sh'+ data['symbol']
    else:
        data['sina'] = 'sz'+ data['symbol']
    return data['sina'].tolist()

#df = get_stockbasket_sinarule_exchange('SZSE','')

#调用新浪api
def get_sina_data(shortname,stockcode):
    content=requests.get('http://hq.sinajs.cn/?format=json&list='+shortname+stockcode).text
    list = content.split(',')
    #print (content)
    #第四列为当前价格
    #print (list[3])
    return list
#stocklist = ['sz000001','sz000004','sz000001','sz000004','sz000001','sz000004','sz000001','sz000004','sz000001','sz000004']

def get_sina_data_sh_df():
    start = time.time()
    i=1
    stocklist_sse = get_stockbasket_sinarule('SSE','')   
    df_result = pd.DataFrame()
    for item in stocklist_sse:
        stock_data = get_sina_data('sh',item)
        ts_code = item+'.SH'
        trade_date = stock_data[30]
        trade_time = stock_data[31]
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        amount_str = stock_data[9]#元
        vol_str = stock_data[8]#股
        #数据格式处理        
        openprice_float = round(float(stock_data[2]),2)
        lastprice_float = round(float(stock_data[3]),2)
        highprice_float = round(float(stock_data[4]),2)
        lowprice_float = round(float(stock_data[5]),2)
        amount_float = round(float(stock_data[9]),0)
        vol_float = round(float(stock_data[8]),0)
        #ratio = round(float(lastprice)/float(openprice)-1,2)
        print (i,trade_date,trade_time,ts_code)
        #if (lastprice_str == highprice_str):
            #if(ratio>0):
            #print (trade_date,trade_time,ts_code,openprice_str,lastprice_str,highprice_str,lowprice_str,amount_str)
            #print (trade_date,trade_time,ts_code,openprice_float,lastprice_float,highprice_float,lowprice_float,vol_float,amount_float)
        #封装数据         
        result_dict = {}
        result_dict['ts_code'] = ts_code
        result_dict['realtime_trade_date'] = trade_date
        result_dict['realtime_trade_time'] = trade_time
        result_dict['realtime_open'] = openprice_float
        result_dict['realtime_close'] = lastprice_float
        result_dict['realtime_high'] = highprice_float
        result_dict['realtime_low'] = lowprice_float
        result_dict['realtime_amount'] = amount_float
        result_dict['realtime_vol'] = vol_float        
        df_result = df_result.append(result_dict,ignore_index=True)
        i+=1
    end = time.time()
    #输出时间
    print (len(df_result),end-start)
    return df_result



#调用新浪api 批量获取行情
def get_batch_sina_data_sh_df():
    start = time.time()
    #i=1
    stocklist_sse = get_stockbasket_sinarule_exchange('SSE','')
    stocklist_batch = [stocklist_sse[i:i+200] for i in range(0,len(stocklist_sse),200)]
    df_result = pd.DataFrame()
    for stocklist_item in stocklist_batch:
        stocklist_string = str(stocklist_item).strip("[]''")
        stocklist_string = stocklist_string.replace("', '",",")
        content=requests.get('http://hq.sinajs.cn/?format=json&list='+stocklist_string).text
        content = content.replace('var hq_json_','')
        #分割个股
        data_list = content.split(';')
        #去掉末尾无效数据
        data_list = data_list[0:-1]
        #print (data_list)
        data_df = pd.DataFrame()
        for data_item in data_list:
            #分割个股内行情数据        
            stock_data = data_item.split(',')
            stock_codename = stock_data[0].split('=')
            stock_code = str(stock_codename[0]).replace('sh','').strip()+'.SH'
            stock_name = str(stock_codename[1])
            trade_date = stock_data[30]
            trade_time = stock_data[31]
            openprice_str = stock_data[2]
            lastprice_str = stock_data[3]
            highprice_str = stock_data[4]
            lowprice_str = stock_data[5]
            amount_str = stock_data[9]#元
            vol_str = stock_data[8]#股
            #数据格式处理        
            openprice_float = round(float(stock_data[2]),2)
            lastprice_float = round(float(stock_data[3]),2)
            highprice_float = round(float(stock_data[4]),2)
            lowprice_float = round(float(stock_data[5]),2)
            amount_float = round(float(stock_data[9]),0)
            vol_float = round(float(stock_data[8]),0)
            #ratio = round(float(lastprice)/float(openprice)-1,2)
            #print (stock_code,stock_name,trade_date,vol_float)
            #封装数据         
            result_dict = {}
            result_dict['ts_code'] = stock_code
            result_dict['stock_name'] = stock_name
            result_dict['realtime_trade_date'] = trade_date
            result_dict['realtime_trade_time'] = trade_time
            result_dict['realtime_open'] = openprice_float
            result_dict['realtime_close'] = lastprice_float
            result_dict['realtime_high'] = highprice_float
            result_dict['realtime_low'] = lowprice_float
            result_dict['realtime_amount'] = amount_float
            result_dict['realtime_vol'] = vol_float        
            data_df = data_df.append(result_dict,ignore_index=True)
        print (data_df['ts_code'][0],data_df['realtime_trade_date'][0],data_df['realtime_trade_time'][0],len(data_df))
        df_result = df_result.append(data_df,ignore_index=True)
    end = time.time()
    #输出时间
    print ('SH',len(df_result),end-start)
    return df_result
#df_sh = get_batch_sina_data_sh_df()

def get_sina_data_sz_df():
    start = time.time()
    i=1
    stocklist_sse = get_stockbasket_sinarule('SZSE','')   
    df_result = pd.DataFrame()
    for item in stocklist_sse:
        stock_data = get_sina_data('sz',item)
        ts_code = item+'.SZ'
        trade_date = stock_data[30]
        trade_time = stock_data[31]
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        amount_str = stock_data[9]#元
        vol_str = stock_data[8]#股
        #数据格式处理        
        openprice_float = round(float(stock_data[2]),2)
        lastprice_float = round(float(stock_data[3]),2)
        highprice_float = round(float(stock_data[4]),2)
        lowprice_float = round(float(stock_data[5]),2)
        amount_float = round(float(stock_data[9]),0)
        vol_float = round(float(stock_data[8]),0)
        #ratio = round(float(lastprice)/float(openprice)-1,2)
        print (i,trade_date,trade_time,ts_code)
        #if (lastprice_str == highprice_str):
            #if(ratio>0):
            #print (trade_date,trade_time,ts_code,openprice_str,lastprice_str,highprice_str,lowprice_str,amount_str)
            #print (trade_date,trade_time,ts_code,openprice_float,lastprice_float,highprice_float,lowprice_float,vol_float,amount_float)
        #封装数据         
        result_dict = {}
        result_dict['ts_code'] = ts_code
        result_dict['realtime_trade_date'] = trade_date
        result_dict['realtime_trade_time'] = trade_time
        result_dict['realtime_open'] = openprice_float
        result_dict['realtime_close'] = lastprice_float
        result_dict['realtime_high'] = highprice_float
        result_dict['realtime_low'] = lowprice_float
        result_dict['realtime_amount'] = amount_float
        result_dict['realtime_vol'] = vol_float        
        df_result = df_result.append(result_dict,ignore_index=True)
        i+=1        
    end = time.time()
    #输出时间
    print (len(df_result),end-start)
    return df_result


def test():
    stocklist_sse = get_stockbasket_sinarule_exchange('SZSE','')
    stocklist_batch = [stocklist_sse[i:i+200] for i in range(0,len(stocklist_sse),200)]
    for stocklist_item in stocklist_batch:
        stocklist_string = str(stocklist_item).strip("[]''")    
        stocklist_string = stocklist_string.replace("', '",",")
        content=requests.get('http://hq.sinajs.cn/?format=json&list='+stocklist_string).text
        content = content.replace('var hq_json_','')
        #分割个股
        data_list = content.split(';')
        #去掉末尾无效数据
        data_list = data_list[0:-1]
        for data_item in data_list:
            stock_data = data_item.split(',')
            stock_codename = stock_data[0].split('=')
            stock_code = str(stock_codename[0]).replace('sz','')+'.SZ'
            stock_name = str(stock_codename[1])
            trade_date = stock_data[30]
            trade_time = stock_data[31]
            print (stock_code,stock_name,trade_date,trade_time)

#test()

#调用新浪api 批量获取行情
def get_batch_sina_data_sz_df():
    start = time.time()
    #i=1
    stocklist_sse = get_stockbasket_sinarule_exchange('SZSE','')
    stocklist_batch = [stocklist_sse[i:i+200] for i in range(0,len(stocklist_sse),200)]
    df_result = pd.DataFrame()
    for stocklist_item in stocklist_batch:
        stocklist_string = str(stocklist_item).strip("[]''")
        stocklist_string = stocklist_string.replace("', '",",")
        content=requests.get('http://hq.sinajs.cn/?format=json&list='+stocklist_string).text
        content = content.replace('var hq_json_','')
        #分割个股
        data_list = content.split(';')
        #去掉末尾无效数据
        data_list = data_list[0:-1]
        #print (data_list)
        data_df = pd.DataFrame()
        for data_item in data_list:
            #分割个股内行情数据        
            stock_data = data_item.split(',')
            stock_codename = stock_data[0].split('=')
            stock_code = str(stock_codename[0]).replace('sz','').strip()+'.SZ'
            stock_name = str(stock_codename[1])
            trade_date = stock_data[30]
            trade_time = stock_data[31]
            openprice_str = stock_data[2]
            lastprice_str = stock_data[3]
            highprice_str = stock_data[4]
            lowprice_str = stock_data[5]
            amount_str = stock_data[9]#元
            vol_str = stock_data[8]#股
            #数据格式处理        
            openprice_float = round(float(stock_data[2]),2)
            lastprice_float = round(float(stock_data[3]),2)
            highprice_float = round(float(stock_data[4]),2)
            lowprice_float = round(float(stock_data[5]),2)
            amount_float = round(float(stock_data[9]),0)
            vol_float = round(float(stock_data[8]),0)
            #ratio = round(float(lastprice)/float(openprice)-1,2)
            #print (stock_code,stock_name,trade_date,vol_float)
            #封装数据         
            result_dict = {}
            result_dict['ts_code'] = stock_code
            result_dict['stock_name'] = stock_name
            result_dict['realtime_trade_date'] = trade_date
            result_dict['realtime_trade_time'] = trade_time
            result_dict['realtime_open'] = openprice_float
            result_dict['realtime_close'] = lastprice_float
            result_dict['realtime_high'] = highprice_float
            result_dict['realtime_low'] = lowprice_float
            result_dict['realtime_amount'] = amount_float
            result_dict['realtime_vol'] = vol_float        
            data_df = data_df.append(result_dict,ignore_index=True)
        print (data_df['ts_code'][0],data_df['realtime_trade_date'][0],
               data_df['realtime_trade_time'][0],len(data_df))
        df_result = df_result.append(data_df,ignore_index=True)
    end = time.time()
    #输出时间
    print ('SZ',len(df_result),end-start)
    return df_result
#df_sz = get_batch_sina_data_sz_df()


#自定义列表获取行情
#TEXT TO LIST
def get_txt_list():
    with open('..\\data\\test_boll_20201217.txt','r') as f:
        text_data = f.read()
        text_list = eval(text_data)
        print (len(text_list),text_list)
    return text_list

def get_sina_data_selflist_df():
    start = time.time()
    i=1
    stocklist = get_txt_list()   
    df_result = pd.DataFrame()
    for item in stocklist:
        shortitem = item.rstrip('.SHZ')
        if (shortitem.startswith('6')):
            stock_data = get_sina_data('sh',shortitem)
        else:
            stock_data = get_sina_data('sz',shortitem)
        ts_code = item
        trade_date = stock_data[30]
        trade_time = stock_data[31]
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        amount_str = stock_data[9]#元
        vol_str = stock_data[8]#股
        #数据格式处理        
        openprice_float = round(float(stock_data[2]),2)
        lastprice_float = round(float(stock_data[3]),2)
        highprice_float = round(float(stock_data[4]),2)
        lowprice_float = round(float(stock_data[5]),2)
        amount_float = round(float(stock_data[9]),0)
        vol_float = round(float(stock_data[8]),0)
        #ratio = round(float(lastprice)/float(openprice)-1,2)
        print (i,trade_date,trade_time,ts_code)
        #if (lastprice_str == highprice_str):
            #if(ratio>0):
            #print (trade_date,trade_time,ts_code,openprice_str,lastprice_str,highprice_str,lowprice_str,amount_str)
            #print (trade_date,trade_time,ts_code,openprice_float,lastprice_float,highprice_float,lowprice_float,vol_float,amount_float)
        #封装数据         
        result_dict = {}
        result_dict['ts_code'] = ts_code
        result_dict['realtime_trade_date'] = trade_date
        result_dict['realtime_trade_time'] = trade_time
        result_dict['realtime_open'] = openprice_float
        result_dict['realtime_close'] = lastprice_float
        result_dict['realtime_high'] = highprice_float
        result_dict['realtime_low'] = lowprice_float
        result_dict['realtime_amount'] = amount_float
        result_dict['realtime_vol'] = vol_float        
        df_result = df_result.append(result_dict,ignore_index=True)
        i+=1        
    end = time.time()
    #输出时间
    print (len(df_result),end-start)
    return df_result

def get_sina_data_selflist_list():
    start = time.time()
    i=1
    stocklist = get_txt_list()   
    list_result = []
    for item in stocklist:
        shortitem = item.rstrip('.SHZ')
        if (shortitem.startswith('6')):
            stock_data = get_sina_data('sh',shortitem)
        else:
            stock_data = get_sina_data('sz',shortitem)
        ts_code = item
        trade_date = stock_data[30]
        trade_time = stock_data[31]
        openprice_str = stock_data[2]
        lastprice_str = stock_data[3]
        highprice_str = stock_data[4]
        lowprice_str = stock_data[5]
        amount_str = stock_data[9]#元
        vol_str = stock_data[8]#股
        #数据格式处理        
        openprice_float = round(float(stock_data[2]),2)
        lastprice_float = round(float(stock_data[3]),2)
        highprice_float = round(float(stock_data[4]),2)
        lowprice_float = round(float(stock_data[5]),2)
        amount_float = round(float(stock_data[9]),0)
        vol_float = round(float(stock_data[8]),0)
        ratio = round(lastprice_float/openprice_float-1,2)
        print (i,trade_date,trade_time,ts_code)
        #封装数据         
        result_dict = {}
        result_dict['ts_code'] = ts_code
        result_dict['realtime_trade_date'] = trade_date
        result_dict['realtime_trade_time'] = trade_time
        result_dict['realtime_open'] = openprice_float
        result_dict['realtime_close'] = lastprice_float
        result_dict['realtime_high'] = highprice_float
        result_dict['realtime_low'] = lowprice_float
        result_dict['realtime_ratio'] = ratio 
        result_dict['realtime_amount'] = amount_float
        result_dict['realtime_vol'] = vol_float
        list_result.append(['MONITOR',result_dict])
        if (lastprice_str == highprice_str and ratio > 0):
            #if(ratio>0):
            #print (trade_date,trade_time,ts_code,openprice_str,lastprice_str,highprice_str,lowprice_str,amount_str)
            #print (trade_date,trade_time,ts_code,openprice_float,lastprice_float,highprice_float,lowprice_float,vol_float,amount_float)      
            list_result.append(['CLOSE',result_dict])
        else:
           list_result.append(['MONITOR',result_dict]) 
        i+=1        
    end = time.time()
    #输出时间
    print (len(list_result),end-start)
    return list_result

def save_realtime_data_selflist():
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print (the_date_str)
    df_self = get_sina_data_selflist_df()
    #定义文档名称
    mycol = mydb['realtime_daily_data_self']
    mycol.remove()
    mycol.insert_many(df_self.to_dict('records'))    
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print (the_date_str)
#save_realtime_data_selflist()

def txt_realtime_data_selflist():
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d')
    the_datetime_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print (the_datetime_str)
    list_self = get_sina_data_selflist_list()
    #定义文档名称
    with open("..\\data\\test_log_"+the_date_str+".txt","a") as f:
        for i in list_self:
            f.write(str(i) + "\n")
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print ('W TXT',the_date_str)

def save_realtime_data_all():
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print ('START',the_date_str)
    #df_sh = get_sina_data_sh_df()
    #df_sz = get_sina_data_sz_df()
    df_sh = get_batch_sina_data_sh_df()
    df_sz = get_batch_sina_data_sz_df()    
    #定义文档名称
    mycol = mydb['realtime_daily_data']
    mycol.remove()
    print ('DATA REMOVE')
    mycol.insert_many(df_sh.to_dict('records'))
    print ('SH DATA MONGODB READY')
    mycol.insert_many(df_sz.to_dict('records'))
    print ('SZ DATA MONGODB READY')
    the_date = datetime.datetime.now()
    the_date_str = the_date.strftime('%Y%m%d %H:%M:%S')
    print ('END',the_date_str)


#inital
save_realtime_data_all()
#txt_realtime_data_selflist()
sched.add_job(save_realtime_data_all, 'interval', seconds=45)
#sched.add_job(save_realtime_data_all, 'interval', minutes=10)
#sched.add_job(save_realtime_data_selflist, 'interval', seconds=20)
#sched.add_job(txt_realtime_data_selflist, 'interval', seconds=20)
sched.start()
