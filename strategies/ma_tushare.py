# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei"]
plt.rcParams['axes.unicode_minus'] = False
# MONGODB CONNECT
import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    #print(the_date_str)
    #print(pre_date_str)
    return pre_date_str
#print(get_day_time(1))
#print(get_day_time(30))

#查询当前所有正常上市交易的股票列表
#data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
#arr=[]
#arr = data['ts_code']
#str=','.join(arr)
#print (str)
#df = pro.daily(ts_code=str, start_date='20200628', end_date='20200703')
ts_df = pro.daily(ts_code='000680.SZ',start_date=get_day_time(100), end_date=get_day_time(1))
mean_df = pd.DataFrame()
print (ts_df)
#计算日线均值MA
def ma(nday):
    n=0
    mean_arr=[]
    #取5组数据
    while n<60:
        result_close = ts_df[0+n:nday+n]['close']
        mean_arr.append(np.mean(result_close))
        n+=1
    #逆序排列，结果保存在原列表里
    mean_arr.reverse()
    return mean_arr

mean_df["5MA"]=ma(5)
mean_df["10MA"]=ma(10)
mean_df["20MA"]=ma(20)
mean_df["30MA"]=ma(30)
print (mean_df)
plt.title(u'计算日线均值MA') 
plt.xlabel(u'日') 
plt.ylabel(u'股价')
#plt.plot(mean_df)
mean_df["5MA"].plot()
mean_df["10MA"].plot()
mean_df["20MA"].plot()
mean_df["30MA"].plot()
plt.legend(loc="upper left") 
plt.show()