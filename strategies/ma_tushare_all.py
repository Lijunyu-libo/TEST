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
mean_df = pd.DataFrame()
#df_tradedate = pd.DataFrame()
df_tushare = pd.DataFrame()
df_ma_5 = pd.DataFrame()

#获取历史交易日
df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(60), end_date=get_day_time(1))
#print (df['cal_date'])

#前日线均值MA
def ma(closearray,nday):
    n=0
    mean_arr=[]
    #取n组数据
    while n<5:
        result_close = closearray[0+n:nday+n]
        mean_arr.append(np.mean(result_close))
        n+=1
    return mean_arr

#获取日线数据
for i in df['cal_date']:
    df_tradedate = pro.daily(trade_date=i)
    df_tushare = df_tushare.append(df_tradedate,ignore_index=True)

df_ma = pd.DataFrame(columns=('ts_code','close', 'ma5', 'ma10','ma20','ma30'))
for name,group in df_tushare.groupby(['ts_code']):
    close_arr=group['close'].tolist()
    close_arr.reverse()
    #ma5=np.mean(close_arr[0:5])
    #ma10=np.mean(close_arr[0:10])
    #ma20=np.mean(close_arr[0:20])
    #ma30=np.mean(close_arr[0:30])
    ma5=ma(close_arr,5)
    ma10=ma(close_arr,10)
    ma20=ma(close_arr,20)
    ma30=ma(close_arr,30)
    df_ma = df_ma.append([{'ts_code':name,'close':close_arr[0],'ma5':ma5,'ma10':ma10,'ma20':ma20,'ma30':ma30}], ignore_index=True)
#print (df_ma)
#result = df_ma[(df_ma['close']>df_ma['ma5'][0])&( df_ma['close'] > df_ma['ma10'][0])&(df_ma['close'] > df_ma['ma20'][0])&(df_ma['close'] > df_ma['ma30'][0])]
result = df_ma['ts_code']
print (result)


'''
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
'''