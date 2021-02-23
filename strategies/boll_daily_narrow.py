# -*- coding: utf-8 -*-
"""
Created on Wed Jul  8 13:31:35 2020
BOLL收窄
@author: iFunk
"""
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams['axes.unicode_minus'] = False
#查询库
from pymongo import MongoClient
client = MongoClient('mongodb://112.12.60.2:27017')
mydb=client["ptest"]
mycollection=mydb["dailytest"]

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    #print(the_date_str)
    #print(pre_date_str)
    return int(pre_date_str)

#获取数据集合
#START_DATE=20200701
#END_DATE=20200707
#query = { "$and": [{"trade_date":{"$gte":START_DATE}},{"trade_date":{"$lte":END_DATE}}]} 
query = { "$and": [{"trade_date":{"$gte":get_day_time(120)}},{"trade_date":{"$lte":get_day_time(1)}}]}
rs_daterange = mycollection.find(query)

#将查询结果转换为Df
df = pd.DataFrame(list(rs_daterange))

#计算日线均前n日曲线
def ma(closearray,nday):
    n=0
    mean_arr=[]
    #取n组数据
    while n<15:
        result_close = closearray[0+n:nday+n]
        mean_arr.append(round(np.mean(result_close),2))
        n+=1
    return mean_arr

#计算BOLL线上轨前n曲线
def boll_up(closearray,nday):
    n=0
    boll_up_arr=[]
    #取n组数据
    while n<15:
        result_close = closearray[0+n:nday+n]
        boll_up_arr.append(round(np.mean(result_close)+1.8*np.std(result_close,ddof=1),2))
        n+=1
    return boll_up_arr

#计算前BOLL线下轨前n曲线
def boll_down(closearray,nday):
    n=0
    boll_down_arr=[]
    #取n组数据
    while n<15:
        result_close = closearray[0+n:nday+n]
        boll_down_arr.append(round(np.mean(result_close)-1.8*np.std(result_close,ddof=1),2))
        n+=1
    return boll_down_arr

#封装df_boll
df_boll = pd.DataFrame(columns=('ts_code','close','preclose', 'ma5', 'ma10','ma20','ma30','bollup','bolldown'))
for name,group in df.groupby(['ts_code']):
    close_arr=group['close'].tolist()
    preclose_arr=group['pre_close'].tolist()
    close_arr.reverse()
    preclose_arr.reverse()
    ma5=ma(close_arr,5)
    ma10=ma(close_arr,10)
    ma20=ma(close_arr,20)
    ma30=ma(close_arr,30)
    bollup=boll_up(close_arr,20)
    bolldown=boll_down(close_arr,20)
    df_boll = df_boll.append([{'ts_code':name,'close':close_arr[0],'preclose':preclose_arr[0],'ma5':ma5,'ma10':ma10,'ma20':ma20,'ma30':ma30,'bollup':bollup,'bolldown':bolldown}], ignore_index=False)
#print (df_boll)

'''
m=1
#BOLL连续2日开口
for row in df_boll.iterrows():
    #print (row[1]['ts_code'],row[1]['bolldown'],row[1]['ma20'],row[1]['bollup'])
    if (row[1]['bollup'][0]<row[1]['bollup'][1]<row[1]['bollup'][2]):
        if (row[1]['bolldown'][0]>row[1]['bolldown'][1]>row[1]['bolldown'][2]):
            if (row[1]['ma20'][0]>row[1]['ma20'][1]):
                print (m,row[1]['ts_code'],row[1]['close'])
                m=m+1
                mean_df = pd.DataFrame()
                row[1]['bolldown'].reverse()
                row[1]['ma20'].reverse()
                row[1]['bollup'].reverse()
                mean_df["20MA"]=row[1]['ma20']
                mean_df["bollup"]=row[1]['bollup']
                mean_df["bolldown"]=row[1]['bolldown']
                print (mean_df)
                plt.title(u'计算BOLL线') 
                plt.xlabel(u'日') 
                plt.ylabel(u'股价')
                #plt.plot(mean_df)
                mean_df["bolldown"].plot()
                mean_df["bollup"].plot()
                mean_df["20MA"].plot()
                plt.legend(loc="upper left") 
                plt.show()
'''

#BOLL上轨与下轨与当前中轨的偏离值小于w
w=0.05
m=1
for row in df_boll.iterrows():
    if ((row[1]['bollup'][1]-row[1]['ma20'][1])/row[1]['ma20'][1]<=w):
         if ((row[1]['bollup'][2]-row[1]['ma20'][2])/row[1]['ma20'][2]<=w):
              if ((row[1]['bollup'][3]-row[1]['ma20'][3])/row[1]['ma20'][3]<=w):
                  if ((row[1]['bollup'][3]-row[1]['ma20'][3])/row[1]['ma20'][3]<=w):
                      print (m,row[1]['ts_code'],row[1]['close'])
                      m=m+1
                      mean_df = pd.DataFrame()
                      row[1]['bolldown'].reverse()
                      row[1]['ma20'].reverse()
                      row[1]['bollup'].reverse()
                      mean_df["20MA"]=row[1]['ma20']
                      mean_df["bollup"]=row[1]['bollup']
                      mean_df["bolldown"]=row[1]['bolldown']
                      #print (mean_df)
                      plt.title(u'计算BOLL线 '+row[1]['ts_code']) 
                      plt.xlabel(u'日') 
                      plt.ylabel(u'股价')
                      #plt.plot(mean_df)
                      mean_df["bolldown"].plot()
                      mean_df["bollup"].plot()
                      mean_df["20MA"].plot()
                      plt.legend(loc="upper left")                       
                      #plt.savefig(row[1]['ts_code']+'pci.png') # 保存图片到本地
                      plt.show()
'''
# N(5,10,20,30)日均线值 连续T日持续上扬
for row in df_ma.iterrows():
    #print (row[1]['ma5'])
    if (row[1]['ma5'][0]>row[1]['ma5'][1]>row[1]['ma5'][2]>row[1]['ma5'][3]>row[1]['ma5'][4]):
        if (row[1]['ma10'][0]>row[1]['ma10'][1]>row[1]['ma10'][2]>row[1]['ma10'][3]>row[1]['ma10'][4]):
            if (row[1]['ma20'][0]>row[1]['ma20'][1]>row[1]['ma20'][2]>row[1]['ma20'][3]>row[1]['ma20'][4]):
                if (row[1]['ma30'][0]>row[1]['ma30'][1]>row[1]['ma30'][2]>row[1]['ma30'][3]>row[1]['ma30'][4]):
                    print (row[1]['ts_code'],row[1]['ma5'])

# N(5,10,20,30)日均线值 连续T日持续下跌
for row in df_ma.iterrows():
    #print (row[1]['ma5'])
    if (row[1]['ma5'][0]<row[1]['ma5'][1]<row[1]['ma5'][2]<row[1]['ma5'][3]<row[1]['ma5'][4]):
        print (row[1]['ts_code'],row[1]['ma5'])   

# N(5,10,20,30)日均线值 连续T日持续上扬 日线T日刚突破5日线
n=0
ratioarr1 = []
ratioarr2 = []
ratioarr3 = []
ratioarr4 = []
for row in df_boll.iterrows():
    #print (row[1]['ma5'])
    if (row[1]['ma5'][0]>row[1]['ma5'][1]>row[1]['ma5'][2]>row[1]['ma5'][3]>row[1]['ma5'][4]):
        if (row[1]['ma10'][0]>row[1]['ma10'][1]>row[1]['ma10'][2]>row[1]['ma10'][3]>row[1]['ma10'][4]):
            if (row[1]['ma20'][0]>row[1]['ma20'][1]>row[1]['ma20'][2]>row[1]['ma20'][3]>row[1]['ma20'][4]):
                if (row[1]['ma30'][0]>row[1]['ma30'][1]>row[1]['ma30'][2]>row[1]['ma30'][3]>row[1]['ma30'][4]):
                    if(row[1]['close']>row[1]['ma5'][0] and row[1]['preclose']<row[1]['ma5'][0]):
                        print (n,row[1]['ts_code'],row[1]['preclose'],row[1]['close'],row[1]['ma5'][0]/row[1]['ma5'][4])
                        ratioarr1.append(row[1]['ma5'][0]/row[1]['ma5'][1])
                        ratioarr2.append(row[1]['ma10'][0]/row[1]['ma10'][1])
                        ratioarr3.append(row[1]['ma20'][0]/row[1]['ma20'][1])
                        ratioarr4.append(row[1]['ma30'][0]/row[1]['ma30'][1])
                        n=n+1
print(np.mean(ratioarr1),np.mean(ratioarr2),np.mean(ratioarr3),np.mean(ratioarr4))


    result = df_ma[]
    #result = df_ma[(df_ma['close']>df_ma['ma5'][i][0])&( df_ma['close'] > df_ma['ma10'][0])&(df_ma['close'] > df_ma['ma20'][0])&(df_ma['close'] > df_ma['ma30'][0])]
    #np.array(a)
    print (result)
    '''