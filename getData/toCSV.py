# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 15:40:01 2020
#保存到CSV文件工具模块
@author: iFunk
"""
import tushare as ts
import pandas as pd
import numpy as np
import talib
import datetime

#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()

def get_stocksbasic_industry_list():
    #查询当前所有正常上市交易的股票列表
    df_stocksbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_groupby_industry = df_stocksbasic.groupby('industry')
    df_result = pd.DataFrame()
    index_code = 1
    for name,group in df_groupby_industry:        
        result_dict = {}
        #GET GROUP
        df_group = pd.DataFrame(group)
        result_dict['index_code'] = str(index_code)
        result_dict['industry_name'] = name
        result_dict['stockslist_count'] = str(len(df_group))
        result_dict['stockslist'] = df_group['ts_code'].tolist()
        #result_dict['industry_stockname_list'] = df_group['name'].tolist()
        df_result = df_result.append(result_dict,ignore_index=True)
        index_code += 1
    #保存CSV文档
    df_result.to_csv('../data/industry/industry.csv')
    #CHECK
    df=pd.read_csv('../data/industry/industry.csv',sep=',') #filename可以直接从盘符开始，标明每一级的文件夹直到csv文件，header=None表示头部为空，sep=' '表示数据间使用空格作为分隔符，如果分隔符是逗号，只需换成 ‘，'即可。
    for index, row in df.iterrows():
        list_result = row['stockslist']
        list_result = list_result.strip("[]")
        list_result = list_result.split(', ')
        itemlist = []
        for item in list_result:
            item = item.strip("''")
            itemlist.append(item)
            #print (item)
        print (row['index_code'],row['industry_name'],len(itemlist),itemlist[0:2])    
    print ('stocksbasic_industry_list:'+str(len(df_result)))
    return df_result
#df = get_stocksbasic_industry_list()
#自选行业输出个股列表函数
def get_stocksbasic_industry_indexcode(indexcode_list):
    result_list = []
    df=pd.read_csv('../data/industry/industry.csv',sep=',') #filename可以直接从盘符开始，标明每一级的文件夹直到csv文件，header=None表示头部为空，sep=' '表示数据间使用空格作为分隔符，如果分隔符是逗号，只需换成 ‘，'即可。
    df = df[df.index_code.isin(indexcode_list)]
    for index, row in df.iterrows():
        list_result = row['stockslist']
        list_result = list_result.strip("[]")
        list_result = list_result.split(', ')
        itemlist = []
        for item in list_result:
            item = item.strip("''")
            itemlist.append(item)
            #print (item)        
        print (row['index_code'],row['industry_name'],len(itemlist),itemlist[0:2])
        #目标列表添加
        result_list.extend(itemlist)
    print ('target_list:'+str(len(result_list)),result_list)
    return df  

indexcode_list = ['1','2']
df_indexcode = get_stocksbasic_industry_indexcode(indexcode_list)    