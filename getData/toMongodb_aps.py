# -*- coding: utf-8 -*-
"""
Created on Mon Jun 29 15:01:41 2020

@author: 李博
"""
import tushare as ts
import pandas as pd
import numpy as np
import talib
from getData import caltools
import datetime
import time
#获取日线前复权数据模块
#import toMongodb_stocks_daily
#获取最新每日指标数据模块
#import toMongodb_dailybasic_last
#获取个股资金流向
#import toMongodb_moneyflow
#定时器初始化
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
from pymongo import MongoClient
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(30), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday
#获取条件集合函数 参数 col 返回df
def get_col_df(col):
    mycollection=mydb[col]
    rs_col = mycollection.find()
    list_col = list(rs_col)
    #将查询结果转换为Df
    df_col = pd.DataFrame(list_col)
    #print (df_stockcode)
    return df_col

#TRADELIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    lasttradeday_list.reverse()
    return lasttradeday_list

#exchange SSE上交所 SZSE深交所
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data
#daily
def get_eachstock_daily_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        #print (stockname)
        mycol = mydb['daily_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq')
        if (df is None):
            print (stockname+' data empty')
        else:
            mycol.insert_many(df.to_dict('records'))
            print ('daily_'+stockname+': '+str(len(df)))
#weekly
def get_eachstock_weekly_qfq():
    stocksbasicdata = pro.query('stock_basic',exchange='',list_status='L')
    stocknamelist = list(stocksbasicdata['ts_code'])
    print (len(stocknamelist))
    for stockname in stocknamelist:
        mycol = mydb['weekly_'+stockname]
        mycol.remove()
        df = ts.pro_bar(ts_code=stockname,adj='qfq',freq='W')
        if (df is None):
            print (stockname+' data empty')
        else:
            mycol.insert_many(df.to_dict('records'))
            print ('weekly_'+stockname+': '+str(len(df)))

#weekly+MACD
def get_eachstock_weekly_qfq_macd():
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #定义文档名称
        mycol = mydb['weekly_qfq_macd_'+stockcode]
        mycol.remove()
        #获取前复权数据，默认为日期降序
        df_qfq = ts.pro_bar(ts_code=stockcode, freq='W', adj='qfq')
        #不符合计算MACD条件
        if (df_qfq is None or len(df_qfq)<2):
            continue
        #计算MACD
        else:            
            #按照日期升序df
            df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
            #重新设置索引
            df_qfq_asc.reset_index(drop=True, inplace=True)
            #初始化CDMA df
            df = pd.DataFrame()
            df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
            df['MACD'] = df['MACD'] * 2
            #按照index反序
            df = df.sort_index(ascending=False)
            #重新设置索引
            df.reset_index(drop=True, inplace=True)
            #赋值series给df_qfq
            df_qfq['DIF'] = df['DIF']
            df_qfq['DEA'] = df['DEA']
            df_qfq['MACD'] = df['MACD']
        mycol.insert_many(df_qfq.to_dict('records'))
        print (df_qfq['trade_date'][0],'weekly_'+stockcode+': '+str(len(df_qfq)))
    print (len(result_list))

#daily+MACD
def get_eachstock_daily_qfq_macd():
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #定义文档名称
        mycol = mydb['daily_qfq_macd_'+stockcode]
        mycol.remove()
        #获取前复权数据，默认为日期降序
        df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
        #不符合计算MACD条件
        if (df_qfq is None or len(df_qfq)<2):
            continue
        #计算MACD
        else:            
            #按照日期升序df
            df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
            #重新设置索引
            df_qfq_asc.reset_index(drop=True, inplace=True)
            #初始化CDMA df
            df = pd.DataFrame()
            df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
            df['MACD'] = df['MACD'] * 2
            #按照index反序
            df = df.sort_index(ascending=False)
            #重新设置索引
            df.reset_index(drop=True, inplace=True)
            #赋值series给df_qfq
            df_qfq['DIF'] = df['DIF']
            df_qfq['DEA'] = df['DEA']
            df_qfq['MACD'] = df['MACD']
            #CCI
            CCI_serise = talib.CCI(df_qfq_asc['high'], df_qfq_asc['low'], df_qfq_asc['close'], timeperiod=14)
            CCI_serise = CCI_serise.sort_index(ascending=False)
            CCI_serise.reset_index(drop=True, inplace=True)
            df_qfq['CCI'] = CCI_serise
            df_qfq = caltools.get_boll(df_qfq)
        mycol.insert_many(df_qfq.to_dict('records'))
        print (df_qfq['trade_date'][0],'daily_'+stockcode+': '+str(len(df_qfq)))
    print (len(result_list))


#daily+MACD 
def get_eachstock_daily_qfq_indexs():
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #CHECK
        df_check = get_col_df('daily_qfq_macd_'+stockcode)
        if (df_check.empty):
            #定义文档名称
            mycol = mydb['daily_qfq_macd_'+stockcode]            
            mycol.remove()
            #获取前复权数据，默认为日期降序
            #df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
            #df_nfq = ts.pro_bar(ts_code=stockcode)
            i = 0
            while i < 3:
                try:
                    df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                    break
                except:
                    print ('PAUSE WAITING 60S TO RECONNECTION')
                    time.sleep(60)
                    i += 1            
            #不符合计算MACD条件
            if (df_qfq is None or len(df_qfq)<2):
                continue
            #计算MACD
            else:            
                #按照日期升序df
                df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
                #重新设置索引
                df_qfq_asc.reset_index(drop=True, inplace=True)
                #初始化CDMA df
                df = pd.DataFrame()
                df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
                df['MACD'] = df['MACD'] * 2
                #按照index反序
                df = df.sort_index(ascending=False)
                #重新设置索引
                df.reset_index(drop=True, inplace=True)
                #赋值series给df_qfq
                df_qfq['DIF'] = df['DIF']
                df_qfq['DEA'] = df['DEA']
                df_qfq['MACD'] = df['MACD']
                #CCI
                CCI_serise = talib.CCI(df_qfq_asc['high'], df_qfq_asc['low'], df_qfq_asc['close'], timeperiod=14)
                CCI_serise = CCI_serise.sort_index(ascending=False)
                CCI_serise.reset_index(drop=True, inplace=True)
                df_qfq['CCI'] = CCI_serise
                #KDJ
                df_kjd = caltools.get_kdj(df_qfq)
                df_qfq['K'] = df_kjd['K']
                df_qfq['D'] = df_kjd['D']
                df_qfq['J'] = df_kjd['J']
                df_qfq = caltools.get_boll(df_qfq)
                mycol.insert_many(df_qfq.to_dict('records'))
                print (df_qfq['trade_date'][0],'daily_'+stockcode+': '+str(len(df_qfq)))        
        else:
            col_trade_date = df_check['trade_date'][0]
            current_trade_date =  get_lasttradedate(0)       
            if (col_trade_date == current_trade_date):
                print (col_trade_date,col_trade_date,stockcode,'DATA ALREADY')
            else:
                #定义文档名称
                mycol = mydb['daily_qfq_macd_'+stockcode]            
                mycol.remove()
                #获取前复权数据，默认为日期降序
                #df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                #df_nfq = ts.pro_bar(ts_code=stockcode)
                i = 0
                while i < 3:
                    try:
                        df_qfq = ts.pro_bar(ts_code=stockcode,adj='qfq')
                        break
                    except:
                        print ('PAUSE WAITING 60S TO RECONNECTION')
                        time.sleep(60)
                        i += 1                  
                #不符合计算MACD条件
                if (df_qfq is None or len(df_qfq)<2):
                    continue
                #计算MACD
                else:            
                    #按照日期升序df
                    df_qfq_asc = df_qfq.sort_values(by="trade_date",ascending=True)
                    #重新设置索引
                    df_qfq_asc.reset_index(drop=True, inplace=True)
                    #初始化CDMA df
                    df = pd.DataFrame()
                    df['DIF'], df['DEA'],df['MACD'] = talib.MACDEXT(df_qfq_asc['close'], fastperiod=12, fastmatype=1, slowperiod=26,slowmatype=1,signalperiod=9, signalmatype=1)
                    df['MACD'] = df['MACD'] * 2
                    #按照index反序
                    df = df.sort_index(ascending=False)
                    #重新设置索引
                    df.reset_index(drop=True, inplace=True)
                    #赋值series给df_qfq
                    df_qfq['DIF'] = df['DIF']
                    df_qfq['DEA'] = df['DEA']
                    df_qfq['MACD'] = df['MACD']
                    #CCI
                    CCI_serise = talib.CCI(df_qfq_asc['high'], df_qfq_asc['low'], df_qfq_asc['close'], timeperiod=14)
                    CCI_serise = CCI_serise.sort_index(ascending=False)
                    CCI_serise.reset_index(drop=True, inplace=True)
                    df_qfq['CCI'] = CCI_serise
                    #KDJ
                    df_kjd = caltools.get_kdj(df_qfq)
                    df_qfq['K'] = df_kjd['K']
                    df_qfq['D'] = df_kjd['D']
                    df_qfq['J'] = df_kjd['J']
                    df_qfq = caltools.get_boll(df_qfq)
                    mycol.insert_many(df_qfq.to_dict('records'))
                    print (df_qfq['trade_date'][0],'daily_'+stockcode+': '+str(len(df_qfq)))
                    #print (df_qfq['trade_date'][0],df_qfq['K'][0],df_qfq['D'][0],df_qfq['J'][0])
                    #break
    print (len(result_list))
    #return df_qfq

#get_eachstock_daily_qfq_indexs()    
def get_stocks_daily_moneyflow():
    result_list = []
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #CHECK
        df_check = get_col_df('daily_moneyflow_'+stockcode)
        if (df_check.empty):
            #定义文档名称
            mycol = mydb['daily_moneyflow_'+stockcode]            
            mycol.remove()
            
            #获取某股票资金流入流出，并指定字段输出
            #df_moneyflow_stock = pro.moneyflow(ts_code=stockcode)
            i = 0
            while i < 3:
                try:
                    df_moneyflow_stock = pro.moneyflow(ts_code=stockcode)
                    break
                except:
                    print ('PAUSE WAITING 60S TO RECONNECTION')
                    time.sleep(60)
                    i += 1
            if (df_moneyflow_stock.empty):
                print ('daily_moneyflow_'+stockcode+': EMPTY')
            else:
                mycol.insert_many(df_moneyflow_stock.to_dict('records'))
                print (df_moneyflow_stock['trade_date'][0],'daily_moneyflow_'+stockcode+': '+str(len(df_moneyflow_stock)))
                result_list.append(stockcode)            
        else:            
            col_trade_date = df_check['trade_date'][0]
            current_trade_date =  get_lasttradedate(0)       
            if (col_trade_date == current_trade_date):
                print (col_trade_date,col_trade_date,stockcode,'DATA ALREADY')
            else:        
                #定义文档名称
                mycol = mydb['daily_moneyflow_'+stockcode]            
                mycol.remove()
                #获取某股票资金流入流出，并指定字段输出
                #df_moneyflow_stock = pro.moneyflow(ts_code=stockcode)
                i = 0
                while i < 3:
                    try:
                        df_moneyflow_stock = pro.moneyflow(ts_code=stockcode)
                        break
                    except:
                        print ('PAUSE WAITING 60S TO RECONNECTION')
                        time.sleep(60)
                        i += 1                
                if (df_moneyflow_stock.empty):
                    print ('daily_moneyflow_'+stockcode+': EMPTY')
                else:
                    mycol.insert_many(df_moneyflow_stock.to_dict('records'))
                    print (df_moneyflow_stock['trade_date'][0],'daily_moneyflow_'+stockcode+': '+str(len(df_moneyflow_stock)))
                    result_list.append(stockcode)
    print (len(result_list))

def get_stocks_dailybasic_lastday():
    lasttradedate = get_lasttradedate(0)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_dailybasic = pro.daily_basic(ts_code='', trade_date=lasttradedate)
    if (df_dailybasic.empty):
        df_dailybasic = pro.daily_basic(ts_code='', trade_date= get_lasttradedate(1))
    df = pd.merge(df_stockbasic, df_dailybasic, how='left', on='ts_code')
    df['stockname'] = df['name']
    #定义文档名称
    mycol = mydb['stocks_dailybasic_lastday']
    mycol.remove()
    mycol.insert_many(df.to_dict('records'))
    print (df['trade_date'][0],'stocks_dailybasic_lastday:'+str(len(df)))

def get_stocks_daily_lastday():
    lasttradedate = get_lasttradedate(0)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_daily = pro.daily(ts_code='', trade_date=lasttradedate)
    if (df_daily.empty):
        df_daily = pro.daily(ts_code='', trade_date= get_lasttradedate(1))
    df = pd.merge(df_stockbasic, df_daily, how='left', on='ts_code')
    df.rename(columns={"name": "stockname"})
    #定义文档名称
    mycol = mydb['stocks_daily_lastday']
    mycol.remove()
    mycol.insert_many(df.to_dict('records'))
    print (df['trade_date'][0],'stocks_daily_lastday:'+str(len(df)))

#最新周线数据更新    
def get_stocks_weekly_last():
    lasttradedate = get_lasttradedate(0)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_weekly = pro.weekly(trade_date=lasttradedate)
    if (df_weekly.empty):
        print ('WEEKDATA NOT READY PLEASE WAIT')
        df_weekly = pro.weekly(trade_date='20201211')
    df_weekly['pct_chg'] = df_weekly['pct_chg']*100
    df_weekly['pct_chg'] = df_weekly['pct_chg'].round(2)
    df = pd.merge(df_stockbasic, df_weekly, how='left', on='ts_code')
    df['stockname'] = df['name']
    #定义文档名称
    mycol = mydb['stocks_weekly_last']
    mycol.remove()
    mycol.insert_many(df.to_dict('records'))
    print (df['trade_date'][0],'stocks_weekly_last:'+str(len(df)))
    

#最新月线数据更新    
def get_stocks_monthly_last():
    lasttradedate = get_lasttradedate(0)
    df_stockbasic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    df_monthly = pro.monthly(trade_date=lasttradedate)
    if (df_monthly.empty):
        print ('MONTHDATA NOT READY PLEASE WAIT')
        df_monthly = pro.monthly(trade_date='20201130')
    df_monthly['pct_chg'] = df_monthly['pct_chg']*100
    df_monthly['pct_chg'] = df_monthly['pct_chg'].round(2)
    df = pd.merge(df_stockbasic, df_monthly, how='left', on='ts_code')
    df['stockname'] = df['name']
    #定义文档名称
    mycol = mydb['stocks_monthly_last']
    mycol.remove()
    mycol.insert_many(df.to_dict('records'))
    print (df['trade_date'][0],'stocks_monthly_last:'+str(len(df)))    
    
def get_stocks_hsgt():
    tradedatelist = get_lasttradedatelist(0,100) 
    df_hsgt = pro.moneyflow_hsgt(start_date=tradedatelist[30], end_date=tradedatelist[0])
    #定义文档名称
    mycol = mydb['hsgt']
    mycol.remove()
    mycol.insert_many(df_hsgt.to_dict('records'))
    print (df_hsgt['trade_date'][0],'df_hsgt:'+str(len(df_hsgt)))



def get_stocksbasic():
    #查询当前所有正常上市交易的股票列表
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,market,name,area,industry,list_date')
    #定义文档名称
    mycol = mydb['stocksbasic']
    mycol.remove()
    mycol.insert_many(df.to_dict('records'))
    print ('stocksbasic:'+str(len(df)))

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
    #定义文档名称
    mycol = mydb['stocksbasic_industry_list']
    mycol.remove()
    mycol.insert_many(df_result.to_dict('records'))
    print ('stocksbasic_industry_list:'+str(len(df_result)))
    '''
    #保存CSV文档
    df_result.to_csv('../data/industry/industry.csv')
    df=pd.read_csv('../data/industry/industry.csv',sep=',') #filename可以直接从盘符开始，标明每一级的文件夹直到csv文件，header=None表示头部为空，sep=' '表示数据间使用空格作为分隔符，如果分隔符是逗号，只需换成 ‘，'即可。
    df_daily = pro.daily(trade_date=get_lasttradedate(0))
    df_daily_result = pd.DataFrame()
    for index, row in df.iterrows():
        list_result = row['stockslist']
        list_result = list_result.strip("[]")
        list_result = list_result.split(', ')
        itemlist = []
        for item in list_result:
            item = item.strip("''")
            itemlist.append(item)
            #print (item)
        #print (len(itemlist),itemlist[0:2])
        df_daily_result = df_daily[df_daily.ts_code.isin(itemlist)]
        print (row['industry_name'],df_daily_result['trade_date'].head(1).iloc[0],round(df_daily_result['pct_chg'].mean(),2))
    '''
    return df_result
#df = get_stocksbasic_industry_list()

#证监会分类数据分析
def get_analysis_industry():    
    df_industry = get_col_df('stocksbasic_industry_list')
    tradedatelist = get_lasttradedatelist(0,10)
    #当前行情
    df_daily_lastday = pro.daily(trade_date=tradedatelist[0])
    if (df_daily_lastday.empty):
        #当前行情
        df_daily_lastday = pro.daily(trade_date=tradedatelist[1])
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[2])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[3])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[1])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[1], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')
        
    else:        
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[1])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[2])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[0])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[0], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')

    df_daily_lastoneday = pd.DataFrame()
    df_daily_lastoneday['vol_1'] = df_daily_lastday_1['vol']
    df_daily_lastoneday['ts_code'] = df_daily_lastday_1['ts_code']
    df_daily_lastoneday['trade_date_1'] = df_daily_lastday_1['trade_date']

    df_daily_lasttwoday = pd.DataFrame()
    df_daily_lasttwoday['close_2'] = df_daily_lastday_2['close']
    df_daily_lasttwoday['ts_code'] = df_daily_lastday_2['ts_code']
    df_daily_lasttwoday['trade_date_2'] = df_daily_lastday_2['trade_date']

    limit_lastday_stockslist = df_limit_lastday['ts_code'].tolist()
    df_result = pd.DataFrame()
    for stockslist in df_industry['stockslist']:
        result_dict={}
        stocks_df = pd.DataFrame()
        stocks_df = pd.DataFrame(columns=['ts_code'], data=stockslist)
        stocks_df = pd.merge(stocks_df, df_daily_lastday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lastoneday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lasttwoday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_dailybasic, how='left', on='ts_code')
        #涨跌个股数量统计
        stocks_count = len(stocks_df['ts_code'])
        stocks_up_count = len(stocks_df[stocks_df['pct_chg']>0])
        stocks_down_count = len(stocks_df[stocks_df['pct_chg']<=0])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)
        #涨停家数
        #统计两个数组相同元素个数
        stocks_limit_count = len(set(stockslist) & set(limit_lastday_stockslist))
        #3日涨幅
        stocks_df['pct_chg_3'] = (stocks_df['close']-stocks_df['close_2'])*100/stocks_df['close_2']
        #平均3日涨幅
        stocks_pct_chg_3_avg = round(stocks_df['pct_chg_3'].mean(),3)        
        #最大涨幅
        stocks_pct_chg_max = round(stocks_df['pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(stocks_df['pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(stocks_df['pct_chg'].mean(),2)
        
        #成交量增长比例
        stocks_df['vol_pct_chg']  = (stocks_df['vol']-stocks_df['vol_1'])/stocks_df['vol_1']        
        #成交量放大个股数量
        stocks_vol_up_count = len(stocks_df[stocks_df['vol_pct_chg']>0])
        #成交量放大个股数量比例
        stocks_vol_up_count_ratio = round(stocks_vol_up_count/stocks_count,2)
        #成交量放大1倍个股数量
        stocks_vol_up_100_count = len(stocks_df[stocks_df['vol_pct_chg']>1])
        #成交量放大1倍个股数量比例
        stocks_vol_up_100_count_ratio = round(stocks_vol_up_100_count/stocks_count,2)        
        #成交量最大涨幅
        stocks_vol_pct_chg_max = round(stocks_df['vol_pct_chg'].max(),2)
        #成交量最小涨幅
        stocks_vol_pct_chg_min = round(stocks_df['vol_pct_chg'].min(),2)
        #成交量平均涨幅
        stocks_vol_pct_chg_avg = round(stocks_df['vol_pct_chg'].mean(),2)
        #高换手率（>N%）个股数
        N = 5.0
        stocks_turnover_up_N_count = len(stocks_df[stocks_df['turnover_rate']>N])
        #高换手率（>N%）个股数占比
        stocks_turnover_up_N_count_ratio = round(stocks_turnover_up_N_count/stocks_count,2)
        #平均换手率
        stocks_turnover_avg = round(stocks_df['turnover_rate'].mean(),2)        
        #高量比（>M%）个股数
        M = 5.0
        stocks_vr_up_M_count = len(stocks_df[stocks_df['volume_ratio']>M])
        #高量比（>M%）个股数占比
        stocks_vr_up_M_count_ratio = round(stocks_vr_up_M_count/stocks_count,2)
        #平均量比
        stocks_vr_avg = round(stocks_df['volume_ratio'].mean(),2) 
        #拼装数据
        result_dict['trade_date'] = stocks_df['trade_date'][0]
        result_dict['trade_date_2'] = stocks_df['trade_date_2'][0]
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_down_count'] = stocks_down_count
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_limit_count'] = stocks_limit_count
        result_dict['stocks_pct_chg_3_avg'] = stocks_pct_chg_3_avg
        
        result_dict['stocks_vol_up_count'] = stocks_vol_up_count
        result_dict['stocks_vol_up_count_ratio'] = stocks_vol_up_count_ratio
        result_dict['stocks_vol_pct_chg_max'] = stocks_vol_pct_chg_max
        result_dict['stocks_vol_pct_chg_min'] = stocks_vol_pct_chg_min
        result_dict['stocks_vol_pct_chg_avg'] = stocks_vol_pct_chg_avg
        result_dict['stocks_vol_up_100_count'] = stocks_vol_up_100_count
        result_dict['stocks_vol_up_100_count_ratio'] = stocks_vol_up_100_count_ratio
        
        result_dict['stocks_turnover_up_N_count_ratio'] = stocks_turnover_up_N_count_ratio
        result_dict['stocks_turnover_avg'] = stocks_turnover_avg
        result_dict['stocks_vr_up_M_count_ratio'] = stocks_vr_up_M_count_ratio
        result_dict['stocks_vr_avg'] = stocks_vr_avg      
        #print (stocks_count,stocks_up_count,stocks_down_count,stocks_pct_chg_max,stocks_pct_chg_min,stocks_pct_chg_avg)        
        df_result = df_result.append(result_dict,ignore_index=True)
    df_result = pd.concat([df_industry, df_result], axis=1)
    mycollection=mydb['analysis_category_stocksbasic_industry_list']
    mycollection.remove()
    mycollection.insert_many(df_result.to_dict('records'))
    print ('analysis_category_stocksbasic_industry_list',df_result['trade_date'][0],str(len(df_result)))
    return df_result
#df = get_analysis_industry()    
#申万行业分类数据入库
#获取申万一到三级行业列表
def sw_list(src,level):
    df = pro.index_classify(level=level, src=src)
    mycollection=mydb[src+level]
    mycollection.remove()
    mycollection.insert_many(df.to_dict('records'))
    print (src,level,str(len(df)))

#获取申万一到三级行业列表及包含个股
def sw_stocks_list(src,level):
    df = pro.index_classify(level=level, src=src)
    swstoocks_list = []
    swstoocks_count_list = []
    for i in df['index_code']:
        #print (i)
        df_swstocks = pro.index_member(index_code=i)
        swstockslist = df_swstocks['con_code'].tolist()
        swstoocks_list.append(swstockslist)
        swstoocks_count_list.append(len(swstockslist))
    df['stockslist'] = pd.Series(swstoocks_list)
    df['stockslist_count'] = pd.Series(swstoocks_count_list)    
    mycollection=mydb[src+level+'_stocks']
    mycollection.remove()
    mycollection.insert_many(df.to_dict('records'))
    print (src,level,str(len(df)))
    return df
#df = sw_stocks_list('sw','l2')

def get_data_sw_levels():
    sw_stocks_list('sw','l1')
    sw_stocks_list('sw','l2')
    sw_stocks_list('sw','l3')
#df=get_data_sw_levels()

def get_analysis_sw(src,level):
    df_sw = get_col_df(src+level+'_stocks')
    tradedatelist = get_lasttradedatelist(0,10)
    #当前行情
    df_daily_lastday = pro.daily(trade_date=tradedatelist[0])
    if (df_daily_lastday.empty):
        #当前行情
        df_daily_lastday = pro.daily(trade_date=tradedatelist[1])
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[2])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[3])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[1])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[1], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')
        
    else:        
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[1])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[2])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[0])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[0], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')

    df_daily_lastoneday = pd.DataFrame()
    df_daily_lastoneday['vol_1'] = df_daily_lastday_1['vol']
    df_daily_lastoneday['ts_code'] = df_daily_lastday_1['ts_code']
    df_daily_lastoneday['trade_date_1'] = df_daily_lastday_1['trade_date']

    df_daily_lasttwoday = pd.DataFrame()
    df_daily_lasttwoday['close_2'] = df_daily_lastday_2['close']
    df_daily_lasttwoday['ts_code'] = df_daily_lastday_2['ts_code']
    df_daily_lasttwoday['trade_date_2'] = df_daily_lastday_2['trade_date']

    limit_lastday_stockslist = df_limit_lastday['ts_code'].tolist()
    df_result = pd.DataFrame()
    for stockslist in df_sw['stockslist']:
        result_dict={}
        stocks_df = pd.DataFrame()
        stocks_df = pd.DataFrame(columns=['ts_code'], data=stockslist)
        stocks_df = pd.merge(stocks_df, df_daily_lastday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lastoneday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lasttwoday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_dailybasic, how='left', on='ts_code')
        #涨跌个股数量统计
        stocks_count = len(stocks_df['pct_chg'])
        stocks_up_count = len(stocks_df[stocks_df['pct_chg']>0])
        stocks_down_count = len(stocks_df[stocks_df['pct_chg']<=0])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)
        #涨停家数
        #统计两个数组相同元素个数
        stocks_limit_count = len(set(stockslist) & set(limit_lastday_stockslist))
        #3日涨幅
        stocks_df['pct_chg_3'] = (stocks_df['close']-stocks_df['close_2'])*100/stocks_df['close_2']
        #平均3日涨幅
        stocks_pct_chg_3_avg = round(stocks_df['pct_chg_3'].mean(),3)        
        #最大涨幅
        stocks_pct_chg_max = round(stocks_df['pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(stocks_df['pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(stocks_df['pct_chg'].mean(),2)
        #成交量增长比例
        stocks_df['vol_pct_chg']  = (stocks_df['vol']-stocks_df['vol_1'])/stocks_df['vol_1']        
        #成交量放大个股数量
        stocks_vol_up_count = len(stocks_df[stocks_df['vol_pct_chg']>0])
        #成交量放大个股数量比例
        stocks_vol_up_count_ratio = round(stocks_vol_up_count/stocks_count,2)
        #成交量放大1倍个股数量
        stocks_vol_up_100_count = len(stocks_df[stocks_df['vol_pct_chg']>1])
        #成交量放大1倍个股数量比例
        stocks_vol_up_100_count_ratio = round(stocks_vol_up_100_count/stocks_count,2)        
        #成交量最大涨幅
        stocks_vol_pct_chg_max = round(stocks_df['vol_pct_chg'].max(),2)
        #成交量最小涨幅
        stocks_vol_pct_chg_min = round(stocks_df['vol_pct_chg'].min(),2)
        #成交量平均涨幅
        stocks_vol_pct_chg_avg = round(stocks_df['vol_pct_chg'].mean(),2)
        #高换手率（>N%）个股数
        N = 5.0
        stocks_turnover_up_N_count = len(stocks_df[stocks_df['turnover_rate']>N])
        #高换手率（>N%）个股数占比
        stocks_turnover_up_N_count_ratio = round(stocks_turnover_up_N_count/stocks_count,2)
        #平均换手率
        stocks_turnover_avg = round(stocks_df['turnover_rate'].mean(),2)        
        #高量比（>M%）个股数
        M = 5.0
        stocks_vr_up_M_count = len(stocks_df[stocks_df['volume_ratio']>M])
        #高量比（>M%）个股数占比
        stocks_vr_up_M_count_ratio = round(stocks_vr_up_M_count/stocks_count,2)
        #平均量比
        stocks_vr_avg = round(stocks_df['volume_ratio'].mean(),2) 
        #拼装数据
        result_dict['trade_date'] = stocks_df['trade_date'][0]
        result_dict['trade_date_2'] = stocks_df['trade_date_2'][0]
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_down_count'] = stocks_down_count
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_limit_count'] = stocks_limit_count
        result_dict['stocks_pct_chg_3_avg'] = stocks_pct_chg_3_avg
        
        result_dict['stocks_vol_up_count'] = stocks_vol_up_count
        result_dict['stocks_vol_up_count_ratio'] = stocks_vol_up_count_ratio
        result_dict['stocks_vol_pct_chg_max'] = stocks_vol_pct_chg_max
        result_dict['stocks_vol_pct_chg_min'] = stocks_vol_pct_chg_min
        result_dict['stocks_vol_pct_chg_avg'] = stocks_vol_pct_chg_avg
        result_dict['stocks_vol_up_100_count'] = stocks_vol_up_100_count
        result_dict['stocks_vol_up_100_count_ratio'] = stocks_vol_up_100_count_ratio
        
        result_dict['stocks_turnover_up_N_count_ratio'] = stocks_turnover_up_N_count_ratio
        result_dict['stocks_turnover_avg'] = stocks_turnover_avg
        result_dict['stocks_vr_up_M_count_ratio'] = stocks_vr_up_M_count_ratio
        result_dict['stocks_vr_avg'] = stocks_vr_avg
        #print (stocks_count,stocks_up_count,stocks_down_count,stocks_pct_chg_max,stocks_pct_chg_min,stocks_pct_chg_avg)        
        df_result = df_result.append(result_dict,ignore_index=True)
    df_result = pd.concat([df_sw, df_result], axis=1)
    mycollection=mydb['analysis_category_'+src+level]
    mycollection.remove()
    mycollection.insert_many(df_result.to_dict('records'))
    print ('analysis_category_',src,level,df_result['trade_date'][0],str(len(df_result)))
    return df_result
#df =get_analysis_sw('sw','l3')

def get_analysis_sw_levels():
    get_analysis_sw('sw','l1')
    get_analysis_sw('sw','l2')
    df = get_analysis_sw('sw','l3')
    return df

#df = get_analysis_sw_levels()


#获取概念列表
def get_data_concept():
    df_concept = pro.concept()
    conceptstockslist=[]
    conceptstockslist_count=[]
    for i in df_concept['code']:
    #获取概念所有个股
        df_conceptstocks = pro.concept_detail(id=i)
        conceptstockslist.append(df_conceptstocks['ts_code'].tolist())
        conceptstockslist_count.append(len(df_conceptstocks['ts_code']))
    df_concept['stockslist'] = pd.Series(conceptstockslist)
    df_concept['stockslist_count'] = pd.Series(conceptstockslist_count)
    df_concept = df_concept.rename(columns={'name':'concept_name'})
    df_concept = df_concept.rename(columns={'code':'concept_code'})
    mycollection=mydb['concept_list']
    mycollection.remove()
    mycollection.insert_many(df_concept.to_dict('records'))
    print ('concept_list',str(len(df_concept)))
    return df_concept
#df = get_data_concept()

#获取概念分类分析
def get_analysis_concept():
    df_concept = get_col_df('concept_list')
    #df_concept['concept_name'] = df_concept['name']
    #df_concept['concept_code'] = df_concept['code']
    tradedatelist = get_lasttradedatelist(0,10)
    #当前行情
    df_daily_lastday = pro.daily(trade_date=tradedatelist[0])
    if (df_daily_lastday.empty):
        #当前行情
        df_daily_lastday = pro.daily(trade_date=tradedatelist[1])
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[2])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[3])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[1])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[1], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')
        
    else:        
        #前1天行情
        df_daily_lastday_1 = pro.daily(trade_date=tradedatelist[1])
        #前2天交易日行情
        df_daily_lastday_2 = pro.daily(trade_date=tradedatelist[2])
        #当前涨停行情
        df_limit_lastday = pro.limit_list(trade_date=tradedatelist[0])
        #当前每日指标
        df_dailybasic = pro.daily_basic(ts_code='', trade_date=tradedatelist[0], fields='ts_code,turnover_rate,turnover_rate_f,volume_ratio')

    df_daily_lastoneday = pd.DataFrame()
    df_daily_lastoneday['vol_1'] = df_daily_lastday_1['vol']
    df_daily_lastoneday['ts_code'] = df_daily_lastday_1['ts_code']
    df_daily_lastoneday['trade_date_1'] = df_daily_lastday_1['trade_date']

    df_daily_lasttwoday = pd.DataFrame()
    df_daily_lasttwoday['close_2'] = df_daily_lastday_2['close']
    df_daily_lasttwoday['ts_code'] = df_daily_lastday_2['ts_code']
    df_daily_lasttwoday['trade_date_2'] = df_daily_lastday_2['trade_date']

    limit_lastday_stockslist = df_limit_lastday['ts_code'].tolist()
    df_result = pd.DataFrame()
    for stockslist in df_concept['stockslist']:
        result_dict={}
        stocks_df = pd.DataFrame()
        stocks_df = pd.DataFrame(columns=['ts_code'], data=stockslist)
        stocks_df = pd.merge(stocks_df, df_daily_lastday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lastoneday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_daily_lasttwoday, how='left', on='ts_code')
        stocks_df = pd.merge(stocks_df, df_dailybasic, how='left', on='ts_code')
        #涨跌个股数量统计
        stocks_count = len(stocks_df['pct_chg'])
        stocks_up_count = len(stocks_df[stocks_df['pct_chg']>0])
        stocks_down_count = len(stocks_df[stocks_df['pct_chg']<=0])
        #上涨个股数比例
        stocks_up_count_ratio = round(stocks_up_count/stocks_count,2)
        #涨停家数
        #统计两个数组相同元素个数
        stocks_limit_count = len(set(stockslist) & set(limit_lastday_stockslist))
        #3日涨幅
        stocks_df['pct_chg_3'] = (stocks_df['close']-stocks_df['close_2'])*100/stocks_df['close_2']
        #平均3日涨幅
        stocks_pct_chg_3_avg = round(stocks_df['pct_chg_3'].mean(),3)        
        #最大涨幅
        stocks_pct_chg_max = round(stocks_df['pct_chg'].max(),2)
        #最小涨幅
        stocks_pct_chg_min = round(stocks_df['pct_chg'].min(),2)
        #平均涨幅
        stocks_pct_chg_avg = round(stocks_df['pct_chg'].mean(),2)
        #成交量增长比例
        stocks_df['vol_pct_chg']  = (stocks_df['vol']-stocks_df['vol_1'])/stocks_df['vol_1']        
        #成交量放大个股数量
        stocks_vol_up_count = len(stocks_df[stocks_df['vol_pct_chg']>0])
        #成交量放大个股数量比例
        stocks_vol_up_count_ratio = round(stocks_vol_up_count/stocks_count,2)
        #成交量放大1倍个股数量
        stocks_vol_up_100_count = len(stocks_df[stocks_df['vol_pct_chg']>1])
        #成交量放大1倍个股数量比例
        stocks_vol_up_100_count_ratio = round(stocks_vol_up_100_count/stocks_count,2)        
        #成交量最大涨幅
        stocks_vol_pct_chg_max = round(stocks_df['vol_pct_chg'].max(),2)
        #成交量最小涨幅
        stocks_vol_pct_chg_min = round(stocks_df['vol_pct_chg'].min(),2)
        #成交量平均涨幅
        stocks_vol_pct_chg_avg = round(stocks_df['vol_pct_chg'].mean(),2)
        #高换手率（>N%）个股数
        N = 5.0
        stocks_turnover_up_N_count = len(stocks_df[stocks_df['turnover_rate']>N])
        #高换手率（>N%）个股数占比
        stocks_turnover_up_N_count_ratio = round(stocks_turnover_up_N_count/stocks_count,2)
        #平均换手率
        stocks_turnover_avg = round(stocks_df['turnover_rate'].mean(),2)        
        #高量比（>M%）个股数
        M = 5.0
        stocks_vr_up_M_count = len(stocks_df[stocks_df['volume_ratio']>M])
        #高量比（>M%）个股数占比
        stocks_vr_up_M_count_ratio = round(stocks_vr_up_M_count/stocks_count,2)
        #平均量比
        stocks_vr_avg = round(stocks_df['volume_ratio'].mean(),2) 
        #拼装数据
        result_dict['trade_date'] = stocks_df['trade_date'][0]
        result_dict['trade_date_2'] = stocks_df['trade_date_2'][0]
        result_dict['stocks_up_count'] = stocks_up_count
        result_dict['stocks_down_count'] = stocks_down_count
        result_dict['stocks_pct_chg_max'] = stocks_pct_chg_max
        result_dict['stocks_pct_chg_min'] = stocks_pct_chg_min
        result_dict['stocks_pct_chg_avg'] = stocks_pct_chg_avg
        result_dict['stocks_up_count_ratio'] = stocks_up_count_ratio
        result_dict['stocks_limit_count'] = stocks_limit_count
        result_dict['stocks_pct_chg_3_avg'] = stocks_pct_chg_3_avg
        
        result_dict['stocks_vol_up_count'] = stocks_vol_up_count
        result_dict['stocks_vol_up_count_ratio'] = stocks_vol_up_count_ratio
        result_dict['stocks_vol_pct_chg_max'] = stocks_vol_pct_chg_max
        result_dict['stocks_vol_pct_chg_min'] = stocks_vol_pct_chg_min
        result_dict['stocks_vol_pct_chg_avg'] = stocks_vol_pct_chg_avg
        result_dict['stocks_vol_up_100_count'] = stocks_vol_up_100_count
        result_dict['stocks_vol_up_100_count_ratio'] = stocks_vol_up_100_count_ratio
        
        result_dict['stocks_turnover_up_N_count_ratio'] = stocks_turnover_up_N_count_ratio
        result_dict['stocks_turnover_avg'] = stocks_turnover_avg
        result_dict['stocks_vr_up_M_count_ratio'] = stocks_vr_up_M_count_ratio
        result_dict['stocks_vr_avg'] = stocks_vr_avg
        #print (stocks_count,stocks_up_count,stocks_down_count,stocks_pct_chg_max,stocks_pct_chg_min,stocks_pct_chg_avg)        
        df_result = df_result.append(result_dict,ignore_index=True)
    df_result = pd.concat([df_concept, df_result], axis=1)
    mycollection=mydb['analysis_category_concept_list']
    mycollection.remove()
    mycollection.insert_many(df_result.to_dict('records'))
    print ('analysis_category_concept_list',df_result['trade_date'][0],str(len(df_result)))
    return df_result
#df = get_analysis_concept()
    

#获取最新交易日的指数行情
def get_sw_indexs_daily_lastday():
    df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(0))
    lasttradedate = df_tradedate['cal_date'].tail(1).iloc[0]
    df_swindex = pro.sw_daily(trade_date=lasttradedate)
    if (df_swindex.empty):        
        df_tradedate = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(1))
        lasttradedate = df_tradedate['cal_date'].tail(1).iloc[0]
        df_swindex = pro.sw_daily(trade_date=lasttradedate)
    df_swindex['index_name'] = df_swindex['name']
    mycollection=mydb['swindexs_daily_last']
    mycollection.remove()
    mycollection.insert(df_swindex.to_dict('records'))
    print ('get_sw_indexs_daily_lastday',lasttradedate,len(df_swindex))
#get_sw_indexs_daily_lastday()
#获取三大产业分类数据
#三大行业分类列表
def get_data_three():
    df = pd.DataFrame()
    df['industry_code'] = pd.Series(['1','2','3'])
    df['industry_name'] = pd.Series(['第一产业','第二产业','第三产业'])
    industry_category_1_list = [{'category_char':'A','category_name':'农、林、牧、渔业'}]
    industry_category_2_list = [{'category_char':'B','category_name':'采矿业'},
                                {'category_char':'C','category_name':'制造业'},
                                {'category_char':'D','category_name':'电力、热力、燃气及水生产和供应业'},
                                {'category_char':'E','category_name':'建筑业'}
                                ]
    industry_category_3_list = [{'category_char':'A','category_name':'农、林、牧、渔专业及辅助性活动'},
                                {'category_char':'B','category_name':'开采专业及辅助性活动'},
                                {'category_char':'C','category_name':'金属制品、机械和设备修理业'},
                                {'category_char':'F','category_name':'批发和零售业'},
                                {'category_char':'G','category_name':'交通运输、仓储和邮政业'},                                
                                {'category_char':'H','category_name':'住宿和餐饮业'},
                                {'category_char':'I','category_name':'信息传输、软件和信息技术服务业'},
                                {'category_char':'J','category_name':'金融业'},
                                {'category_char':'K','category_name':'房地产业'},
                                {'category_char':'L','category_name':'租赁和商务服务业'},
                                {'category_char':'M','category_name':'科学研究和技术服务业'},
                                {'category_char':'N','category_name':'水利、环境和公共设施管理业'},
                                {'category_char':'O','category_name':'居民服务、修理和其他服务业'},
                                {'category_char':'P','category_name':'教育'},
                                {'category_char':'Q','category_name':'卫生和社会工作'},
                                {'category_char':'R','category_name':'文化、体育和娱乐业'}                              
                                ]
    df['industry_category'] = pd.Series([industry_category_1_list,industry_category_2_list,industry_category_3_list])    
    mycollection=mydb['industry_three']
    mycollection.remove()
    mycollection.insert_many(df.to_dict('records'))
    print ('industry_three',len(df))

def get_data_three_detail():
    df = pd.DataFrame()
    df['industry_detail_code'] = pd.Series(['1A','2B','2C','2D','2E','3A','3B',
                                              '3C','3F','3G','3H','3I','3J','3K',
                                              '3L','3M','3N','3O','3P','3Q','3R'    
                                          ])
    industry_detail_category_1A_list = [{'category_char':'1','category_detail_char':'A01','category_detail_name':'农业'},
                                     {'category_char':'1','category_detail_char':'A02','category_detail_name':'林业'},
                                     {'category_char':'1','category_detail_char':'A03','category_detail_name':'畜牧业'},
                                     {'category_char':'1','category_detail_char':'A04','category_detail_name':'渔业'}]
    industry_detail_category_2B_list = [{'category_char':'2','category_detail_char':'B06','category_detail_name':'煤炭开采和洗选业'},
                                     {'category_char':'2','category_detail_char':'B07','category_detail_name':'石油和天然气开采业'},
                                     {'category_char':'2','category_detail_char':'B08','category_detail_name':'黑色金属矿采选业'},
                                     {'category_char':'2','category_detail_char':'B09','category_detail_name':'有色金属矿采选业'},
                                     {'category_char':'2','category_detail_char':'B10','category_detail_name':'非金属矿采选业'},
                                     {'category_char':'2','category_detail_char':'B12','category_detail_name':'其他采矿业'}]
    industry_detail_category_2C_list = [{'category_char':'2','category_detail_char':'C13','category_detail_name':'农副食品加工业'},
                                     {'category_char':'2','category_detail_char':'C14','category_detail_name':'食品制造业'},
                                     {'category_char':'2','category_detail_char':'C15','category_detail_name':'酒、饮料和精制茶制造业'},
                                     {'category_char':'2','category_detail_char':'C16','category_detail_name':'烟草制品业'},
                                     {'category_char':'2','category_detail_char':'C17','category_detail_name':'纺织业'},
                                     {'category_char':'2','category_detail_char':'C18','category_detail_name':'纺织服装、服饰业'},
                                     {'category_char':'2','category_detail_char':'C19','category_detail_name':'皮革、毛皮、羽毛及其制品和制鞋业'},
                                     {'category_char':'2','category_detail_char':'C20','category_detail_name':'木材加工和木、竹、藤、棕、草制品业'},
                                     {'category_char':'2','category_detail_char':'C21','category_detail_name':'家具制造业'},
                                     {'category_char':'2','category_detail_char':'C22','category_detail_name':'造纸和纸制品业'},
                                     {'category_char':'2','category_detail_char':'C23','category_detail_name':'印刷和记录媒介复制业'},                                     
                                     {'category_char':'2','category_detail_char':'C24','category_detail_name':'文教、工美、体育和娱乐用品制造业'},
                                     {'category_char':'2','category_detail_char':'C25','category_detail_name':'石油、煤炭及其他燃料加工业'},
                                     {'category_char':'2','category_detail_char':'C26','category_detail_name':'化学原料和化学制品制造业'},
                                     {'category_char':'2','category_detail_char':'C27','category_detail_name':'医药制造业'},
                                     {'category_char':'2','category_detail_char':'C28','category_detail_name':'化学纤维制造业'},
                                     {'category_char':'2','category_detail_char':'C29','category_detail_name':'橡胶和塑料制品业'},
                                     {'category_char':'2','category_detail_char':'C30','category_detail_name':'非金属矿物制品业'},
                                     {'category_char':'2','category_detail_char':'C31','category_detail_name':'黑色金属冶炼和压延加工业'},
                                     {'category_char':'2','category_detail_char':'C32','category_detail_name':'有色金属冶炼和压延加工业'},
                                     {'category_char':'2','category_detail_char':'C33','category_detail_name':'金属制品业'},                                     
                                     {'category_char':'2','category_detail_char':'C34','category_detail_name':'通用设备制造业'},
                                     {'category_char':'2','category_detail_char':'C35','category_detail_name':'专用设备制造业'},
                                     {'category_char':'2','category_detail_char':'C36','category_detail_name':'汽车制造业'},
                                     {'category_char':'2','category_detail_char':'C37','category_detail_name':'铁路、船舶、航空航天和其他运输设备制造业'},
                                     {'category_char':'2','category_detail_char':'C38','category_detail_name':'电气机械和器材制造业'},
                                     {'category_char':'2','category_detail_char':'C39','category_detail_name':'计算机、通信和其他电子设备制造业'},
                                     {'category_char':'2','category_detail_char':'C40','category_detail_name':'仪器仪表制造业'},
                                     {'category_char':'2','category_detail_char':'C41','category_detail_name':'其他制造业'},                                     
                                     {'category_char':'2','category_detail_char':'C42','category_detail_name':'废弃资源综合利用业'}]
    industry_detail_category_2D_list = [{'category_char':'2','category_detail_char':'D44','category_detail_name':'电力、热力生产和供应业'},
                                     {'category_char':'2','category_detail_char':'D45','category_detail_name':'燃气生产和供应业'},
                                     {'category_char':'2','category_detail_char':'D46','category_detail_name':'水的生产和供应业'}]
    industry_detail_category_2E_list = [{'category_char':'2','category_detail_char':'E47','category_detail_name':'房屋建筑业'},
                                     {'category_char':'2','category_detail_char':'E48','category_detail_name':'土木工程建筑业'},
                                     {'category_char':'2','category_detail_char':'E49','category_detail_name':'建筑安装业'},
                                     {'category_char':'2','category_detail_char':'E50','category_detail_name':'建筑装饰、装修和其他建筑业'}
                                     ]
    industry_detail_category_3A_list = [{'category_char':'3','category_detail_char':'A05','category_detail_name':'农、林、牧、渔专业及辅助性活动'}]    
    industry_detail_category_3B_list = [{'category_char':'3','category_detail_char':'B11','category_detail_name':'开采专业及辅助性活动'}]   
    industry_detail_category_3C_list = [{'category_char':'3','category_detail_char':'C43','category_detail_name':'金属制品、机械和设备修理业'}]       
    industry_detail_category_3F_list = [
                                     {'category_char':'3','category_detail_char':'F51','category_detail_name':'批发业'},
                                     {'category_char':'3','category_detail_char':'F52','category_detail_name':'零售业'}
                                     ]
    industry_detail_category_3G_list = [
                                     {'category_char':'3','category_detail_char':'G53','category_detail_name':'铁路运输业'},
                                     {'category_char':'3','category_detail_char':'G54','category_detail_name':'道路运输业'},
                                     {'category_char':'3','category_detail_char':'G55','category_detail_name':'水上运输业'},
                                     {'category_char':'3','category_detail_char':'G56','category_detail_name':'航空运输业'},
                                     {'category_char':'3','category_detail_char':'G57','category_detail_name':'管道运输业'},
                                     {'category_char':'3','category_detail_char':'G58','category_detail_name':'多式联运和运输代理业'},
                                     {'category_char':'3','category_detail_char':'G59','category_detail_name':'装卸搬运和仓储业'},
                                     {'category_char':'3','category_detail_char':'G60','category_detail_name':'邮政业'}                                    
                                     ] 
    industry_detail_category_3H_list = [
                                     {'category_char':'3','category_detail_char':'H61','category_detail_name':'住宿业'},
                                     {'category_char':'3','category_detail_char':'H62','category_detail_name':'餐饮业'}
                                     ]   
    industry_detail_category_3I_list = [
                                    {'category_char':'3','category_detail_char':'I63','category_detail_name':'电信、广播电视和卫星传输服务'},
                                     {'category_char':'3','category_detail_char':'I64','category_detail_name':'互联网和相关服务'},
                                     {'category_char':'3','category_detail_char':'I65','category_detail_name':'软件和信息技术服务业'}
                                     ]
    industry_detail_category_3J_list = [
                                    {'category_char':'3','category_detail_char':'J66','category_detail_name':'货币金融服务'},
                                     {'category_char':'3','category_detail_char':'J67','category_detail_name':'资本市场服务'},
                                     {'category_char':'3','category_detail_char':'J68','category_detail_name':'保险业'},
                                     {'category_char':'3','category_detail_char':'J69','category_detail_name':'其他金融业'}
                                     ]    
    industry_detail_category_3K_list = [{'category_char':'3','category_detail_char':'K70','category_detail_name':'房地产业'}]    
    industry_detail_category_3L_list = [
                                     {'category_char':'3','category_detail_char':'L71','category_detail_name':'租赁业'},
                                     {'category_char':'3','category_detail_char':'L72','category_detail_name':'商务服务业'}
                                     ] 
    industry_detail_category_3M_list = [
                                    {'category_char':'3','category_detail_char':'M73','category_detail_name':'研究和试验发展'},
                                     {'category_char':'3','category_detail_char':'M74','category_detail_name':'专业技术服务业'},
                                     {'category_char':'3','category_detail_char':'M75','category_detail_name':'科技推广和应用服务业'}
                                     ]
    industry_detail_category_3N_list = [
                                    {'category_char':'3','category_detail_char':'N76','category_detail_name':'水利管理业'},
                                     {'category_char':'3','category_detail_char':'N77','category_detail_name':'生态保护和环境治理业'},
                                     {'category_char':'3','category_detail_char':'N78','category_detail_name':'公共设施管理业'},
                                     {'category_char':'3','category_detail_char':'N79','category_detail_name':'土地管理业'}
                                     ]
    industry_detail_category_3O_list = [
                                    {'category_char':'3','category_detail_char':'O80','category_detail_name':'居民服务业'},
                                     {'category_char':'3','category_detail_char':'O81','category_detail_name':'机动车、电子产品和日用产品修理业'},
                                     {'category_char':'3','category_detail_char':'O82','category_detail_name':'其他服务业'}
                                     ]  
    industry_detail_category_3P_list = [{'category_char':'3','category_detail_char':'P83','category_detail_name':'教育'}]    
    industry_detail_category_3Q_list = [
                                     {'category_char':'3','category_detail_char':'Q84','category_detail_name':'卫生'},
                                     {'category_char':'3','category_detail_char':'Q85','category_detail_name':'社会工作'}
                                     ] 
    industry_detail_category_3R_list = [
                                    {'category_char':'3','category_detail_char':'R86','category_detail_name':'新闻和出版业'},
                                     {'category_char':'3','category_detail_char':'R87','category_detail_name':'广播、电视、电影和录音制作业'},
                                     {'category_char':'3','category_detail_char':'R88','category_detail_name':'文化艺术业'},
                                     {'category_char':'3','category_detail_char':'R89','category_detail_name':'体育'},
                                     {'category_char':'3','category_detail_char':'R90','category_detail_name':'娱乐业'}
                                     ]
    df['industry_detail_category_list'] = pd.Series([industry_detail_category_1A_list,
                                                      industry_detail_category_2B_list,
                                                      industry_detail_category_2C_list,
                                                      industry_detail_category_2D_list,
                                                      industry_detail_category_2E_list,
                                                      industry_detail_category_3A_list,
                                                      industry_detail_category_3B_list,
                                                      industry_detail_category_3C_list,
                                                      industry_detail_category_3F_list,
                                                      industry_detail_category_3G_list,
                                                      industry_detail_category_3H_list,
                                                      industry_detail_category_3I_list,
                                                      industry_detail_category_3J_list,
                                                      industry_detail_category_3K_list,
                                                      industry_detail_category_3L_list,
                                                      industry_detail_category_3M_list,
                                                      industry_detail_category_3N_list,
                                                      industry_detail_category_3O_list,
                                                      industry_detail_category_3P_list,
                                                      industry_detail_category_3Q_list,
                                                      industry_detail_category_3R_list
                                                      ])
    mycollection=mydb['industry_three_detail']
    mycollection.remove()
    mycollection.insert_many(df.to_dict('records'))
    print ('industry_three_detail',len(df))

#get_data_three()  
'''
#get_daily_qfq_asp
sched.add_job(toMongodb_moneyflow.get_eachstock_moneyflow,'cron',day_of_week='mon-fri', hour=19, minute=30)
sched.add_job(toMongodb_dailybasic_last.get_dailybasic_last,'cron',day_of_week='mon-fri', hour=20, minute=50)                      
sched.add_job(toMongodb_stocks_daily.get_daily_qfq_asp,'cron',day_of_week='mon-fri', hour=21, minute=1)            
sched.add_job(get_eachstock_daily_qfq,'cron',day_of_week='mon-fri', hour=23, minute=30)
sched.add_job(get_eachstock_weekly_qfq,'cron',day_of_week='sat', hour=21, minute=1)
sched.add_job(toMongodb_stocks_daily.get_weekly_qfq_asp,'cron',day_of_week='sat', hour=20, minute=1)
sched.start()
'''