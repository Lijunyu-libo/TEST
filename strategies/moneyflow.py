# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 11:55:29 2020
资金流向策略模块
最近交易日 资金净流入金额 top10
@author: 李博
"""
import pandas as pd
import datetime
import time

import tushare as ts
pro = ts.pro_api('003137d7baa1439f01f9d2917992de6b8511f70f84612c78574d6996')
TODAY = time.strftime('%Y%m%d',)

#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    return pre_date_str

def get_lasttradedate(n):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(10), end_date=get_day_time(n))
    lasttradeday = df['cal_date'].tail(1).iloc[0]
    return lasttradeday
#TRADEDATE LIST
def get_lasttradedatelist(n,days):
    df = pro.trade_cal(exchange='SSE', is_open='1',fileds='cal_date',start_date=get_day_time(n+days), end_date=get_day_time(n))
    lasttradeday_list = df['cal_date'].tolist()
    return lasttradeday_list

def get_moneyflow_last(df):
    #获取历史交易日
    lasttradeday = get_lasttradedate(0)
    #获取某日股票资金流入流出，并指定字段输出
    df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    if (df_moneyflow_lastday.empty):
        lasttradeday = get_lasttradedate(1)
        df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    result = pd.merge(df,df_moneyflow_lastday , how='left', on=['ts_code'])
    return result

def get_moneyflow_last_top(param,n):
    #获取历史交易日
    lasttradeday = get_lasttradedate(0)
    #获取某日股票资金流入流出，并指定字段输出
    df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    if (df_moneyflow_lastday.empty):
        lasttradeday = get_lasttradedate(1)
        df_moneyflow_lastday = pro.moneyflow(trade_date=lasttradeday)
    df_moneyflow_lastday = df_moneyflow_lastday.sort_values(by=param,ascending=False).head(n)
    return df_moneyflow_lastday

def get_moneyflow_last_netmfamount_top10(df):
    df_moneyflow_lastday =  get_moneyflow_last_top('net_mf_amount',10)
    result = pd.merge(df,df_moneyflow_lastday , how='right', on=['ts_code'])
    result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='net_mf_amount',ascending=False)
    return result

def get_moneyflow_last_buyelgamount_top10(df):
    df_moneyflow_lastday =  get_moneyflow_last_top('buy_elg_amount',10)
    result = pd.merge(df,df_moneyflow_lastday , how='right', on=['ts_code'])
    result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='buy_elg_amount',ascending=False)
    return result

def get_moneyflow_last_buylgamount_top10(df):
    df_moneyflow_lastday =  get_moneyflow_last_top('buy_lg_amount',10)
    result = pd.merge(df,df_moneyflow_lastday , how='right', on=['ts_code'])
    result.dropna(subset=['market'],inplace=True)
    result = result.sort_values(by='buy_lg_amount',ascending=False)
    return result

def get_moneyflow_stock(stockcode):
    #获取某股票资金流入流出，并指定字段输出
    df_moneyflow_stock = pro.moneyflow(ts_code=stockcode)
    return df_moneyflow_stock

def demo(buy_date,sell_date):
    stock_daily_moneyflow_df = pro.moneyflow(trade_date= buy_date)
    df_1 = pro.daily(trade_date= buy_date)
    stock_daily_moneyflow_df['buy_elg_lg_amount_ratio'] = round((stock_daily_moneyflow_df['buy_elg_amount']+stock_daily_moneyflow_df['buy_lg_amount'])/(stock_daily_moneyflow_df['buy_sm_amount']+stock_daily_moneyflow_df['buy_md_amount']+stock_daily_moneyflow_df['buy_lg_amount']+stock_daily_moneyflow_df['buy_elg_amount']),2)
    df = stock_daily_moneyflow_df[stock_daily_moneyflow_df['buy_elg_lg_amount_ratio']>=0.75]
    stockslist = df['ts_code'].tolist()
    df_0 = pro.daily(trade_date= sell_date)
    #df_daily_merge = pd.DataFrame()
    df_daily_1 = pd.DataFrame()
    df_daily_1['ts_code'] = df_1['ts_code']
    df_daily_1['close_1'] = df_1['close']
    df_daily_1['amount_1'] = df_1['amount']
    df_0 = pd.merge(df_0, df_daily_1, how='left', on='ts_code')
    df_0['profit'] = (df_0['close']-df_0['close_1'])/df_0['close_1']
    df_0['amount_diff'] = df_0['amount']-df_0['amount_1']
    df_result = df_0[df_0.ts_code.isin(stockslist)]
    #df_result = df_result[df_result['pct_chg']<1.0]
    #TONGJI
    df_result = df_result[df_result['close']>5]
    profit_up_count = len(df_result[df_result['profit']>0])
    df_result_count = len(df_result)
    if (df_result_count !=0):
        profit_up_count_ratio = round(profit_up_count/df_result_count,3)
        tradedate = df_result['trade_date'].tolist()[0]
    else:
        profit_up_count_ratio = profit_up_count
        tradedate = 'TRADEDATE'
    print (tradedate,df_result_count,profit_up_count_ratio,round(df_result['profit'].mean(),2),round(df_result['profit'].max(),2),round(df_result['profit'].min(),2))
    return df_result
'''
lasttradeday_list = get_lasttradedatelist(1,100)
i = 0
while i<len(lasttradeday_list)-1:
    demo(lasttradeday_list[i],lasttradeday_list[i+2])
    i+=1      
'''
def demo2(buy_date):
    stock_daily_moneyflow_df = pro.moneyflow(trade_date= buy_date)
    df_daily = pro.daily(trade_date= buy_date)
    stock_daily_moneyflow_df['buy_elg_lg_amount_ratio'] = round((stock_daily_moneyflow_df['buy_elg_amount']+stock_daily_moneyflow_df['buy_lg_amount'])/(stock_daily_moneyflow_df['buy_sm_amount']+stock_daily_moneyflow_df['buy_md_amount']+stock_daily_moneyflow_df['buy_lg_amount']+stock_daily_moneyflow_df['buy_elg_amount']),2)
    df = stock_daily_moneyflow_df[stock_daily_moneyflow_df['buy_elg_lg_amount_ratio']>=0.75]
    stockslist = df['ts_code'].tolist()
    df_result = df_daily[df_daily.ts_code.isin(stockslist)]
    df_result = df_result[df_result['close']>5]
    return df_result

#df = demo2('20201127')