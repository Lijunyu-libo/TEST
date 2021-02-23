# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 11:26:58 2020
策略自动执行模块
@author: iFunk
"""
#import dispatch
from getData import toMongodb_aps
from getData import toMongodb_daily_index
#stocks
from getData import toMongodb_stocks
#forecast
#from getData import toMongodb_fina_forecast
from getData import toMongodb_sw_index 
from getData import toMongodb_sw
from getData import toMongodb_moneyflow
from getData import toMongodb_limit
from apscheduler.schedulers.blocking import BlockingScheduler
scheduler = BlockingScheduler()

#执行队列
def apslist():
    #最新股票池基本信息更新
    toMongodb_aps.get_stocksbasic()
    #最新个股当日行情
    toMongodb_aps.get_stocks_dailybasic_lastday()
    toMongodb_aps.get_stocks_daily_lastday()
    #日线更新
    #toMongodb_aps.get_eachstock_daily_qfq_indexs()
    toMongodb_stocks.get_eachstock_daily_qfq_indexs()
    #行业基础数据
    toMongodb_aps.get_stocksbasic_industry_list() 
    toMongodb_aps.get_data_concept()
    toMongodb_aps.get_data_sw_levels()
    toMongodb_sw.get_sw_stocks_data()
    #指数更新
    toMongodb_daily_index.get_indexs_data()    

    #财务数据
    #toMongodb_fina_forecast.get_forecast_2020()
    
    #资金流向更新
    toMongodb_aps.get_stocks_hsgt()
    toMongodb_moneyflow.get_moneyflow_last()
    toMongodb_limit.get_limit_last()

    #资金流向更新
    toMongodb_aps.get_stocks_daily_moneyflow()    
    
    #行业分析
    toMongodb_aps.get_analysis_concept()
    toMongodb_aps.get_analysis_industry()
    toMongodb_aps.get_analysis_sw_levels()
    toMongodb_sw_index.get_sw_indexs_data()    
    toMongodb_aps.get_sw_indexs_daily_lastday()
    #周线更新
    toMongodb_aps.get_stocks_weekly_last()
    #toMongodb_aps.get_eachstock_weekly_qfq_macd()
    toMongodb_stocks.get_eachstock_weekly_qfq_indexs()
    
    #月线更新
    #toMongodb_aps.get_stocks_monthly_last()
    #toMongodb_stocks.get_eachstock_monthly_qfq_indexs()

    #个股策略
    #dispatch.autostdemo()

#apslist()
toMongodb_stocks.get_eachstock_weekly_qfq_indexs()
#toMongodb_stocks.get_eachstock_monthly_qfq_indexs()
#toMongodb_sw.get_sw_stocks_data()
#toMongodb_stocks.get_eachstock_daily_qfq_indexs()
#toMongodb_aps.get_eachstock_daily_qfq_indexs()
#toMongodb_limit.get_limit_last()
#toMongodb_moneyflow.get_moneyflow_last()
#toMongodb_sw_index.get_sw_indexs_data()
#toMongodb_sw.get_sw_stocks_data()
#toMongodb_aps.get_stocks_weekly_last()
#toMongodb_aps.get_stocks_monthly_last()
#toMongodb_aps.get_stocks_daily_lastday()
#toMongodb_aps.get_stocks_hsgt()
#toMongodb_aps.get_analysis_concept()
#toMongodb_aps.get_analysis_industry()
#toMongodb_aps.get_data_sw_levels()
#toMongodb_aps.get_analysis_sw('sw','l3')
#toMongodb_aps.get_stocks_daily_moneyflow()
#toMongodb_aps.get_analysis_concept()
#toMongodb_aps.get_analysis_industry()
#toMongodb_aps.get_analysis_sw_levels()
'''
#定时执行
scheduler.add_job(func=apslist,
                  trigger='cron',
                  day_of_week='mon-fri',
                  hour=16, minute=30)
scheduler.start()
'''