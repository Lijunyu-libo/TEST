# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 10:39:58 2020
策略汇总调度
@author: iFunk
"""
import pandas as pd
import numpy as np
import inspect
import time
#策略工具
from strategies import caltools
from strategies import stockbasket
from strategies import dailybasic
from strategies import tradememo
from strategies import monitorpoll
#资金流向策略
from strategies import moneyflow
from strategies import moneyflowtoptogether

#量价策略
from strategies import volume
from strategies import limit

#趋势策略
from strategies import low
from strategies import ma
from strategies import mastickup2

#指标策略
from strategies import boll
from strategies import macd
from strategies import obv
from strategies import rsi

#板块策略
from strategies import sw

#MACD
#最近交易日个股日MACD金叉策略    
def get_stocks_daily_macd_cross():    
    resultdf = macd.get_stocks_daily_macd_cross()
    tradememo.autotrade(resultdf)

#0轴上方金叉
def get_stocks_daily_macd_cross_abovezreo():    
    resultdf = macd.get_stocks_daily_macd_cross_abovezreo()
    tradememo.autotrade(resultdf)

#MACD主升浪组合策略
def get_macd_mainwave():
    resultdf = macd.get_macd_mainwave()
    tradememo.autotrade(resultdf)

#3交易日前日0轴上MACD金叉，后n日量增价增策略    
def get_stocks_daily_n_macd_cross_abovezero_n_vol_close_up():
    resultdf = macd.get_stocks_daily_n_macd_cross_abovezero_n_vol_close_up(3)
    tradememo.autotrade(resultdf)

#4周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_4_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(4)
    tradememo.autotrade(resultdf)
#5周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_5_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(5)
    tradememo.autotrade(resultdf)
#6周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_6_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(6)
    tradememo.autotrade(resultdf)
#7周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_7_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(7)
    tradememo.autotrade(resultdf)
#8周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_8_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(8)
    tradememo.autotrade(resultdf)
#9周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_9_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(9)
    tradememo.autotrade(resultdf)    
#6周前周线金叉后计算n周换手率大于300策略
def get_stocks_weekly_10_macd_cross_turnover():
    resultdf = macd.get_stocks_weekly_n_macd_cross_turnover(10)
    tradememo.autotrade(resultdf)


#3交易日前日MACD金叉，后3日DIF增策略
def get_stocks_daily_n_macd_cross_n_dif_up():
    resultdf = macd.get_stocks_daily_n_macd_cross_n_dif_up(3)
    tradememo.autotrade(resultdf)

#当前交易日个股N日MACDDIFF极端低点策略
def get_stocks_daily_n_macd_diff_low():
    resultdf = macd.get_stocks_daily_n_macd_diff_low(220)
    tradememo.autotrade(resultdf)

#N交易日个股MACD金叉后出现二次金叉策略
def get_stocks_daily_macd_doublecross():
    resultdf = macd.get_stocks_daily_macd_doublecross()
    tradememo.autotrade(resultdf)


#VOL
#30日成交量最低位阶段性低点策略
def get_stocks_daily_60_vol_low():
    resultdf = volume.get_stocks_daily_n_vol_low(30)
    tradememo.autotrade(resultdf)

#最近40交易日盘整后成交量天量股价拉升换手高策略
def get_stocks_daily_40_close_low_vol_close_up_max():
    resultdf = volume.get_stocks_daily_n_close_low_vol_close_up_max(40)
    tradememo.autotrade(resultdf)

#最近10交易日天量天价高换手策略
def get_stocks_daily_10_turnover_up_vol_close_up():
    resultdf = volume.get_stocks_daily_n_turnover_up_vol_close_up(10)
    tradememo.autotrade(resultdf)    

#MA
#N最近交易日MA10ma20ma30粘合MACDDIF0轴上方策略   
def get_stocks_daily_n_ma_stick_macd_diff_abovezero():
    resultdf = ma.get_stocks_daily_n_ma_stick_macd_diff_abovezero(0)
    tradememo.autotrade(resultdf)
    
#N最近交易日MA20上升MACD0轴上方金叉策略
def get_stocks_daily_n_ma_up_macd_cross_abovezero():
    resultdf = ma.get_stocks_daily_n_ma_up_macd_cross_abovezero(2)
    tradememo.autotrade(resultdf) 

#INDUSTRY
def get_sw_indexs_up_stocks_top_n():
    resultdf = sw.get_sw_indexs_up_stocks_top_n(10)
    tradememo.autotrade(resultdf)  
    

#MONITOR
#监控N日前MACD金叉
def get_stocks_n_daily_macd_cross():
    resultdf = macd.get_stocks_n_daily_macd_cross(10)
    monitorpoll.automonitor(resultdf)      

#手动执行模拟交易
#get_stocks_daily_macd_doublecross()

#自动模拟交易集合
def autostdemo():
    #MONITOR
    get_stocks_n_daily_macd_cross()
    #MACD
    #get_stocks_daily_macd_cross()
    get_stocks_daily_macd_cross_abovezreo()
    get_macd_mainwave()
    get_stocks_daily_n_macd_cross_abovezero_n_vol_close_up()
    get_stocks_weekly_4_macd_cross_turnover()
    get_stocks_weekly_5_macd_cross_turnover()
    get_stocks_weekly_6_macd_cross_turnover()
    get_stocks_weekly_7_macd_cross_turnover()
    get_stocks_weekly_8_macd_cross_turnover()
    get_stocks_weekly_9_macd_cross_turnover()
    get_stocks_weekly_10_macd_cross_turnover()
    get_stocks_daily_n_macd_cross_n_dif_up()
    get_stocks_daily_n_macd_diff_low()
    get_stocks_daily_macd_doublecross()
    #VOL
    get_stocks_daily_40_close_low_vol_close_up_max()
    get_stocks_daily_10_turnover_up_vol_close_up()
    #INDUSTRY
    #get_sw_indexs_up_stocks_top_n()
    #MA
    get_stocks_daily_n_ma_stick_macd_diff_abovezero()
    get_stocks_daily_n_ma_up_macd_cross_abovezero()

  