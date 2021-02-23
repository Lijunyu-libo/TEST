# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 10:39:58 2020
策略汇总调度
@author: iFunk
"""
import pandas as pd
import numpy as np
#策略工具
from strategies import caltools
from strategies import stockbasket
from strategies import dailybasic

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

#demo
def get_stocks_daily_macd_cross():
    macd.get_stocks_daily_macd_cross
    