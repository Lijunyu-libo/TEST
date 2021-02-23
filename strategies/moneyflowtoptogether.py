# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 10:31:10 2020
策略执行基本模块
@author: iFunk
"""
import pandas as pd
#import moneyflow
from strategies import moneyflow

def moneytoptogether(df):
    df_result_net_top = moneyflow.get_moneyflow_last_netmfamount_top10(df)
    df_result_buyelg_top = moneyflow.get_moneyflow_last_buyelgamount_top10(df)
    df_result_buylg_top = moneyflow.get_moneyflow_last_buylgamount_top10(df)
    df_temp = pd.merge(df_result_net_top,df_result_buyelg_top)
    result = pd.merge(df_temp,df_result_buylg_top)
    return result