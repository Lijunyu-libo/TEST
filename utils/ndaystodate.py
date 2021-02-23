# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 15:57:04 2020
说明：时间段计算模块
功能：计算今日起向前N日的具体日期，返回格式“20200704”
@author: libo
"""
import datetime


#计算指定日期的前N天的时间戳
def get_day_time(n):
    #the_date = datetime.datetime(2018,11,10) #指定当前日期 2018-11-10
    the_date = datetime.datetime.now()
    #the_date_str = the_date.strftime('%Y%m%d')
    pre_date = the_date - datetime.timedelta(days=n-1)
    pre_date_str = pre_date.strftime('%Y%m%d')#将日期转换为指定的显示格式
    #print(the_date_str)
    print(pre_date_str)
    return pre_date
get_day_time(1)
get_day_time(2)
get_day_time(3)
get_day_time(5)
get_day_time(15)
get_day_time(30)
