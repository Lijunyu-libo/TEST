# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 14:37:58 2020

@author: 李博
"""
import datetime
import calendar

#获取当前日期年月日
y = datetime.datetime.now().year
m = datetime.datetime.now().month
d = datetime.datetime.now().day
date = datetime.datetime.now().date()

#获取当前星期
w = datetime.datetime.now().weekday()
print (y,m,d,date,w)

#获取本月第一天和最后一天的日期
m_d_range = calendar.monthrange(y,m)
print (y,m,m_d_range[0],y,m,m_d_range[1])

#获取本周一、五、日日期
this_week_start = datetime.datetime.now() - datetime.timedelta(days = w)
this_week_end = datetime.datetime.now() + datetime.timedelta(days = 6 - w)
this_week_fri = datetime.datetime.now() + datetime.timedelta(days = 4 - w)
print (this_week_start.date(),this_week_fri.date(),this_week_end.date())


