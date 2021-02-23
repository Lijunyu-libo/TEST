# -*- coding:utf-8 -*- 
'''
@author: JM
'''
import pandas as pd
import tushare as ts
import json
import logging, sys
# MONGODB CONNECT
from pymongo import MongoClient
import json
#client = MongoClient('mongodb://112.12.60.2:27017')
client = MongoClient('mongodb://127.0.0.1:27017')
mydb=client["ptest"]
#tushare初始化
ts.set_token('78282dabb315ee578fb73a9b328f493026e97d5af709acb331b7b348')
pro = ts.pro_api()
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
logger = logging.getLogger(__name__)

"""
沪深港通实时资金流向：HQ_MF_HSGT
返回值：[time, hk2sh_net, hk2sh_remain, hk2sz_net, hk2sz_remain, sh2hk_net, sh2hk_remain, sz2hk_net, sz2hk_remain, north_money, south_monety]
time:时间int，e.g. : 110212表示11点02分12秒
hk2sh_net:沪股通流入净额（万元，下同）
hk2sh_remain：沪股通余额
hk2sz_net:深股通流入净额
hk2sz_remain：深股通余额
sh2hk_net:港股通（沪）流入净额
sh2hk_remain：港股通（沪）余额
sz2hk_net:港股通（深）流入净额
sz2hk_remain：港股通（深）余额
north_money：北向资金
south_monety：南向资金

HQ_STK_MIN字段：
TS_CODE:股票代码（e.g.: 000001.SZ 600000.SH） 0
NAME:股票名称 1
TRADE_TIME:交易时间（交易所）2
OPEN:开盘价 3
HIGH:最高价 4
LOW:最低价 5
CLOSE:收盘价 6
VOL:成交量 7
AMOUNT:成交金额 8

HQ_STK_TICK字段：
TS_CODE:股票代码（e.g.: 000001.SZ 600000.SH）
NAME:股票名称 
TRADE_TIME:交易时间（交易所）
PRICE:当前最新价
PRE_CLOSE:前收盘
OPEN:开盘价
HIGH:最高价
LOW:最低价
CLOSE:收盘价
VOL:成交量
AMOUNT:成交金额
ASK_P1:卖一价
ASK_V1:卖一量
ASK_P2:卖二价
ASK_V2:卖二量
ASK_P3:卖三价
ASK_V3:卖三量
ASK_P4:卖四价
ASK_V4:卖四量
ASK_P5:卖五价
ASK_V5:卖五量
BID_P1:买一价
BID_V1:买一量
BID_P2:买二价
BID_V2:买二量
BID_P3:买三价
BID_V3:买三量
BID_P4:买四价
BID_V4:买四量
BID_P5:买五价
BID_V5:买五量


"""
#market 主板 科创板 主板 中小板 创业板
def get_stockbasket(exchange,market):
    data = pro.stock_basic(exchange=exchange, list_status='L')
    data = data[~ data['name'].str.contains('ST|退')]
    if (market!=''):
        data = data[data['market']==market]
    data['ts_name'] = data['name']
    return data

#获取stockcode条件集合函数 参数 stockcode 返回df
def get_df_stockcode(col):
    mycollection=mydb[col]
    rs_stockcode = mycollection.find()
    list_stockcode = list(rs_stockcode)
    #将查询结果转换为Df
    df_stockcode = pd.DataFrame(list_stockcode)
    #print (df_stockcode)
    return df_stockcode

def get_stocks_daily_macd_cross():
    result_list=[]
    df_stocks = get_stockbasket('','')
    for stockcode in df_stocks['ts_code']:
        #获取前复权数据，默认为日期降序
        df_qfq = get_df_stockcode('daily_qfq_macd_'+stockcode)
        if (df_qfq is None or len(df_qfq)<2):
            continue
        else:
            DIF_1 = round(df_qfq['DIF'][1],4)
            DEA_1 = round(df_qfq['DEA'][1],4)
            DIF_0 = round(df_qfq['DIF'][0],4)
            DEA_0 = round(df_qfq['DEA'][0],4)
            #MACD_0 = round(df_qfq['MACD'][0],4)
            #MACD_1 = round(df_qfq['MACD'][1],4)
            #print (df_qfq['trade_date'][0],df_qfq['close'][0],DIF_0,DEA_0,MACD_0)
            if (DIF_1 < DEA_1 and DIF_0 > DEA_0 and DIF_1 > 0):
                print (stockcode,df_qfq['trade_date'][0],df_qfq['close'][0],DIF_1,DEA_1,DIF_0,DEA_0)
                result_list.append(stockcode)
    print (len(result_list))
    return result_list

#target_list = get_stocks_daily_macd_cross()
def sub_hsgt_moneyflow():    
    app = ts.subs()
    @app.register(topic='HQ_STK_MIN', codes=['1MIN:00*.SZ'])
    #@app.register(topic='HQ_STK_MIN', codes=['1MIN:*.*'])
    def data_back(record):
        #在这里处理数据
        upratio = round((record[6]-record[3])/record[3],3)
        if (upratio>=0.00):
            print(record[0],upratio)
    app.run()

if __name__ == '__main__':
    sub_hsgt_moneyflow()
