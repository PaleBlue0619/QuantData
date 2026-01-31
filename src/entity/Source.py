import pandas as pd
import dolphindb as ddb
import tushare as ts
from tushare.pro.client import DataApi
from typing import List,Dict
pd.set_option("display.max_columns", None)

def get_stock_info(pro: DataApi) -> pd.DataFrame:
    """股票基本信息"""
    fields = "ts_code,symbol,name,area,industry,market,exchange,curr_type,list_status,list_date,delist_date"
    data = pro.stock_basic(exchange='', fields=fields)
    data["list_date"] = data["list_date"].apply(pd.Timestamp)
    data["delist_date"] = data["delist_date"].apply(pd.Timestamp)
    data["createTime"] = pd.Timestamp.now()
    return data

def get_stock_disclosure(pro: DataApi, dateList: List[pd.Timestamp]) -> pd.DataFrame:
    """财报披露时间表"""
    totalData = []
    fields = "ts_code,ann_date,end_date,pre_date,actual_date,modify_date"
    for date in dateList:
        data = pro.disclosure_date(end_date=date.strftime("%Y%m%d"), fields=fields)
        totalData.append(data)
    totalData = pd.concat(totalData, axis=0, ignore_index=True)
    totalData["ann_date"] = totalData["ann_date"].apply(pd.Timestamp)
    totalData["end_date"] = totalData["end_date"].apply(pd.Timestamp)
    totalData["pre_date"] = totalData["pre_date"].apply(pd.Timestamp)
    totalData["actual_date"] = totalData["actual_date"].apply(pd.Timestamp)
    totalData["modify_date"] = totalData["modify_date"].apply(str)
    totalData["createTime"] = pd.Timestamp.now()
    return totalData

def get_stock_dailyBar(pro: DataApi, dateList: List[pd.Timestamp]) -> pd.DataFrame:
    """股票日K数据"""
    totalData = []
    fields = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
    for date in dateList:
        data = pro.daily(trade_date=date.strftime("%Y%m%d"), fields=fields)
        totalData.append(data)
    totalData = pd.concat(totalData, axis=0, ignore_index=True)
    totalData["trade_date"] = totalData["trade_date"].apply(pd.Timestamp)

    adjFactor = []
    fields = "ts_code,trade_date,adj_factor"
    for date in dateList:
        data = pro.adj_factor(trade_date=date.strftime("%Y%m%d"), fields=fields)
        adjFactor.append(data)
    adjFactor = pd.concat(adjFactor, axis=0, ignore_index=True)
    adjFactor["trade_date"] = adjFactor["trade_date"].apply(pd.Timestamp)

    totalData = totalData.merge(adjFactor, on=["ts_code", "trade_date"], how="left")
    totalData["vol"] = totalData["vol"].apply(int)
    totalData["createTime"] = pd.Timestamp.now()
    return totalData

def get_stock_dailyBasic(pro: DataApi, dateList: List[pd.Timestamp]) -> pd.Timestamp:
    """股票日频财务数据"""
    totalData = []
    fields = "ts_code,trade_date,close,turnover_rate,turnover_rate_f,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,total_share,float_share,free_share,total_mv,circ_mv"
    for date in dateList:
        data = pro.daily_basic(trade_date=date.strftime("%Y%m%d"), fields=fields)
        totalData.append(data)
    totalData = pd.concat(totalData, axis=0, ignore_index=True)
    totalData["trade_date"] = totalData["trade_date"].apply(pd.Timestamp)
    totalData["createTime"] = pd.Timestamp.now()
    return totalData