import pandas as pd
import dolphindb as ddb
import tushare as ts
from tushare.pro.client import DataApi
from typing import List,Dict
pd.set_option("display.max_columns", None)

def get_stock_basic(pro: DataApi) -> pd.DataFrame:
    """股票基本信息"""
    fields = "ts_code,symbol,name,area,industry,market,exchange,curr_type,list_status,list_date,delist_date"
    data = pro.stock_basic(exchange='', fields=fields)
    data["list_date"] = data["list_date"].apply(pd.Timestamp)
    data["delist_date"] = data["delist_date"].apply(pd.Timestamp)
    data["createTime"] = pd.Timestamp.now()
    return data

def get_disclosure(pro: DataApi, dateList: List[pd.Timestamp]) -> pd.DataFrame:
    """财报披露时间表"""
    totalData = []
    fields = "ts_code,ann_date,end_date,pre_date,actual_date,modify_date"
    for date in dateList:
        data = pro.disclosure_date(end_date=date.strftime("%Y%m%d"), fields=fields)
        data["ann_date"] = data["ann_date"].apply(pd.Timestamp)
        data["end_date"] = data["end_date"].apply(pd.Timestamp)
        data["pre_date"] = data["pre_date"].apply(pd.Timestamp)
        data["actual_date"] = data["actual_date"].apply(pd.Timestamp)
        data["modify_date"] = data["modify_date"].apply(str)
        data["createTime"] = pd.Timestamp.now()
        totalData.append(data)
    return pd.concat(totalData, axis=0, ignore_index=True)