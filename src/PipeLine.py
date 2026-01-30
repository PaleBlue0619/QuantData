import time, json, json5
import inspect
import pandas as pd
import tushare as ts
import dolphindb as ddb
from tushare.pro.client import DataApi
from src.time.Time import Time
from src.entity.Source import *
from src.entity.Operator import Operator
from typing import Dict, List

class PipeLine(Operator, Time):
    def __init__(self, token: str, host: str, port: int, userid: str, password: str,
                 startDate: pd.Timestamp, pipelineDict: Dict[str, Dict[str, str]], tableDict: Dict[str, Dict]):
        super().__init__()
        self.pro = ts.pro_api(token=token, timeout=30)
        self.pipelineDict = pipelineDict
        self.tableDict = tableDict
        self.startDate = pd.Timestamp(startDate)
        self.currentDate = pd.Timestamp.now().date()
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.startDate = startDate
        self.session = ddb.session(host, port, userid, password)

    def getState(self, dbName, tbName) -> int:
        return self.session.run(f"""
        count = exec count(*) from objByName(`sys, true) where dbName="{dbName}" and tbName="{tbName}"
        if (count == 0){{
            state = 0
        }}else{{
            state = exec state from objByName(`sys, true) where dbName="{dbName}" and tbName="{tbName}"
        }}
        state;
        """)

    def getLastDate(self, dbName, tbName) -> pd.Timestamp:
        resDict = self.session.run(f"""
        count = exec count(*) from objByName(`sys, true) where dbName="{dbName}" and tbName="{tbName}"
        if (count == 0){{
            lastDate_ = NULL
        }}else{{
            lastDate_ = exec lastDate from objByName(`sys, true) where dbName="{dbName}" and tbName="{tbName}"
        }}
        resDict = dict(STRING, DATE);
        resDict["lastDate"] = lastDate_;
        resDict;
        """)
        lastDate = resDict["lastDate"]
        if lastDate is None:
            return pd.Timestamp(self.startDate)
        else:
            return pd.Timestamp(lastDate)

    def stockBasic(self) -> None:
        """
        股票基本信息表(静态信息表) -> 删除 -> 重建
        """
        funcName = inspect.currentframe().f_code.co_name    # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        session = ddb.session(self.host, self.port, self.userid, self.password)
        self.deleteFromDDB(session, dbName, tbName)
        t0 = time.time()
        data = get_stock_basic(self.pro)
        t1 = time.time()
        if data.empty:
            print("stockBasic 获取为空!")
            return
        self.insertToDDB(session, dbName=dbName, tbName=tbName, data=data,
                         isInfo=True, dateCol=dateCol, timeCost=t1-t0)

    def stockDisclosure(self) -> None:
        """
        股票财务报告披露时间表 ->
        """
        funcName = inspect.currentframe().f_code.co_name    # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        session = ddb.session(self.host, self.port, self.userid, self.password)
        state = self.getState(dbName, tbName)
        if state == 0:
            totalDateList = self.get_totalDate(self.startDate, self.currentDate, freq="Q")
        else:
            nextDate = self.getLastDate(dbName, tbName) + pd.Timedelta(1,"D")
            totalDateList = self.get_totalDate(nextDate, self.currentDate, freq="Q")
        t0 = time.time()
        data = get_disclosure(self.pro, totalDateList)
        t1 = time.time()
        if data.empty:
            print("stockDisclosure 获取为空!")
            return
        self.insertToDDB(session, dbName=dbName, tbName=tbName, data=data,
                         isInfo=False, dateCol=dateCol, timeCost=t1-t0)

    def run(self):
        """
        运行该函数,期望得到以下效果:
        1. 没有日的相关数据就补起来
        2.
        """
        self.stockBasic()
        self.stockDisclosure()

if __name__ == "__main__":
    with open(r"D:\DolphinDB\Project\QuantData\src\cons\pipeline.json5","r",encoding="utf-8") as f:
        pipelineDict = json5.load(f)
    with open(r"D:\DolphinDB\Project\QuantData\src\cons\table.json5","r",encoding="utf-8") as f:
        tableDict = json5.load(f)
    pipe = PipeLine(token="ffc4899988221cf550031a4c1c8494f00ab589403ac1c24ac692871b",
                    host="localhost", port=8848, userid="admin", password="123456",
                    startDate=pd.Timestamp("20100101"),
                    pipelineDict=pipelineDict,
                    tableDict=tableDict)
    pipe.run()
