import time, json, json5, tqdm
import inspect
import pandas as pd
import tushare as ts
import dolphindb as ddb
from tushare.pro.client import DataApi
from src.entity.Source import *
from src.entity.Mode import Mode
from typing import Dict, List

class PipeLine(Mode):
    def __init__(self, token: str, host: str, port: int, userid: str, password: str, startDate: pd.Timestamp,
                 pipelineDict: Dict[str, Dict[str, str]], tableDict: Dict[str, Dict]):
        super().__init__(token, host, port, userid, password, startDate, pipelineDict, tableDict)
        self.pro = ts.pro_api(token=token, timeout=30)
        self.pipelineDict = pipelineDict
        self.tableDict = tableDict
        self.startDate = pd.Timestamp(startDate)
        self.currentDate = pd.Timestamp.now().date()
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.session = ddb.session(host, port, userid, password)

    def stockInfo(self) -> None:
        """
        股票基本信息表(静态信息表) -> 删除 -> 重建
        """
        funcName = inspect.currentframe().f_code.co_name    # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        self.deleteAll_getAll_insertAll(dataFunc=get_stock_info, params={"pro": self.pro},
                                    dbName=dbName, tbName=tbName, isInfo=True, dateCol=dateCol)

    def stockDisclosure(self) -> None:
        """
        股票财务报告披露时间表 -> 判断当前状态 -> 拉取数据
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
        data = get_stock_disclosure(self.pro, totalDateList)
        t1 = time.time()
        if data.empty:
            print("stockDisclosure 获取为空!")
            return
        self.insertToDDB(session, dbName=dbName, tbName=tbName, data=data,
                         isInfo=False, dateCol=dateCol, timeCost=t1-t0)

    def stockDailyBar(self) -> None:
        """
        股票日线表(包含复权因子) -> 判断当前状态 -> 拉取数据
        """
        funcName = inspect.currentframe().f_code.co_name  # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        session = ddb.session(self.host, self.port, self.userid, self.password)
        state = self.getState(dbName, tbName)
        if state == 0:  # 说明是第一次运行 -> 大批量拉取
            startDate = self.startDate
            endDate = self.currentDate
        else:   # 说明不是第一次运行 -> 小批量拉取
            startDate = self.getLastDate(dbName, tbName) + pd.Timedelta(1, "D")  # 开始日期
            endDate = self.currentDate
        totalDateList = self.get_totalDate(startDate, endDate, freq="D")
        for date in tqdm.tqdm(totalDateList, desc="fetching stockDayBar..."):
            t0 = time.time()
            data = get_stock_dailyBar(self.pro, dateList=[date])
            t1 = time.time()
            if data.empty:
                continue
            self.insertToDDB(session, dbName=dbName, tbName=tbName, data=data,
                             isInfo=False, dateCol=dateCol, timeCost=t1-t0)

    def run(self):
        self.stockInfo()
        # self.stockDisclosure()
        # self.stockDailyBar()

if __name__ == "__main__":
    with open(r"D:\DolphinDB\Project\QuantData\src\cons\pipeline.json5","r",encoding="utf-8") as f:
        pipelineDict = json5.load(f)
    with open(r"D:\DolphinDB\Project\QuantData\src\cons\table.json5","r",encoding="utf-8") as f:
        tableDict = json5.load(f)
    pipe = PipeLine(token="d335278c1e7d55ac97971ce6ab30f0c93cff6212d2625f7f98bec144",
                    host="localhost", port=8848, userid="admin", password="123456",
                    startDate=pd.Timestamp("20100101"),
                    pipelineDict=pipelineDict,
                    tableDict=tableDict)
    pipe.run()
