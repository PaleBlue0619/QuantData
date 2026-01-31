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
                                        dbName=dbName, tbName=tbName, isInfo=True, dateCol=dateCol,
                                        logStr=funcName)

    def stockDisclosure(self) -> None:
        """
        股票财务报告披露时间表 -> 判断当前状态 -> 拉取数据
        """
        funcName = inspect.currentframe().f_code.co_name    # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        self.check_getAll_insertAll(dataFunc=get_stock_disclosure, params={"pro": self.pro},
                                    dbName=dbName, tbName=tbName, isInfo=True, dateCol=dateCol,
                                    logStr=funcName)

    def stockDailyBar(self) -> None:
        """
        股票日线表(包含复权因子) -> 判断当前状态 -> 拉取数据
        """
        funcName = inspect.currentframe().f_code.co_name  # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        dateCol = self.tableDict[funcName]["dateCol"]
        self.check_getByDate_insertByDate(dataFunc=get_stock_dailyBar, params={"pro": self.pro},
                                          dbName=dbName, tbName=tbName, isInfo=True, dateCol=dateCol,
                                          logStr=funcName)

    def run(self):
        self.stockInfo()
        self.stockDisclosure()
        self.stockDailyBar()

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
