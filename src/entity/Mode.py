import tqdm
import time
import tushare as ts
import pandas as pd
import dolphindb as ddb
from src.time.Time import Time
from src.entity.Operator import Operator
from typing import List, Dict, Callable

"""
Mode模块是为了减少判断状态 -> 拉数据 -> 传数据+记录的重复代码的
例如stockDailyBar与stockDailyBasic的Mode一定是完全一致的，
两者的唯一不同就是调的API不同->执行函数不同->写入的数据库不同
因而可以被归类在同一个Mode中, 以此类推
"""

class Mode(Operator, Time):
    def __init__(self, token: str, host: str, port: int, userid: str, password: str, startDate: pd.Timestamp,
                 pipelineDict: Dict[str, Dict[str, str]], tableDict: Dict[str, Dict]):
        super().__init__(host, port, userid, password, startDate)
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.session = ddb.session(host, port, userid, password)
        self.token = token
        self.pro = ts.pro_api(token=token, timeout=30)
        self.pipelineDict = pipelineDict
        self.currentDate = pd.Timestamp.now().date()
        self.nextDate = self.currentDate + pd.Timedelta(1, "D")
        self.startDate = pd.Timestamp(startDate)
        self.tableDict = tableDict

    def deleteAll_getAll_insertAll(self, dbName: str, tbName: str, isInfo: bool, dateCol: str,
                                   dataFunc: Callable, params: Dict[str, any] = None, logStr: str = ""):
        """删除ALL -> 拉取全量数据 -> 插入, 适用于: 不定期更新的静态信息 + 整体数据较少的情况
        1. 静态信息表
        """
        session = ddb.session(self.host, self.port, self.userid, self.password)
        self.deleteFromDDB(session, dbName, tbName) # deleteAll
        t0 = time.time()
        data: pd.DataFrame = dataFunc(**params)
        t1 = time.time()
        if data.empty:
            print(dbName,"/",tbName,f":{logStr}插入为空")
            return
        self.insertToDDB(session, dbName, tbName, data,
                         isInfo=isInfo, dateCol=dateCol,
                         timeCost=t1-t0)

    def check_getAll_insertAll(self, dbName: str, tbName: str, isInfo: bool, dateCol: str,
                               dataFunc: Callable, params: Dict[str, any] = None, logStr: str = ""):
        """
        判断当前状态 -> 拉取所有增量数据,
        适用于每日增量更新 + 整体数据较少的情况
        1. 财报预计披露日期
        """
        session = ddb.session(self.host, self.port, self.userid, self.password)
        state = self.getState(dbName=dbName, tbName=tbName)
        if state == 0:
            totalDateList = self.get_totalDate(self.startDate, self.currentDate, freq="Q")
        else:
            nextDate = self.getLastDate(dbName, tbName) + pd.Timedelta(1, "D")
            totalDateList = self.get_totalDate(nextDate, self.currentDate, freq="Q")
        t0 = time.time()
        data = dataFunc(**params, dateList=totalDateList)
        t1 = time.time()
        if data.empty:
            print(dbName,"/", tbName, f":{logStr}插入为空")
            return
        self.insertToDDB(session, dbName, tbName, data,
                         isInfo=isInfo, dateCol=dateCol,
                         timeCost=t1-t0)

    def check_getByDate_insertByDate(self, dbName: str, tbName: str, isInfo: bool, dateCol: str,
                                dataFunc: Callable, params: Dict[str, any] = None, logStr: str = ""):
        """
        判断当前状态 -> for loop(date: 拉取增量数据 -> 插入),
        适用于每日增量更新 + 整体数据较多的情况(不能一次性拉完 + for拉完要等很久,最好边拉边写)
        1. 日K线 & 日特征
        """
        session = ddb.session(self.host, self.port, self.userid, self.password)
        state = self.getState(dbName=dbName, tbName=tbName)
        if state == 0:  # 说明是第一次运行 -> 大批量拉取
            startDate = self.startDate
            endDate = self.currentDate
        else:  # 说明不是第一次运行 -> 小批量拉取
            startDate = self.getLastDate(dbName, tbName) + pd.Timedelta(1, "D")  # 开始日期
            endDate = self.currentDate
        totalDateList = self.get_totalDate(startDate, endDate, freq="D")
        for date in tqdm.tqdm(totalDateList, desc=f"{logStr} fetching..."):
            t0 = time.time()
            data = dataFunc(**params, dateList=[date])
            t1 = time.time()
            if data.empty:
                continue
            self.insertToDDB(session, dbName=dbName, tbName=tbName, data=data,
                             isInfo=False, dateCol=dateCol, timeCost=t1-t0)


