import pandas as pd
import dolphindb as ddb
from typing import Dict,List

class Table:
    def __init__(self, isTSDB: bool = None, isInfo: bool = None, dbName: str = None, tbName: str = None,
                 indicator: Dict = None, addCreateTime: bool = None,
                 dateCol: str = None,
                 partitionCol: List[str] = None,
                 sortCol: List[str] = None, keepDuplicates: bool = None):
        self.dbName = dbName
        self.tbName = tbName
        self.indicator = indicator
        self.addCreateTime = addCreateTime
        self.partitionCol = partitionCol
        self.isInfo = isInfo
        self.dateCol = dateCol
        # TSDB特有属性
        self.sortCol = sortCol
        self.keepDuplicates = keepDuplicates
        self.isTSDB = isTSDB

    def fromDict(self, tableDict: Dict[str, any]):
        """从字典进行初始化的构造函数"""
        self.dbName = tableDict["dbName"]
        self.tbName = tableDict["tbName"]
        self.indicator = tableDict["indicator"]
        self.addCreateTime = tableDict["addCreateTime"]
        self.partitionCol = tableDict["partitionCol"]
        self.isInfo = tableDict["isInfo"]
        self.dateCol = tableDict["dateCol"]
        if self.isTSDB:
            self.sortCol = tableDict["sortCol"]
            self.keepDuplicates = tableDict["keepDuplicates"]
        if "createTime" not in self.indicator and self.addCreateTime:
            self.indicator["createTime"] = "TIMESTAMP"


