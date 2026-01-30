import os,json,json5
from src.entity.Table import Table
from typing import Dict,List
import pandas as pd
import dolphindb as ddb

class DataCenter:
    def __init__(self, session: ddb.session):
        self.tableDict: Dict[str, Table] = {}
        self.session = session

    def fromDict(self, Dict: Dict[str, any]):
        """从字典初始化"""
        for tableName, tableDict in Dict.items():
            tableObj = Table()
            tableObj.fromDict(tableDict=tableDict)
            self.tableDict[tableName] = tableObj

    def init(self):
        """
        创建全局状态共享键值表 -> ("sys")
        dbName tbName createTime updateTime firstDate lastDate nextDate state timeCost
        数据库名(排序列1) 数据表名(排序列2) 创建时间 更新时间 表内最小日期 表内最大日期 上次耗时(s)
        """
        tbName = "sys"
        indexCols = ["dbName","tbName"]
        colNames = ["dbName","tbName","createTime","updateTime","firstDate","lastDate","state","timeCost"]
        colTypes = ["STRING","STRING","TIMESTAMP","TIMESTAMP","DATE","DATE","INT","DOUBLE"]
        # 判断是否存在该表 -> 不存在则创建
        self.session.run(f"""
        if (defined(`sys,SHARED) == 0){{
            // 说明当前全局共享状态键值表不存在 -> 进行创建   
            tab = keyedTable({indexCols},1:0,{colNames},{colTypes})
            share(tab,"{tbName}")
        }}
        """)

    def createTB(self, unEqualDelete: bool):
        """
        创建数据库+数据表
        unequalDelete: bool: 是否不一致就直接删除
        """
        for _, tableObj in self.tableDict.items():
            dbName, tbName = tableObj.dbName, tableObj.tbName
            colNames = list(tableObj.indicator.keys())
            colTypes = list(tableObj.indicator.values())
            colDict = dict(zip(colNames, colTypes))
            partitionCol = tableObj.partitionCol
            # Step1. 检查是否存在数据库 -> 不存在则创建该数据库
            if not self.session.existsDatabase(dbUrl=dbName):
                self.session.run(f"""
                db = database("{dbName}", RANGE, 1990.01M..2030.01M, engine="OLAP")
                """)

            # Step2. 如果存在该数据表 + 若不一致则删除 -> 检查是colNames & colTypes & partitionCol 是否对得上
            if self.session.existsTable(dbUrl=dbName, tableName=tbName) and unEqualDelete:
                # 获取当前数据表的属性
                colDefs = self.session.run(f"""
                schema(loadTable("{dbName}","{tbName}"))["colDefs"]
                """)
                colNames_ = colDefs["name"].tolist()
                colTypes_ = colDefs["typeString"].tolist()
                colDict_ = dict(zip(colNames_, colTypes_))
                isEqual: bool = True    # 假定一开始是相等的
                if len(colDict_)!=len(colDict):
                    isEqual = False
                else:
                    for key, value in colDict_.items():
                        if key not in colDict:
                            isEqual = False
                            break
                        if colDict_[key]!= colDict[key]:
                            isEqual = False
                            break
                if not isEqual: # 说明数据库不一致 + 需要被删除
                    self.session.dropTable(dbPath=dbName, tableName=tbName)

            # Step3. 检查在该数据库下存在数据表 -> 不存在则创建该数据表
            if not self.session.existsTable(dbUrl=dbName,tableName=tbName):
                self.session.run(f"""
                db = database("{dbName}")
                colNames = {colNames}
                colTypes = {colTypes}
                schemaTb = table(1:0, colNames, colTypes)
                db.createPartitionedTable(schemaTb, tableName="{tbName}", partitionColumns={partitionCol})
                """)

if __name__ == "__main__":
    with open(r"D:\DolphinDB\Project\QuantData\src\cons\table.json5","r",encoding='utf-8') as f:
        tableDict = json5.load(f)
    DCObj = DataCenter(session=ddb.session("localhost",8848,"admin","123456"))
    DCObj.fromDict(Dict=tableDict)
    DCObj.init()
    DCObj.createTB(unEqualDelete=True)