import pandas as pd
import dolphindb as ddb
from typing import List, Dict

"""
Operator为数据层与数据源的交互，可以分为以下几种情况:
1. 清空数据(truncate) -> 
2. 删除指定时间段内的数据 -> (必然非静态维度表) -> 更新状态keyedTable
3. 添加数据 -> 是否为静态维度表 -> 更新状态keyedTable
"""

class Operator:
    def __init__(self):
        return

    @staticmethod
    def refreshState(session: ddb.session, dbName: str, tbName: str,
                    updateTime: pd.Timestamp, state: int,
                    isInfo: bool = False, dateCol: str = None):
        """刷新状态: 即重新统计该表的状态"""
        updateTimeStr = pd.Timestamp(updateTime).strftime("%Y.%m.%d %H:%M:%S.%f")
        session.run(f"""
        isInfo = {int(isInfo)}
        tab = objByName(`sys,true);  // 反射获取共享键值表对象
        
        // 原封不动的属性
        dbName_ = "{dbName}"
        tbName_ = "{tbName}"
        
        // 传入的属性
        updateTime_ = temporalParse("{updateTimeStr}","yyyy.MM.dd HH:mm:ss.SSS")
        state_ = int({state}); // 初始化状态
        count = exec count(*) from tab where dbName = dbName_ and tbName = tbName_
        if (count == 0){{
            createTime_ = updateTime_; // 说明此时sys中还没有这条记录 -> updateTime = createTime
        }}else{{
            createTime_ = exec createTime from tab where dbName = dbName_ and tbName = tbName_
        }}
         
        // 更新的属性 -> firstDate lastDate
        pt = loadTable(dbName_, tbName_)
        if (isInfo == 0){{
            firstDate_ = exec min({dateCol}) from pt;
            lastDate_ = exec max({dateCol}) from pt;
        }}else{{
            firstDate_ = NULL;
            lastDate_ = NULL;
        }}
        insert into tab values(
            dbName_, tbName_, createTime_, updateTime_, firstDate_, lastDate_, state_);
        """)


    @staticmethod
    def deleteFromDDB(session: ddb.session, dbName: str, tbName: str):
        """清空DDB表中的数据 -> 重置状态为0
        若 isInfo为True -> 说明为静态信息的表, 则不进行任何日志的记录与维护
        """
        session.run(f"""
        truncate("{dbName}", "{tbName}")
        """)
        timestampStr = pd.Timestamp.now().strftime("%Y.%m.%d %H:%M:%S.%f")
        session.run(f"""
        tab = objByName(`sys,true);  // 反射获取共享键值表对象
        dbName_ = "{dbName}"
        tbName_ = "{tbName}"
        createTime_ = temporalParse("{timestampStr}","yyyy.MM.dd HH:mm:ss.SSS")
        updateTime_ = createTime_
        firstDate_ = NULL
        lastDate_ = NULL
        state_ = 0
        insert into tab values(
            dbName_, tbName_, createTime_, updateTime_, firstDate_, lastDate_, state_);
        """)

    @staticmethod
    def deleteDateFromDDB(session: ddb.session, dbName: str, tbName: str, dateCol: str,
                          startDate: pd.Timestamp = None, endDate: pd.Timestamp = None):
        """
        清空DDB表中某个时间区间的数据 -> 重置表的相关状态属性
        """
        if not startDate:
            startDate = pd.Timestamp("19900101")
        if not endDate:
            endDate = pd.Timestamp("20400101")
        startDotDate = pd.Timestamp(startDate).strftime("%Y.%m.%d")
        endDotDate = pd.Timestamp(endDate).strftime("%Y.%m.%d")
        session.run(f"""
        delete from loadTable("{dbName}","{tbName}") where {dateCol} between {startDotDate} and {endDotDate};
        """)
        state: int = 1  # 上次正常运行
        updateTime = pd.Timestamp.now()
        Operator.refreshState(session=session, dbName=dbName, tbName=tbName,
                              updateTime=updateTime, state=state,
                              isInfo=False, dateCol=dateCol)

    @staticmethod
    def insertToDDB(session: ddb.session, dbName: str, tbName: str, data: pd.DataFrame,
                    isInfo: bool = False, dateCol: str = None):
        """
        DDB tableInsert 同步写入
        """
        tableAppender = ddb.TableAppender(dbPath=dbName, tableName=tbName, ddbSession=session)
        tableAppender.append(data)
        state: int = 1  # 上次正常运行
        updateTime = pd.Timestamp.now()
        if isInfo:
            Operator.refreshState(session=session, dbName=dbName, tbName=tbName,
                              updateTime=updateTime, state=state,
                              isInfo=True)
        else:
            Operator.refreshState(session=session, dbName=dbName, tbName=tbName,
                              updateTime=updateTime, state=state,
                              isInfo=False, dateCol=dateCol)

    # @staticmethod
    # def insertToDBBMtw(writer: ddb.MultithreadedTableWriter, data: pd.DataFrame,
    #                    strCols: List[str] = None, dateCols: List[str] = None,
    #                    floatCols: List[str] = None, intCols: List[str] = None,
    #                    timestampCols: List[str] = None) -> ddb.MultithreadedTableWriterStatus:
    #     """
    #     DDB MultiTableWriter 异步写入
    #     若需要内部批量处理可以输入对应的列名, 也可以全为None处理完了再丢进来
    #     """
    #     # 预先批量转换数据
    #     if strCols:
    #         for col in strCols:
    #             data[col] = data[col].apply(lambda x: str(x))
    #     if dateCols:
    #         for col in dateCols:
    #             data[col] = data[col].apply(pd.Timestamp)
    #     if floatCols:
    #         for col in floatCols:
    #             data[col] = pd.to_numeric(data[col], errors="coerce")
    #     if intCols:
    #         for col in intCols:
    #             data[col] = data[col].apply(int)
    #     if timestampCols:
    #         for col in timestampCols:
    #             data[col] = data[col].apply(pd.Timestamp)
    #
    #     # 循环插入数据
    #     for row in df_converted.itertuples():
    #         values = list(row[1:])  # 跳过索引
    #         writer.insert(*values)
    #
    #     writer.waitForThreadCompletion()
    #     return writer.getStatus()