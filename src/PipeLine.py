import json, json5
import inspect
import pandas as pd
import tushare as ts
import dolphindb as ddb
from tushare.pro.client import DataApi
from dagster import asset, AssetGroup, Definitions, ResourceDefinition, ScheduleDefinition, define_asset_job
from src.DataSource import *
from typing import Dict, List

class PipeLine:
    def __init__(self, token: str, host: str, post: int,
                 userid: str, password: str,
                 startDate: pd.Timestamp,
                 endDate: pd.Timestamp,
                 pipelineDict: Dict[str, Dict[str, str]]):
        self.token = token
        self.host = host
        self.post = post
        self.userid = userid
        self.password = password
        self.pro = ts.pro_api(token=token, timeout=30)
        self.pipelineDict = pipelineDict
        self.startDate = startDate
        self.endDate = endDate

    @asset(deps=[get_stock_basic], description="获取股票基本信息->DDB", group_name="pipeline")
    def stockBasic(self, data: pd.DataFrame) -> None:
        funcName = inspect.currentframe().f_code.co_name    # 获取当前函数名称
        dbName = self.pipelineDict[funcName]["dbName"]
        tbName = self.pipelineDict[funcName]["tbName"]
        session = ddb.session(self.host, self.port, self.userid, self.password)
        insertToDDB(session, dbName, tbName, data)

    @job
    def stockDataJob(self):
        """
        股票模块的任务链: 后续可添加
        """
        data = get_stock_basic(self.pro)
        self.stockBasic(data)

    def run(self):
        """执行DAG任务调度"""
        # 定义Dagster资源
        defs = Definitions(
            jobs=[self.stockDataJob],
            resources={

            }
        )


        return

if __name__ == "__main__":
    with open("D:\DolphinDB\Project\QuantData\src\cons\pipeline.json5","r",encoding="utf-8") as f:
        pipelineDict = json5.load(f)
    pipe = PipeLine(token="3879419d53b98d0972af48d506fb42b1aeb8b6699c8c926dfffbe5db",
                    host="localhost",post=8848,userid="admin",password="123456",
                    startDate=pd.Timestamp("20100101"), endDate=None,
                    pipelineDict=pipelineDict)

