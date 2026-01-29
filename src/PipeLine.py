import pandas as pd
import tushare as ts
import dolphindb as ddb

class PipeLine:
    def __init__(self, token: str):
        self.pro = ts.pro_api(token=token, timeout=30)

    def run(self):
        """执行DAG任务调度"""
