import pandas as pd
import numpy as np
import dolphindb as ddb
from typing import List, Dict, Optional

class Time:
    """
    时间模块
    """
    def __init__(self, session: ddb.session):
        self.currentDate = pd.Timestamp.date(pd.Timestamp.now())
        self.currentTime = pd.Timestamp.now()
        self.nextDate = self.currentDate + pd.Timedelta(1, "D")
        self.session = session

    def get_totalDate(self, startDate, endDate, freq="D") -> List[pd.Timestamp]:
        """
        获取不同频率的时间列表
        """
        if freq == "D":
            return self.session.run(f"""
            startDate = {pd.Timestamp(startDate).strftime("%Y.%m.%d")}
            endDate = {pd.Timestamp(endDate).strftime("%Y.%m.%d")}
            table(startDate..endDate as `totalDay)
            """)["totalDay"].tolist()
        elif freq == "M":
            return self.session.run(f"""
            startDate = {pd.Timestamp(startDate).strftime("%Y.%m.%d")}
            endDate = {pd.Timestamp(endDate).strftime("%Y.%m.%d")}
            table(sort(distinct(monthEnd(startDate..endDate))) as `totalDay)
            """)["totalDay"].tolist()
        elif freq == "Q":
            return self.session.run(f"""
            startDate = {pd.Timestamp(startDate).strftime("%Y.%m.%d")}
            endDate = {pd.Timestamp(endDate).strftime("%Y.%m.%d")}
            table(sort(distinct(quarterEnd(startDate..endDate))) as `totalDay)
            """)["totalDay"].tolist()

    def get_tradeDate(self, startDate, endDate: pd.Timestamp) -> List[pd.Timestamp]:
        return self.session.run(f"""
        startDate = {pd.Timestamp(startDate).strftime("%Y.%m.%d")}
        endDate = {pd.Timestamp(endDate).strftime("%Y.%m.%d")}
        table(getMarketCalendar("CFFEX",startDate,endDate) as `tradeDay)
        """)["tradeDay"].tolist()

if __name__ == "__main__":
    T = Time()
    session = ddb.session("localhost",8848,"admin","123456")
    print(T.currentDate)