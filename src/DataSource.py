import pandas as pd
import dolphindb as ddb
import tushare as ts
from tushare.pro.client import DataApi
from dagster import asset, Definitions, ScheduleDefinition, define_asset_job

@asset(description="同步写入DolphinDB",
       group_name="insert")
def insertToDDB(session: ddb.session, dbName: str, tbName: str, data: pd.DataFrame):
    """
    DDB tableInsert 同步写入
    """
    session.run(f"""
    pt = loadTable("{dbName}","{tbName}");
    tableInsert{{pt}}""", data)

@asset(description="异步写入DolphinDB",
       group_name="insert")
def insertToDBBMtw(writer: ddb.MultithreadedTableWriter, dbName: str, tbName: str, data: pd.DataFrame,
                   strCols: List[str] = None, dateCols: List[str] = None,
                   floatCols: List[str] = None, intCols: List[str] = None,
                   timestampCols: List[str] = None) -> ddb.MultithreadedTableWriterStatus:
    """
    DDB MultiTableWriter 异步写入
    若需要内部批量处理可以输入对应的列名, 也可以全为None处理完了再丢进来
    """
    # 预先批量转换数据
    if strCols:
        for col in strCols:
            data[col] = data[col].apply(lambda x: str(x))
    if dateCols:
        for col in dateCols:
            data[col] = data[col].apply(pd.Timestamp)
    if floatCols:
        for col in floatCols:
            data[col] = pd.to_numeric(data[col], errors="coerce")
    if intCols:
        for col in intCols:
            data[col] = data[col].apply(int)
    if timestampCols:
        for col in timestampCols:
            data[col] = data[col].apply(pd.Timestamp)

    # 循环插入数据
    for row in df_converted.itertuples():
        values = list(row[1:])  # 跳过索引
        writer.insert(*values)

    writer.waitForThreadCompletion()
    return writer.getStatus()

@asset(description="获取A股基本标的信息",
       group_name="stock")
def get_stock_basic(pro: DataApi) -> pd.DataFrame:
    # 查询当前所有正常上市交易的股票列表
    data = pro.stock_basic(exchange='')
    data["list_date"] = data["list_date"].apply(pd.Timestamp)
    data["delist_date"] = data["delist_date"].apply(pd.Timestamp)
    return data
