import tushare as ts

if __name__ == "__main__":
    token = "2598d86cf61700793b10bf2d1d36d59bccc00379451e506e60b5cf15"
    pro = ts.pro_api(token=token,timeout=30)
    # 查询当前所有正常上市交易的股票列表
    data = pro.stock_basic(exchange='', fields='ts_code,symbol,name,area,industry,list_date')
    print(data)