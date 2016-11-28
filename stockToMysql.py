from mysqlHelper import  MysqlHelper
import getStockData as stockData
import pandas as pd



def init_database():
    init_stockInf()

def init_stockInf():
    create_table_Stock_info = "DROP TABLE IF EXISTS `stock`.`stock_inf`;CREATE TABLE `stock`.`stock_inf` ( `code` VARCHAR(6) NOT NULL COMMENT '代码'," \
                              "`name` NVARCHAR(50) NULL COMMENT '名称'," \
                              "`trade` NVARCHAR(50) NULL COMMENT '行业分类'," \
                              "`industry` NVARCHAR(50) NULL COMMENT '所在行业'," \
                              "`area` VARCHAR(45) NULL COMMENT '所在区域'," \
                              "`PE` DOUBLE(8,2) NULL COMMENT '市盈率pe'," \
                              "`outstanding` DOUBLE(16,2) NULL COMMENT '流通股本', " \
                              "`totals` DOUBLE(16,2) NULL COMMENT '总股本'," \
                              "`totalAssets` DOUBLE(16,2) NULL COMMENT '总资产'," \
                              "`liquidAssets` DOUBLE(16,2) NULL COMMENT '流动资产'," \
                              "`fixedAssets` DOUBLE(16,2) NULL COMMENT '固定资产'," \
                              "`reserved` DOUBLE(16,2) NULL COMMENT '公积金'," \
                              "`reservedPerShare` DOUBLE(16,4) NULL COMMENT '每股公积金'," \
                              "`eps` DOUBLE(16,4) NULL COMMENT '每股收益'," \
                              "`bvps` DOUBLE(16,4) NULL COMMENT '每股净资产'," \
                              "`pb` DOUBLE(16,4) NULL COMMENT '市净率'," \
                              "`timeToMarket` DATETIME NULL COMMENT '上市日期'," \
                              "`price` DOUBLE(8,4) NULL COMMENT '',PRIMARY KEY (`code`)  COMMENT '') ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8;"
    #mysql = MysqlHelper('localhost', 3306, 'root', '1', 'stock')
    mysql = MysqlHelper()
    mysql.connect()
    mysql.exec_sql(create_table_Stock_info)
    if (mysql.connected):
        print("stockToMysql: ","stock_inf 建立成功")
    df = (pd.DataFrame)(stockData.getStockData())

    print("stockToMysql: ","存入stock_inf...")

    for iter in df.index:
        code = df.loc[iter,"code"]
        name = df.loc[iter, "股票名称"]
        trade = df.loc[iter, "行业"]
        industry = df.loc[iter, "板块"]
        area = df.loc[iter, "地区"]

        PE = df.loc[iter, "市盈率pe"]
        outstanding = df.loc[iter, "流通股本"]
        totals = df.loc[iter, "总股本"]
        totalAssets = df.loc[iter, "总资产"]
        liquidAssets = df.loc[iter, "流动资产"]
        fixedAssets = df.loc[iter, "固定资产"]
        reserved = df.loc[iter, "公积金"]
        reservedPerShare = df.loc[iter, "每股公积金"]
        eps = df.loc[iter, "每股收益"]
        bvps = df.loc[iter, "每股净资产"]

        pb = df.loc[iter, "市净率pb"]
        timeToMarket = df.loc[iter, "上市日期"]

        stock_info_insert = "INSERT INTO `stock`.`stock_inf`(`code`,`name`,`trade`,`industry`,`area`,`PE`," \
                            "`outstanding`,`totals`,`totalAssets`,`" \
                            "liquidAssets`,`fixedAssets`,`reserved`,`reservedPerShare`," \
                            "`eps`,`bvps`,`pb`,`timeToMarket`) "
        stock_info_insert = stock_info_insert+"VALUES ('%s','%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,'%s');" % (
        code, name, trade, industry, area, float(PE), float(outstanding), float(totals), float(totalAssets),
        float(liquidAssets), float(fixedAssets), float(reserved), float(reservedPerShare), float(eps), float(bvps),
        float(pb), timeToMarket)

        if len('%d'%timeToMarket) < 8:
            stock_info_insert = "INSERT INTO `stock`.`stock_inf`(`code`,`name`,`trade`,`industry`,`area`,`PE`," \
                                "`outstanding`,`totals`,`totalAssets`,`" \
                                "liquidAssets`,`fixedAssets`,`reserved`,`reservedPerShare`," \
                                "`eps`,`bvps`,`pb`) "
            stock_info_insert = stock_info_insert + "VALUES ('%s','%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d);" % (
                code, name, trade, industry, area, float(PE), float(outstanding), float(totals), float(totalAssets),
                float(liquidAssets), float(fixedAssets), float(reserved), float(reservedPerShare), float(eps),
                float(bvps),
                float(pb))

        mysql.exec_sql(stock_info_insert)
        # print(df.loc[iter,:])
        # print(stock_info_insert)
    print("stockToMysql: ", "存入stock_inf完毕")
    return

def init_currentdayprice():
    create_price_current="DROP TABLE IF EXISTS `stock`.`price_currentday`; " \
                             "CREATE TABLE `stock`.`price_currentday` (`code` NVARCHAR(6) NOT NULL COMMENT ''," \
                             "`name` NVARCHAR(50) NULL COMMENT '',`changepercent` DOUBLE(5,2) NULL COMMENT '涨跌幅'," \
                             "`trade_price` DOUBLE(7,2) NULL COMMENT '现价',`open_price` DOUBLE(7,2) NULL COMMENT '开盘价'," \
                             "`high_price` DOUBLE(7,2) NULL COMMENT '最高价',`low_price` DOUBLE(7,2) NULL COMMENT '最低价'," \
                             "`settlement_price` DOUBLE(7,2) NULL COMMENT '昨日收盘价',`volume` DOUBLE(11,0) NULL COMMENT '成交量'," \
                             "`turnover_ratio` DOUBLE(5,2) NULL COMMENT '换手率',`amount` DOUBLE(11,0) NULL COMMENT '成交量'," \
                             "`per` DOUBLE(5,0) NULL COMMENT '市盈率',`pb` DOUBLE(5,0) NULL COMMENT ''," \
                             "`mktcap` DOUBLE(16,0) NULL COMMENT '总市值',`nmc` DOUBLE(16,0) NULL COMMENT '流通市值'," \
                             "`date` datetime DEFAULT CURRENT_TIMESTAMP," \
                             "PRIMARY KEY (`code`)  COMMENT '') ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"
    #mysql = MysqlHelper('localhost', 3306, 'root', '1', 'stock')
    mysql = MysqlHelper()
    mysql.connect()
    mysql.exec_sql(create_price_current)
    if (mysql.connected):
        print("stockToMysql: ","price_currentday 建立成功")
    return

def get_currentdayprice():
    print("stockToMysql: ", "获取当前股价")
    df = (pd.DataFrame)(stockData.getStockTodayPrice(''))
    mysql = MysqlHelper()
    mysql.connect()
    print("stockToMysql: ", "存入save_current_price中...")
    #i = 0
    print("stockToMysql: ","开始保存 ",)
    for iter in df.index:
        code = df.loc[iter, "code"]
        name = df.loc[iter, "name"]
        changepercent = df.loc[iter, "changepercent"]
        trade = df.loc[iter, "trade"]
        open = df.loc[iter, "open"]
        high = df.loc[iter, "high"]
        low = df.loc[iter, "low"]
        settlement = df.loc[iter, "settlement"]
        volume = df.loc[iter, "volume"]
        turnoverratio = df.loc[iter, "turnoverratio"]
        amount = df.loc[iter, "amount"]
        per = df.loc[iter, "per"]
        pb = df.loc[iter, "pb"]
        mktcap = df.loc[iter, "mktcap"]
        nmc = df.loc[iter, "nmc"]

        sql_str="CALL `stock`.`save_current_price`('%s','%s',%d, %d, %d, %d, %d,%d, %d, %d,%d,%d, %d, %d,%d);"% \
                (code, name, float(changepercent), float(trade), float(open), float(high),
                float(low), float(settlement), float(volume), float(turnoverratio), float(amount),
                float(per), float(pb), float(mktcap), float(nmc))

        mysql.exec_sql(sql_str)
        #i = i + 1
        # if (i % 295 == 0):
        #     print("#")
    print()
    print("stockToMysql: ", "存入save_current_price完毕")



    # code：代码     name:名称     changepercent:涨跌幅       trade:现价    open:开盘价    high:最高价
    # low:最低价     settlement:昨日收盘价    volume:成交量      turnoverratio:换手率       amount:成交量
    # per:市盈率     pb:市净率      mktcap:总市值      nmc:流通市值
    # if (len(str)>0):
    #   print(str)
