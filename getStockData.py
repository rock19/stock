# http://tushare.org/trading.html#id2
# pip3.5 install pymysql3
import tushare as ts
from pandas import Series,DataFrame
import pymysql as pymysql
from mysqlHelper import  MysqlHelper


def getStockData():

    # stockData=pd.DataFrame(columns={'code','股票名称','细分行业','市盈率pe','地区','流通股本','总股本','总资产','流动资产','固定资产',
    #                                 '公积金','每股公积金','每股收益','每股净资产','市净率pb','上市日期'})

    data = []
    labelMat=['code','股票名称','板块','行业','地区','市盈率pe','流通股本','总股本','总资产','流动资产','固定资产',
                                    '公积金','每股公积金','每股收益','每股净资产','市净率pb','上市日期']
    print("读取stock_inf...")
    stockDataArray =  ts.get_stock_basics() #获取股票基本面
    df_classified = ts.get_industry_classified() #获取分类

    # code：代码     name:名称     changepercent:涨跌幅       trade:现价    open:开盘价    high:最高价
    # low:最低价     settlement:昨日收盘价    volume:成交量      turnoverratio:换手率       amount:成交量
    # per:市盈率     pb:市净率      mktcap:总市值      nmc:流通市值

    print("转换stock_inf...")
    for index in range(len(stockDataArray.index)):

        name=stockDataArray.iloc[index,0]
        code=stockDataArray['name'].index[index]
        cname = getStockCname(df_classified,code)  #获取行业
        # stockTodayPrice=getStockTodayPrice(stockToday,code)  #获取当前价格
        industry=stockDataArray.iloc[index,1]
        area=stockDataArray.iloc[index,2]
        pe=stockDataArray.iloc[index,3]
        outstanding=stockDataArray.iloc[index,4]
        totals=stockDataArray.iloc[index,5]
        totalAssets=stockDataArray.iloc[index,6]
        liquidAssets=stockDataArray.iloc[index,7]
        fixedAssets=stockDataArray.iloc[index,8]
        reserved=stockDataArray.iloc[index,9]
        reservedPerShare=stockDataArray.iloc[index,10]

        eps=stockDataArray.iloc[index,11]
        bvps=stockDataArray.iloc[index,12]
        pb=stockDataArray.iloc[index,13]
        timeTomarket=stockDataArray.iloc[index,14]
        data.append([code,name,cname,industry,area,pe,outstanding,totals,totalAssets,liquidAssets,fixedAssets,reserved,reservedPerShare,
                    eps,bvps,pb,timeTomarket])

        #print(data[index])
    frame2 = DataFrame(data, columns=labelMat)
    print("转换stock_inf成功...")
    return frame2


def getStockCname(df,code): #获取股票所在行业


    str=df[(df['code']==code)]
    if (len(str)>0):
        str=str.iloc[0,2]
    else:
        str=""
        #df[['c_name']][(df['code']==code)]
    #print(str)
    return str

def getStockTodayPrice(code):
    labelTodayPrice = ['code', '股票名称', '涨跌幅', '现价', '开盘价', '最高价', '最低价', '昨日收盘价', '成交量', '换手率', '成交量', '市盈率', '市净率',
                       '总市值', '流通市值']
    stockToday = ts.get_today_all()
    if (len(code)>1):
        stockToday=stockToday[(stockToday['code']==code)]
    # code：代码     name:名称     changepercent:涨跌幅       trade:现价    open:开盘价    high:最高价
    # low:最低价     settlement:昨日收盘价    volume:成交量      turnoverratio:换手率       amount:成交量
    # per:市盈率     pb:市净率      mktcap:总市值      nmc:流通市值
    #if (len(str)>0):
     #   print(str)

    return stockToday


#df=ts.get_hist_data('600848')
#print(df)
# setStockData()
#
#
# mysql=MysqlHelper('localhost',3306,'root','1','stock')
# mysql.connect()
# if(mysql.connected):
#     print('数据库链接成功')
# row_count,rows=mysql.select_sql("select @@version",None)
# #df=ts.get_realtime_quotes(['sh','600848'])
# print(row_count,rows)

#df=ts.get_stock_basics()

#print(df)