#环境  pandas、tushare、pymysql3
# http://tushare.org/trading.html#id2
# pip3.5 install pymysql3


import stockToMysql
import threading

def init_database():
    stockToMysql.init_database()
    stockToMysql.init_currentdayprice()


# init_database()

stockToMysql.init_currentdayprice()
stockToMysql.get_currentdayprice()
