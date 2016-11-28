import pymysql as pymysql


class MysqlHelper:
    host = 'localhost'
    port = 3306
    user = 'root'
    password = '1'
    db = 'stock'
    connected = False
    myConnect = ''

    def __init__(self, _host_name, _port, _user, _password, _db):
        self.set_connect_param(_host_name,_port,_user,_password,_db)

    def __init__(self):
        return

    def set_connect_param(self, _host_name, _port, _user, _password, _db):
        self.host = _host_name
        self.port = _port
        self.user = _user
        self.password = _password
        self.db = _db

    def connect(self):

        try:
            self.myConnect = pymysql.Connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.password,
                db=self.db,
                charset='utf8'
            )


            error_text = "成功"
            self.connected = True
        except Exception as e:
            error_text = "数据库链接失败," + e
            self.connected = False
        return error_text

    def __del__(self):
        self.connected=False
        # self.cursor.close()
        # self.myConnect.close()

    def exec_sql(self,_str_sql):
        message = '未链接'
        if (self.connected == False):
            return message
        mycursor = self.myConnect.cursor()
        try:
            mycursor.execute(_str_sql)
            self.myConnect.commit()
            message = '成功'
        except Exception as e:
            self.myConnect.rollback()
            print("error,",e,"   ",_str_sql)
            #print(e)
            message = e
        finally:
            mycursor.close()
        return message

    def select_sql(self,_str_sql,params):
        mycursor = self.myConnect.cursor()
        if (params!=None):
            row_count = mycursor.execute(_str_sql, (params))
        else:
            row_count = mycursor.execute(_str_sql)
        rows=mycursor.fetchall()
        self.myConnect.commit()
        mycursor.close()

        return row_count,rows