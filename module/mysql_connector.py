import mysql.connector

class mysql_connector:
    def __init__(self,host=None,user=None,password=None,database=None):
        self.mysql_connector = mysql.connector
        self.DB_CONFIG = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.conn = self.connect()

    def connect(self):
        try:
            self.conn = self.mysql_connector.connect(**self.DB_CONFIG)
            print(f"✓ 已連接到 MySQL 資料庫: {self.DB_CONFIG['database']}")
            return self.conn
        except self.mysql_connector.Error as err:
            print(f"✗ 資料庫連接錯誤: {err}")
            return None

    def disconnect(self):
        if self.conn.is_connected():
            self.conn.close()
            print("\n資料庫連接已關閉")

    def execute_query(self, query, params = [], dictionary=False):
        try:
            cursor = self.conn.cursor(dictionary=dictionary)
            cursor.execute(query, params)
            result = cursor.fetchall()
            cursor.close()
            print(f"✓ 查詢到 {len(result)} 筆資料")
            return result
        except self.mysql_connector.Error as err:
            print(f"✗ 查詢錯誤: {err}")
            return None