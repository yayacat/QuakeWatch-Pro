import mysql.connector
import os
import time
import queue
from datetime import timedelta
from dotenv import load_dotenv
from threading import Thread
from .utils import Utils

class MySQLDatabase:
    def __init__(self, data_queue, collecting_active):
        load_dotenv()
        self.utils = Utils()
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_DATABASE'),
            'ssl_disabled': True
        }
        self.conn = self.connect()
        self.data_queue = data_queue
        self.collecting_active = collecting_active

    def connect(self):
        """初始化mysql資料庫連接，並檢查/建立mysql資料庫和資料表"""
        try:
            # 先不指定 database，以便檢查並建立它
            conn_check = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                autocommit=True,
                ssl_disabled=True
            )
            cursor = conn_check.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor.close()
            conn_check.close()
            print(f"✓ mysql資料庫已確認/建立: {self.db_config['database']}")

            # 連接到指定的mysql資料庫
            self.conn = mysql.connector.connect(**self.db_config)
            cursor = self.conn.cursor()
            print(f"✓ 已連接到 MySQL mysql資料庫: {self.db_config['database']}")

            # 三軸加速度原始資料
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    timestamp_ms BIGINT NOT NULL,
                    x FLOAT NOT NULL,
                    y FLOAT NOT NULL,
                    z FLOAT NOT NULL,
                    received_time DOUBLE NOT NULL,
                    INDEX idx_sensor_timestamp (timestamp_ms)
                ) ENGINE=InnoDB;
            """)

            # 震度資料
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS intensity_data (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    timestamp_ms BIGINT NOT NULL,
                    intensity FLOAT NOT NULL,
                    a FLOAT NOT NULL,
                    received_time DOUBLE NOT NULL,
                    INDEX idx_intensity_timestamp (timestamp_ms)
                ) ENGINE=InnoDB;
            """)

            # 三軸加速度濾波資料 (ESP32 JMA 濾波: h1, h2, v)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS filtered_data (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    timestamp_ms BIGINT NOT NULL,
                    h1 FLOAT NOT NULL,
                    h2 FLOAT NOT NULL,
                    v FLOAT NOT NULL,
                    received_time DOUBLE NOT NULL,
                    INDEX idx_filtered_timestamp (timestamp_ms)
                ) ENGINE=InnoDB;
            """)

            self.conn.commit()
            print(f"✓ mysql資料表已確認/建立完成")
            return self.conn
        except mysql.connector.Error as err:
            print(f"✗ mysql資料庫錯誤: {err}")
            if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
                print("   請檢查您的使用者名稱和密碼。")
            elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
                print(f"   mysql資料庫 '{self.db_config['database']}' 不存在或權限不足。")
            else:
                print(f"   錯誤碼: {err.errno}")
            self.conn = None
            return None

    def is_connected(self):
        """檢查mysql資料庫連接是否有效"""
        return self.conn is not None and self.conn.is_connected()

    def reconnect(self):
        """嘗試重新連接mysql資料庫"""
        print("[mysql資料庫執行序] 嘗試重新連接mysql資料庫...")
        self.conn = self.connect()
        if self.conn is not None:
            print("[mysql資料庫執行序] 重新連接成功")
            return True
        else:
            print("[mysql資料庫執行序] 重新連接失敗")
            return False

    def close(self):
        """關閉mysql資料庫連接"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print("✓ MySQL mysql資料庫連接已關閉。")

    def mysql_writer_thread(self):
        """mysql資料庫寫入線程主函式 (高效批次處理)"""
        print("[mysql資料庫執行序] 已啟動")

        # 緩衝區設定
        WRITE_INTERVAL = 0.5  # 每 0.5 秒寫入一次
        MAX_BATCH_SIZE = 10000 # 或緩衝區達到 10000 筆數據時寫入
        last_write_time = time.time()

        # 清理設定
        CLEANUP_INTERVAL = 3600 # 每小時清理一次 (3600 秒)
        last_cleanup_time = 0   # 設置為 0 可確保程式啟動後不久就進行第一次清理

        sensor_buffer = []
        filtered_buffer = []
        intensity_buffer = []

        def flush_buffers():
            """寫入所有緩衝區的數據"""
            if not self.is_connected():
                print("[mysql資料庫執行序] 連接無效，跳過寫入")
                # 將數據保留在緩衝區中，等待下次重連成功後寫入
                return

            try:
                if sensor_buffer:
                    self.write_to_database('sensor', sensor_buffer)
                    sensor_buffer.clear()
                if filtered_buffer:
                    self.write_to_database('filtered', filtered_buffer)
                    filtered_buffer.clear()
                if intensity_buffer:
                    self.write_to_database('intensity', intensity_buffer)
                    intensity_buffer.clear()
            except mysql.connector.Error as db_err:
                print(f"\n[mysql資料庫執行序] 寫入失敗: {db_err}")
                # 數據保留在緩衝區中，等待下次重試
                # 這裡不主動斷開連接，依賴 write_to_database 內的重連邏輯
            except Exception as e:
                print(f"\n[mysql資料庫執行序] 寫入時發生未知錯誤: {e}")

        while self.collecting_active.is_set() or not self.data_queue['mysql'].empty():
            current_time = time.time()

            # --- 定期清理 ---
            if self.is_connected() and (current_time - last_cleanup_time > CLEANUP_INTERVAL):
                self.cleanup_old_data()
                last_cleanup_time = current_time
            # ----------------

            if not self.is_connected():
                print("[mysql資料庫執行序] mysql資料庫未連接，將持續嘗試重新連接...")
                while not self.reconnect():
                    print("[mysql資料庫執行序] 3 秒後重試...")
                    time.sleep(3)
                continue

            items_fetched = 0
            try:
                # 批量從隊列中取出數據，直到達到批次大小或隊列為空
                while items_fetched < MAX_BATCH_SIZE:
                    # 使用 get_nowait() 避免阻塞
                    data_type, data_list = self.data_queue['mysql'].get_nowait()

                    # data_list 預期是一個包含多個數據點的列表
                    if data_type == 'sensor':
                        sensor_buffer.extend(data_list)
                    elif data_type == 'filtered':
                        filtered_buffer.extend(data_list)
                    elif data_type == 'intensity':
                        intensity_buffer.extend(data_list)

                    self.data_queue['mysql'].task_done()
                    items_fetched += len(data_list) # 根據實際取出的項目數增加
            except queue.Empty:
                # 隊列為空，正常現象，繼續執行下面的寫入邏輯
                pass

            # 檢查是否滿足任一寫入條件
            force_write = (current_time - last_write_time >= WRITE_INTERVAL) and \
                          (sensor_buffer or filtered_buffer or intensity_buffer)

            buffer_full = (len(sensor_buffer) >= MAX_BATCH_SIZE or
                           len(filtered_buffer) >= MAX_BATCH_SIZE or
                           len(intensity_buffer) >= MAX_BATCH_SIZE)

            if force_write or buffer_full:
                flush_buffers()
                last_write_time = current_time
            else:
                # 如果隊列為空且不需要寫入，短暫休眠以防止CPU空轉
                time.sleep(0.01)

        # 程式結束前，清空所有剩餘的數據
        print("[mysql資料庫執行序] 正在寫入剩餘數據...")
        flush_buffers()
        print("[mysql資料庫執行序] 已停止")

    def start_database_thread(self):
        """啟動mysql資料庫寫入線程"""
        mysql_thread = Thread(target=self.mysql_writer_thread, daemon=True)
        mysql_thread.start()
        return mysql_thread

    def write_to_database(self, data_type, data_list, retries=3, delay=1):
        """批次寫入mysql資料庫，帶有重試機制。如果最終失敗，會拋出 mysql.connector.Error。"""
        if not data_list or self.conn is None:
            return 0

        last_exception = None
        for attempt in range(retries):
            try:
                # 每次重試都應該獲取新的 cursor，以防 connection 處於不良狀態
                self.conn.ping(reconnect=True)
                cursor = self.conn.cursor()
                received_time = time.time()

                if data_type == 'sensor':
                    sql = 'INSERT INTO sensor_data (timestamp_ms, x, y, z, received_time) VALUES (%s, %s, %s, %s, %s)'
                    values = [(d[1], d[2], d[3], d[4], received_time) for d in data_list]
                elif data_type == 'filtered':
                    sql = 'INSERT INTO filtered_data (timestamp_ms, h1, h2, v, received_time) VALUES (%s, %s, %s, %s, %s)'
                    values = [(d[1], d[2], d[3], d[4], received_time) for d in data_list]
                elif data_type == 'intensity':
                    sql = 'INSERT INTO intensity_data (timestamp_ms, intensity, a, received_time) VALUES (%s, %s, %s, %s)'
                    values = [(d[1], d[2], d[3], received_time) for d in data_list]
                else:
                    return 0  # 不支持的 data_type

                if values:  # 確保有值可寫
                    cursor.executemany(sql, values)

                self.conn.commit()
                cursor.close()
                return len(data_list)  # 寫入成功，返回
            except mysql.connector.Error as err:
                last_exception = err
                print(f"✗ mysql資料庫寫入錯誤 (嘗試 {attempt + 1}/{retries}): {err}")
                try:
                    self.conn.rollback()  # 嘗試回滾
                except mysql.connector.Error as rb_err:
                    print(f"  - 回滾失敗: {rb_err}")

                # 擴大偵測連接錯誤的範圍
                is_connection_error = err.errno in (
                    mysql.connector.errorcode.CR_SERVER_GONE_ERROR,
                    mysql.connector.errorcode.CR_SERVER_LOST,
                    mysql.connector.errorcode.ER_CON_COUNT_ERROR
                ) or "Lost connection" in str(err) or "MySQL Connection not available" in str(err)

                if is_connection_error:
                    print("  - 偵測到連接錯誤，嘗試重新連接...")
                    try:
                        self.conn.reconnect(attempts=3, delay=2)
                        print("  - ✓ 重新連接成功")
                        continue # 重試當前操作
                    except mysql.connector.Error as reconn_err:
                        print(f"  - ✗ 重新連接失敗: {reconn_err}")
                        # 如果重連失敗，則不再繼續重試，直接拋出異常
                        raise last_exception from reconn_err

                time.sleep(delay)

        # 如果所有重試都失敗了，拋出最後一個異常
        if last_exception is not None:
            raise last_exception
        return 0

    def cleanup_old_data(self, retries=3, delay=1):
        """清理超過一天的舊資料，帶有重試和批次刪除機制"""
        if not self.is_connected():
            print("[mysql清理] mysql資料庫未連接，跳過清理")
            return

        last_exception = None
        for attempt in range(retries):
            try:
                self.conn.ping(reconnect=True)
                cursor = self.conn.cursor()
                cleanup_datetime = self.utils.get_now_utc8() - timedelta(days=1)
                cleanup_timestamp_ms = int(cleanup_datetime.timestamp() * 1000)
                cleanup_time_str = cleanup_datetime.strftime('%Y-%m-%d %H:%M:%S')

                tables_to_clean = ['sensor_data', 'intensity_data', 'filtered_data']
                total_deleted_rows = 0
                batch_size = 10000  # 每次刪除的筆數

                if attempt == 0:
                    print(f"\n[mysql清理] 開始清理時間戳早於 {cleanup_time_str} (UTC+8) 的舊資料 (批次大小: {batch_size})...")

                for table in tables_to_clean:
                    table_total_deleted = 0
                    while True:
                        # 執行批次刪除
                        sql = f"DELETE FROM {table} WHERE timestamp_ms < %s LIMIT %s"
                        cursor.execute(sql, (cleanup_timestamp_ms, batch_size))
                        deleted_count = cursor.rowcount
                        self.conn.commit()

                        if deleted_count > 0:
                            table_total_deleted += deleted_count
                            total_deleted_rows += deleted_count
                            # 短暫休眠以降低mysql資料庫負載
                            time.sleep(0.1)

                        # 如果刪除的筆數小於批次大小，表示這個表已經清完了
                        if deleted_count < batch_size:
                            if table_total_deleted > 0:
                                print(f"[mysql清理] ✓ 已從 {table} 刪除 {table_total_deleted} 筆舊資料。")
                            break

                if total_deleted_rows > 0:
                    print(f"[mysql清理] ✓ 清理完畢，總共刪除 {total_deleted_rows} 筆舊資料。")
                else:
                    print("[mysql清理] ✓ 無舊資料需要清理。")

                cursor.close()
                return
            except mysql.connector.Error as err:
                last_exception = err
                print(f"[mysql清理] ✗ 清理時發生錯誤 (嘗試 {attempt + 1}/{retries}): {err}")
                try:
                    self.conn.rollback()
                except mysql.connector.Error as rb_err:
                    print(f"[mysql清理] ✗ 回滾失敗: {rb_err}")

                if err.errno in (mysql.connector.errorcode.CR_SERVER_GONE_ERROR,
                                mysql.connector.errorcode.CR_SERVER_LOST,
                                mysql.connector.errorcode.ER_CON_COUNT_ERROR):
                    print("[mysql清理] ✗ 偵測到連接錯誤，嘗試重新連接...")
                    try:
                        self.conn.reconnect(attempts=3, delay=2)
                        print("[mysql清理] ✓ 重新連接成功")
                        continue
                    except mysql.connector.Error as reconn_err:
                        print(f"[mysql清理] ✗ 重新連接失敗: {reconn_err}")
                        raise last_exception from reconn_err
                time.sleep(delay)

        if last_exception is not None:
            raise last_exception
