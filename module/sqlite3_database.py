import sqlite3
import time
import queue
from threading import Thread

class Sqlite3Database:
    def __init__(self, data_queue, collecting_active, db_file='earthquake_data.db'):
        self.db_file = db_file
        self.conn = self._init_database()
        self.conn_client = self._init_database_client()
        self.data_queue = data_queue
        self.collecting_active = collecting_active

    def _init_database(self):
        """初始化sqlite3資料庫（啟用 WAL 與最佳化 PRAGMA）"""
        conn = sqlite3.connect(self.db_file, timeout=10.0, check_same_thread=False)
        cursor = conn.cursor()

        # --- 關鍵：啟用 WAL 與降低同步成本，並設置鎖等待 ---
        cursor.execute('PRAGMA journal_mode=WAL;')
        cursor.execute('PRAGMA synchronous=NORMAL;')
        cursor.execute('PRAGMA busy_timeout=5000;')
        cursor.execute('PRAGMA temp_store=MEMORY;')
        cursor.execute('PRAGMA mmap_size=300000000;')
        cursor.execute('PRAGMA wal_autocheckpoint=1000;')  # 避免太頻繁 checkpoint
        cursor.execute('PRAGMA cache_size=-10000;')        # ~10MB 快取

        # 三軸加速度原始資料
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                x REAL NOT NULL,
                y REAL NOT NULL,
                z REAL NOT NULL,
                received_time REAL NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_data(timestamp_ms)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_received ON sensor_data(received_time)')

        # 震度資料
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS intensity_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                intensity REAL NOT NULL,
                a REAL NOT NULL,
                received_time REAL NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intensity_timestamp ON intensity_data(timestamp_ms)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_intensity_received ON intensity_data(received_time)')

        # 三軸加速度濾波資料 (ESP32 JMA 濾波: h1, h2, v)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS filtered_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                h1 REAL NOT NULL,
                h2 REAL NOT NULL,
                v REAL NOT NULL,
                received_time REAL NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filtered_timestamp ON filtered_data(timestamp_ms)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filtered_received ON filtered_data(received_time)')

        conn.commit()
        print(f"✓ sqlite3資料庫已初始化 (WAL 啟用): {self.db_file}")
        return conn

    def _init_database_client(self):
        """初始化 sqlite3 資料庫用於客戶端查詢的連接"""
        # 長連線唯讀（避免反覆開關）
        conn = sqlite3.connect(f'file:{self.db_file}?mode=ro', uri=True, timeout=10.0, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('PRAGMA busy_timeout=5000;')  # 被鎖時等待
        cursor.execute('PRAGMA query_only=ON;')      # 嚴格唯讀
        cursor.execute('PRAGMA read_uncommitted=1;') # 放寬一致性，減少阻塞

        return conn

    def _commit_with_retry(self, retries=5, backoff=0.05):
        """對付 'database is locked' 的指數退避 commit"""
        for i in range(retries):
            try:
                self.conn.commit()
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower():
                    raise
                time.sleep(backoff * (2 ** i))
        # 最後一次嘗試，仍失敗就拋出讓外面看到
        self.conn.commit()

    def write_to_database(self, data_type, data_list):
        """批次寫入sqlite3資料庫（帶 commit 重試）"""
        if not data_list:
            return 0

        if self.conn:
            cursor = self.conn.cursor()
            received_time = time.time()

            if data_type == 'sensor':
                cursor.executemany(
                    'INSERT INTO sensor_data (timestamp_ms, x, y, z, received_time) VALUES (?, ?, ?, ?, ?)',
                    [(d[1], d[2], d[3], d[4], received_time) for d in data_list]
                )
            elif data_type == 'filtered':
                cursor.executemany(
                    'INSERT INTO filtered_data (timestamp_ms, h1, h2, v, received_time) VALUES (?, ?, ?, ?, ?)',
                    [(d[1], d[2], d[3], d[4], received_time) for d in data_list]
                )
            elif data_type == 'intensity':
                cursor.executemany(
                    'INSERT INTO intensity_data (timestamp_ms, intensity, a, received_time) VALUES (?, ?, ?, ?)',
                    [(d[1], d[2], d[3], received_time) for d in data_list]
                )

            self._commit_with_retry()
        return len(data_list)

    def close(self):
        """關閉sqlite3資料庫連接"""
        if self.conn:
            self.conn.close()
            print(f"sqlite3資料庫連接已關閉: {self.db_file}")

    def is_connected(self):
        """檢查sqlite3資料庫連接是否有效"""
        try:
            # 嘗試執行一個簡單的查詢來驗證連接
            self.conn.execute('SELECT 1')
            return True
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            # ProgrammingError: a closed database
            # OperationalError: database is locked
            return False

    def reconnect(self):
        """重新連接到sqlite3資料庫"""
        self.close()
        try:
            self.conn = self._init_database()
            return self.is_connected()
        except Exception as e:
            print(f"sqlite3資料庫重新連接失敗: {e}")
            self.conn = None
            return False

    def cleanup_old_data(self, days_to_keep=1):
        """清理超過指定天數的舊數據"""
        if not self.is_connected():
            print("[sqlite3資料庫清理] sqlite3資料庫未連接，跳過清理。")
            return

        cutoff_time = time.time() - (days_to_keep * 24 * 3600)

        tables = ['sensor_data', 'filtered_data', 'intensity_data']

        print(f"[sqlite3資料庫清理] 正在清理 {days_to_keep} 天前的舊數據...")

        try:
            cursor = self.conn.cursor()
            for table in tables:
                # 使用 received_time 進行清理
                cursor.execute(f"DELETE FROM {table} WHERE received_time < ?", (cutoff_time,))

            self.conn.commit()
            print("[sqlite3資料庫清理] 舊數據清理完成。")

            # 執行 VACUUM 來釋放空間
            print("[sqlite3資料庫清理] 正在執行 VACUUM...")
            self.conn.execute("VACUUM")
            print("[sqlite3資料庫清理] VACUUM 完成。")

        except sqlite3.Error as e:
            print(f"[sqlite3資料庫清理] 清理過程中發生錯誤: {e}")
            try:
                self.conn.rollback()
            except sqlite3.Error as rb_e:
                print(f"[sqlite3資料庫清理] Rollback 失敗: {rb_e}")

    def sqlite3_writer_thread(self):
        """sqlite3資料庫寫入線程主函式 (高效批次處理)"""
        print("[sqlite3資料庫執行序] 已啟動")

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
                print("[sqlite3資料庫執行序] 連接無效，跳過寫入")
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
            except sqlite3.Error as db_err:
                print(f"\n[sqlite3資料庫執行序] 寫入失敗: {db_err}")
                # 數據保留在緩衝區中，等待下次重試
            except Exception as e:
                print(f"\n[sqlite3資料庫執行序] 寫入時發生未知錯誤: {e}")

        while self.collecting_active.is_set() or not self.data_queue['sqlite3'].empty():
            current_time = time.time()

            # --- 定期清理 ---
            if self.is_connected() and (current_time - last_cleanup_time > CLEANUP_INTERVAL):
                self.cleanup_old_data()
                last_cleanup_time = current_time
            # ----------------

            if not self.is_connected():
                print("[sqlite3資料庫執行序] sqlite3資料庫未連接，等待 3 秒...")
                time.sleep(3)
                reconnected = self.reconnect()
                if not reconnected:
                    print("[sqlite3資料庫執行序] 重新連接失敗，將繼續等待...")
                continue

            items_fetched = 0
            try:
                # 批量從隊列中取出數據，直到達到批次大小或隊列為空
                while items_fetched < MAX_BATCH_SIZE:
                    # 使用 get_nowait() 避免阻塞
                    data_type, data_list = self.data_queue['sqlite3'].get_nowait()

                    # data_list 預期是一個包含多個數據點的列表
                    if data_type == 'sensor':
                        sensor_buffer.extend(data_list)
                    elif data_type == 'filtered':
                        filtered_buffer.extend(data_list)
                    elif data_type == 'intensity':
                        intensity_buffer.extend(data_list)

                    self.data_queue['sqlite3'].task_done()
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
        print("[sqlite3資料庫執行序] 正在寫入剩餘數據...")
        flush_buffers()
        print("[sqlite3資料庫執行序] 已停止")

    def start_database_thread(self):
        """啟動sqlite3資料庫寫入線程"""
        database_thread = Thread(target=self.sqlite3_writer_thread, daemon=True)
        database_thread.start()
        return database_thread