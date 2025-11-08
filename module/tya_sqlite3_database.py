import sqlite3
import time

class TyaSqlite3Database:
    def __init__(self, db_file='earthquake_data.db'):
        self.db_file = db_file
        self.conn = self._init_database()

    def _init_database(self):
        """初始化資料庫（啟用 WAL 與最佳化 PRAGMA）"""
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
        print(f"✓ 數據庫已初始化 (WAL 啟用): {self.db_file}")
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
        """批次寫入資料庫（帶 commit 重試）"""
        if not data_list:
            return 0

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
        """關閉資料庫連接"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            print(f"數據庫連接已關閉: {self.db_file}")
