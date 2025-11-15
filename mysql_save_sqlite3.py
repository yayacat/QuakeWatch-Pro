import os
import argparse
import mysql.connector
import sqlite3
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- MySQL Database Configuration ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}

# --- Table Schemas ---
TABLES = {
    'sensor_data': {
        'mysql_cols': ['timestamp_ms', 'x', 'y', 'z', 'received_time'],
        'sqlite_cols': ['timestamp_ms', 'x', 'y', 'z', 'received_time'],
        'sqlite_schema': '''
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL UNIQUE,
                x REAL NOT NULL,
                y REAL NOT NULL,
                z REAL NOT NULL,
                received_time REAL NOT NULL
            )'''
    },
    'filtered_data': {
        'mysql_cols': ['timestamp_ms', 'h1', 'h2', 'v', 'received_time'],
        'sqlite_cols': ['timestamp_ms', 'h1', 'h2', 'v', 'received_time'],
        'sqlite_schema': '''
            CREATE TABLE IF NOT EXISTS filtered_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL UNIQUE,
                h1 REAL NOT NULL,
                h2 REAL NOT NULL,
                v REAL NOT NULL,
                received_time REAL NOT NULL
            )'''
    },
    'intensity_data': {
        'mysql_cols': ['timestamp_ms', 'intensity', 'a', 'received_time'],
        'sqlite_cols': ['timestamp_ms', 'intensity', 'a', 'received_time'],
        'sqlite_schema': '''
            CREATE TABLE IF NOT EXISTS intensity_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL UNIQUE,
                intensity REAL NOT NULL,
                a REAL NOT NULL,
                received_time REAL NOT NULL
            )'''
    }
}

def get_mysql_connection():
    """Establishes and returns a MySQL database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print(f"✓ Connected to MySQL database: {DB_CONFIG['database']}")
        return conn
    except mysql.connector.Error as err:
        print(f"✗ MySQL connection error: {err}")
        return None

def get_sqlite_connection(db_file):
    """Establishes and returns a SQLite3 database connection and creates tables."""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print(f"✓ Connected to SQLite3 database: {db_file}")

        # Enable WAL mode for better concurrency
        cursor.execute('PRAGMA journal_mode=WAL;')

        # Create tables if they don't exist
        for table_name, schema_info in TABLES.items():
            cursor.execute(schema_info['sqlite_schema'])
            # Add index
            cursor.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp_ms)')

        conn.commit()
        print("✓ SQLite3 tables verified/created.")
        return conn
    except sqlite3.Error as err:
        print(f"✗ SQLite3 connection error: {err}")
        return None

def transfer_data(mysql_conn, sqlite_conn, start_time_ms, end_time_ms):
    """Transfers data from MySQL to SQLite for a given time range."""
    mysql_cursor = mysql_conn.cursor()
    sqlite_cursor = sqlite_conn.cursor()

    total_transferred = 0

    for table_name, config in TABLES.items():
        mysql_cols_str = ", ".join(config['mysql_cols'])

        # 1. Fetch data from MySQL
        query = f"""
            SELECT {mysql_cols_str}
            FROM {table_name}
            WHERE timestamp_ms >= %s AND timestamp_ms <= %s
            ORDER BY timestamp_ms ASC;
        """
        try:
            mysql_cursor.execute(query, (start_time_ms, end_time_ms))
            results = mysql_cursor.fetchall()
            print(f"✓ Fetched {len(results)} rows from MySQL table '{table_name}'.")
        except mysql.connector.Error as err:
            print(f"✗ Error fetching from MySQL table '{table_name}': {err}")
            continue

        if not results:
            continue

        # 2. Insert or Replace data into SQLite
        sqlite_cols_str = ", ".join(config['sqlite_cols'])
        placeholders = ", ".join(["?"] * len(config['sqlite_cols']))

        insert_query = f"INSERT OR REPLACE INTO {table_name} ({sqlite_cols_str}) VALUES ({placeholders})"

        try:
            sqlite_cursor.executemany(insert_query, results)
            sqlite_conn.commit()
            print(f"✓ Transferred {sqlite_cursor.rowcount} rows to SQLite table '{table_name}'.")
            total_transferred += sqlite_cursor.rowcount
        except sqlite3.Error as err:
            print(f"✗ Error inserting into SQLite table '{table_name}': {err}")
            sqlite_conn.rollback()

    mysql_cursor.close()
    sqlite_cursor.close()
    return total_transferred

def main():
    parser = argparse.ArgumentParser(description='將地震資料從 MySQL 資料庫傳輸到 SQLite 資料庫。')
    parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-t', '--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')
    parser.add_argument('-o', '--output', default='earthquake_data_archive.db', help='Output SQLite database file name. Default is earthquake_data_archive.db.')

    args = parser.parse_args()

    tz_utc_8 = timezone(timedelta(hours=8))

    # o_time 自動定出前後5 分鐘為開始與結束時間，優先級比 start_time 和 end_time 高
    if args.o_time:
        try:
            o_time_dt_naive = datetime.strptime(args.o_time, '%Y-%m-%dT%H:%M:%S')
            o_time_dt_aware = o_time_dt_naive.replace(tzinfo=tz_utc_8)
            start_dt_aware = o_time_dt_aware - timedelta(minutes=args.time)
            end_dt_aware = o_time_dt_aware + timedelta(minutes=args.time)
            print(f"已提供 o_time，自動設定時間範圍為: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
        except ValueError:
            parser.error("錯誤的 o_time 格式。請使用 YYYY-MM-DDTHH:MM:SS。")
    elif args.start_time is None or args.end_time is None:
        end_dt_aware = datetime.now(tz_utc_8)
        start_dt_aware = end_dt_aware - timedelta(minutes=args.time)
        print(f"未提供時間範圍，自動使用最近 {args.time} 分鐘: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
    else:
        try:
            start_dt_naive = datetime.strptime(args.start_time, '%Y-%m-%dT%H:%M:%S')
            end_dt_naive = datetime.strptime(args.end_time, '%Y-%m-%dT%H:%M:%S')
            start_dt_aware = start_dt_naive.replace(tzinfo=tz_utc_8)
            end_dt_aware = end_dt_naive.replace(tzinfo=tz_utc_8)
        except ValueError:
            parser.error("錯誤的時間格式。請使用 YYYY-MM-DDTHH:MM:SS。")

    mysql_conn = None
    sqlite_conn = None

    try:
        # Establish connections
        mysql_conn = get_mysql_connection()
        if not mysql_conn:
            return

        args.output = start_dt_aware.strftime('earthquake_data_%Y%m%dT%H%M%S.db')

        sqlite_conn = get_sqlite_connection(args.output)
        if not sqlite_conn:
            return

        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

        # Perform the data transfer
        total_rows = transfer_data(mysql_conn, sqlite_conn, start_time_ms, end_time_ms)

        print(f"\n✓ Data transfer complete. Total rows transferred: {total_rows}")

    except Exception as e:
        print(f"✗ An unexpected error occurred: {e}")
    finally:
        # Close connections
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
            print("✓ MySQL connection closed.")
        if sqlite_conn:
            sqlite_conn.close()
            print("✓ SQLite3 connection closed.")

if __name__ == '__main__':
    main()