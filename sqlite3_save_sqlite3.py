import argparse
import sqlite3
from datetime import datetime, timezone, timedelta

# --- Table Schemas ---
TABLES = {
    'sensor_data': {
        'cols': ['timestamp_ms', 'x', 'y', 'z', 'received_time'],
        'schema': '''
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
        'cols': ['timestamp_ms', 'h1', 'h2', 'v', 'received_time'],
        'schema': '''
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
        'cols': ['timestamp_ms', 'intensity', 'a', 'received_time'],
        'schema': '''
            CREATE TABLE IF NOT EXISTS intensity_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL UNIQUE,
                intensity REAL NOT NULL,
                a REAL NOT NULL,
                received_time REAL NOT NULL
            )'''
    }
}

def get_sqlite_connection(db_file, create_tables=False):
    """Establishes and returns a SQLite3 database connection."""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        print(f"✓ Connected to SQLite3 database: {db_file}")

        # Enable WAL mode for better concurrency
        cursor.execute('PRAGMA journal_mode=WAL;')

        if create_tables:
            # Create tables if they don't exist in the destination
            for table_name, schema_info in TABLES.items():
                cursor.execute(schema_info['schema'])
                # Add index
                cursor.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp_ms)')
            conn.commit()
            print("✓ Destination SQLite3 tables verified/created.")

        return conn
    except sqlite3.Error as err:
        print(f"✗ SQLite3 connection error: {err}")
        return None

def transfer_data(source_conn, dest_conn, start_time_ms, end_time_ms):
    """Transfers data from a source SQLite database to a destination SQLite database for a given time range."""
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    total_transferred = 0

    for table_name, config in TABLES.items():
        cols_str = ", ".join(config['cols'])

        # 1. Fetch data from Source SQLite
        query = f"""
            SELECT {cols_str}
            FROM {table_name}
            WHERE timestamp_ms >= ? AND timestamp_ms <= ?
            ORDER BY timestamp_ms ASC;
        """
        try:
            source_cursor.execute(query, (start_time_ms, end_time_ms))
            results = source_cursor.fetchall()
            print(f"✓ Fetched {len(results)} rows from source table '{table_name}'.")
        except sqlite3.Error as err:
            print(f"✗ Error fetching from source table '{table_name}': {err}")
            continue

        if not results:
            continue

        # 2. Insert or Replace data into Destination SQLite
        placeholders = ", ".join(["?"] * len(config['cols']))
        insert_query = f"INSERT OR REPLACE INTO {table_name} ({cols_str}) VALUES ({placeholders})"

        try:
            dest_cursor.executemany(insert_query, results)
            dest_conn.commit()
            print(f"✓ Transferred {dest_cursor.rowcount} rows to destination table '{table_name}'.")
            total_transferred += dest_cursor.rowcount
        except sqlite3.Error as err:
            print(f"✗ Error inserting into destination table '{table_name}': {err}")
            dest_conn.rollback()

    source_cursor.close()
    dest_cursor.close()
    return total_transferred

def main():
    parser = argparse.ArgumentParser(description='將一段時間區間的地震資料從一個 SQLite 資料庫傳輸到另一個 SQLite 資料庫。')
    parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-t', '--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')
    parser.add_argument('-o', '--output', default=None, help='輸出 SQLite 資料庫檔案名稱。預設會自動生成。')

    args = parser.parse_args()

    tz_utc_8 = timezone(timedelta(hours=8))

    if args.o_time:
        try:
            o_time_dt_naive = datetime.strptime(args.o_time, '%Y-%m-%dT%H:%M:%S')
            o_time_dt_aware = o_time_dt_naive.replace(tzinfo=tz_utc_8)
            start_dt_aware = o_time_dt_aware - timedelta(minutes=args.time)
            end_dt_aware = o_time_dt_aware + timedelta(minutes=args.time)
            print(f"已提供 o_time，自動設定時間範圍為: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
        except ValueError:
            parser.error("錯誤的 o_time 格式。請使用 YYYY-MM-DDTHH:MM:SS。")
    elif args.start_time and args.end_time:
        try:
            start_dt_naive = datetime.strptime(args.start_time, '%Y-%m-%dT%H:%M:%S')
            end_dt_naive = datetime.strptime(args.end_time, '%Y-%m-%dT%H:%M:%S')
            start_dt_aware = start_dt_naive.replace(tzinfo=tz_utc_8)
            end_dt_aware = end_dt_naive.replace(tzinfo=tz_utc_8)
        except ValueError:
            parser.error("錯誤的時間格式。請使用 YYYY-MM-DDTHH:MM:SS。")
    else:
        end_dt_aware = datetime.now(tz_utc_8)
        start_dt_aware = end_dt_aware - timedelta(minutes=args.time)
        print(f"未提供時間範圍，自動使用最近 {args.time} 分鐘: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")


    source_conn = None
    dest_conn = None

    try:
        # Establish connections
        source_conn = get_sqlite_connection('earthquake_data.db')
        if not source_conn:
            return

        output_db_name = args.output if args.output else start_dt_aware.strftime('earthquake_data_%Y%m%dT%H%M%S.db')

        dest_conn = get_sqlite_connection(output_db_name, create_tables=True)
        if not dest_conn:
            return

        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

        # Perform the data transfer
        total_rows = transfer_data(source_conn, dest_conn, start_time_ms, end_time_ms)

        print(f"\n✓ Data transfer complete. Total rows transferred: {total_rows}")
        print(f"✓ Data saved to: {output_db_name}")

    except Exception as e:
        print(f"✗ An unexpected error occurred: {e}")
    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            print("✓ Source SQLite connection closed.")
        if dest_conn:
            dest_conn.close()
            print("✓ Destination SQLite connection closed.")

if __name__ == '__main__':
    main()