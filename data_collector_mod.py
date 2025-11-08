import sys
import time
from threading import Event
from queue import Queue
import argparse
import os
from dotenv import load_dotenv
from module.utils import Utils
from module.serial_data_collector import SerialDataCollector
from module.sqlite3_database import Sqlite3Database
from module.mysql_database import MySQLDatabase
from module.seedlink import DataHttpPoster
from module.websocket import WebSocketServer

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="QuakeWatch - Serial Data Collector Module")
    parser.add_argument('--sqlite3', action='store_true', help='Enable SQLite3 output')
    parser.add_argument('--mysql', action='store_true', help='Enable MySQL output')
    parser.add_argument('--http', action='store_true', help='Enable HTTP output')
    parser.add_argument('--all', action='store_true', help='Enable all outputs')
    parser.add_argument('--auto', action='store_true', help='Automatically select serial port')
    args = parser.parse_args()

    if args.all:
        args.sqlite3 = True
        args.mysql = True
        args.http = True

    if not any([args.sqlite3, args.mysql, args.http]):
        print("請至少選擇一個輸出選項: --sqlite3, --mysql, --http, --all")
        sys.exit(1)

    print("QuakeWatch - Serial Data Collector Module")
    print("="*60)

    # Shared resources
    data_queue = {
        'sqlite3': Queue(maxsize=10000),
        'mysql': Queue(maxsize=10000),
        'http': Queue(maxsize=10000)
    }
    collecting_active = Event()
    collecting_active.set()
    packet_count = {'sensor': 0, 'intensity': 0, 'filtered': 0, 'error': 0, 'dropped': 0}
    load_dotenv()
    post_url = os.getenv("post_url")
    station = os.getenv("station", "ESPRO")

    utils = Utils()

    collector = SerialDataCollector(data_queue, collecting_active, packet_count, args)
    if args.auto:
        selected_port = collector.find_serial_port_auto()
    else:
        selected_port = collector.select_serial_port()
    if not selected_port:
        print("未選擇串列埠，程式結束。")
        sys.exit(0)

    if not collector.open_serial_port(selected_port):
        print("無法開啟串列埠，程式結束。")
        sys.exit(1)

    threads = []
    db_mysql = None

    if args.sqlite3:
        db = Sqlite3Database(data_queue, collecting_active)
        db_writer_thread = db.start_database_thread()
        threads.append(db_writer_thread)
        print("✓ SQLite3 輸出已啟用")
        ws = WebSocketServer(db.conn_client, port=8765)
        ws_thread = ws.start_in_thread()
        threads.append(ws_thread)
        print("✓ SQLite3 WebSocket 伺服器已啟動")

    if args.mysql:
        db_mysql = MySQLDatabase(data_queue, collecting_active)
        if db_mysql.conn is None:
            print("無法連接到 MySQL 資料庫，程式結束。")
            if not args.sqlite3 and not args.http:
                sys.exit(1)
        else:
            db_mysql_writer_thread = db_mysql.start_database_thread()
            threads.append(db_mysql_writer_thread)
            print("✓ MySQL 輸出已啟用")

    if args.http:
        http_poster = DataHttpPoster(data_queue=data_queue, active_event=collecting_active, post_url=post_url, station=station)
        http_poster_thread = http_poster.start_http_thread()
        threads.append(http_poster_thread)
        print("✓ HTTP 輸出已啟用")

    collector_thread = collector.start_collection_thread(selected_port)
    print("開始收集數據... (按 Ctrl+C 停止)")

    try:
        while collecting_active.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\n正在關閉...")
    finally:
        collecting_active.clear()
        collector_thread.join(timeout=2.0)
        for thread in threads:
            thread.join(timeout=2.0)

        collector.close_serial_port()
        if db_mysql:
            db_mysql.close()

    print("\n" + "="*60)
    print(f"原始三軸封包: {packet_count['sensor']}")
    print(f"濾波三軸封包: {packet_count['filtered']}")
    print(f"震度封包: {packet_count['intensity']}")
    print(f"錯誤封包: {packet_count['error']}")
    print(f"丟棄封包: {packet_count['dropped']}")

    q_sizes = []
    if args.sqlite3:
        q_sizes.append(f"sqlite3: {data_queue['sqlite3'].qsize()}")
    if args.mysql:
        q_sizes.append(f"mysql: {data_queue['mysql'].qsize()}")
    if args.http:
        q_sizes.append(f"http: {data_queue['http'].qsize()}")
    print(f"最終隊列大小: {' / '.join(q_sizes)}")

    print("="*60)
    print("Serial Data Collector Module Finished.")
