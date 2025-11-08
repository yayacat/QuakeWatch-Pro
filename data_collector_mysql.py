import struct
import sys
import time
import serial
import serial.tools.list_ports
import mysql.connector
import os
from datetime import datetime, timezone, timedelta
from threading import Thread, Event
from queue import Queue, Empty, Full
from dotenv import load_dotenv

# --- MySQL 資料庫設定 (從 .env 檔案讀取) ---
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}
# ------------------------------------

BAUD_RATE = 115200

packet_count = {'sensor': 0, 'intensity': 0, 'filtered': 0, 'error': 0, 'dropped': 0}
collecting_active = Event()
collecting_active.set()

# 線程安全隊列，用於在收集線程和資料庫寫入線程之間傳遞數據
# maxsize 可防止記憶體無限制增長
data_queue = Queue(maxsize=10000)

TZ_UTC_8 = timezone(timedelta(hours=8))


def get_timestamp_utc8():
    """獲取 UTC+8 時間戳(毫秒)"""
    now_utc8 = datetime.now(TZ_UTC_8)
    epoch = datetime(1970, 1, 1, tzinfo=TZ_UTC_8)
    return int((now_utc8 - epoch).total_seconds() * 1000)


def init_database():
    """初始化資料庫"""
    try:
        # 先不指定 database，以便檢查並建立它
        conn_check = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            autocommit=True
        )
        cursor = conn_check.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cursor.close()
        conn_check.close()

        # 連接到指定的資料庫
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"✓ 已連接到 MySQL 資料庫: {DB_CONFIG['database']}")

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

        conn.commit()
        print(f"✓ 資料表已確認/建立完成")
        return conn
    except mysql.connector.Error as err:
        print(f"✗ 資料庫錯誤: {err}")
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("   請檢查您的使用者名稱和密碼。")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print(f"   資料庫 '{DB_CONFIG['database']}' 不存在或權限不足。")
        else:
            print(f"   錯誤碼: {err.errno}")
        return None


def read_exact_bytes(ser, num_bytes, timeout_ms=100):
    """
    Windows 相容: 精確讀取指定 bytes
    """
    if os.name != 'nt':  # macOS / Linux - 使用直接讀取
        data = ser.read(num_bytes)
        if len(data) != num_bytes:
            return None
        return data

    # Windows - 使用循環讀取
    data = bytearray()
    start_time = time.time()
    timeout_sec = timeout_ms / 1000.0

    while len(data) < num_bytes:
        if time.time() - start_time > timeout_sec:
            return None

        remaining = num_bytes - len(data)
        chunk = ser.read(remaining)
        if chunk:
            data.extend(chunk)
        elif len(data) == 0:
            time.sleep(0.001)

    return bytes(data)


def parse_serial_data(ser):
    """
    解析串列埠資料
    0x53 = Sensor (原始三軸)
    0x49 = Intensity (震度)
    0x46 = Filtered (濾波三軸)
    """
    try:
        header = read_exact_bytes(ser, 1, timeout_ms=50)
        if header is None:
            return None

        header_byte = header[0]

        if header_byte not in [0x53, 0x49, 0x46]:
            packet_count['error'] += 1
            return None

        # Sensor data (0x53)
        if header_byte == 0x53:
            data_plus_checksum = read_exact_bytes(ser, 21, timeout_ms=100)
            if data_plus_checksum is None:
                packet_count['error'] += 1
                return None

            data = data_plus_checksum[:20]
            checksum = data_plus_checksum[20]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                packet_count['error'] += 1
                return None

            timestamp, x, y, z = struct.unpack('<Qfff', data)
            packet_count['sensor'] += 1
            return ('sensor', timestamp, x, y, z)

        # Intensity data (0x49)
        elif header_byte == 0x49:
            data_plus_checksum = read_exact_bytes(ser, 17, timeout_ms=100)
            if data_plus_checksum is None:
                packet_count['error'] += 1
                return None

            data = data_plus_checksum[:16]
            checksum = data_plus_checksum[16]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                packet_count['error'] += 1
                return None

            timestamp, intensity, a = struct.unpack('<Qff', data)
            if timestamp == 0:
                timestamp = get_timestamp_utc8()
            packet_count['intensity'] += 1
            return ('intensity', timestamp, intensity, a)

        # Filtered data (0x46)
        elif header_byte == 0x46:
            data_plus_checksum = read_exact_bytes(ser, 21, timeout_ms=100)
            if data_plus_checksum is None:
                packet_count['error'] += 1
                return None

            data = data_plus_checksum[:20]
            checksum = data_plus_checksum[20]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                packet_count['error'] += 1
                return None

            timestamp, x, y, z = struct.unpack('<Qfff', data)
            packet_count['filtered'] += 1
            return ('filtered', timestamp, x, y, z)

    except Exception:
        packet_count['error'] += 1
        return None


def write_to_database(conn, data_type, data_list, retries=3, delay=1):
    """批次寫入資料庫，帶有重試機制。如果最終失敗，會拋出 mysql.connector.Error。"""
    if not data_list or conn is None:
        return 0

    last_exception = None
    for attempt in range(retries):
        try:
            # 每次重試都應該獲取新的 cursor，以防 connection 處於不良狀態
            conn.ping(reconnect=True) # 檢查連接並在需要時重新連接
            cursor = conn.cursor()
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

            conn.commit()
            cursor.close()
            return len(data_list)  # 寫入成功，返回
        except mysql.connector.Error as err:
            last_exception = err
            print(f"✗ 資料庫寫入錯誤 (嘗試 {attempt + 1}/{retries}): {err}")
            try:
                conn.rollback()  # 嘗試回滾
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
                    conn.reconnect(attempts=3, delay=2)
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


def cleanup_old_data(conn, retries=3, delay=1):
    """清理超過一天的舊資料，帶有重試和批次刪除機制"""
    if not conn or not conn.is_connected():
        print("[清理] 資料庫未連接，跳過清理")
        return

    last_exception = None
    for attempt in range(retries):
        try:
            conn.ping(reconnect=True)
            cursor = conn.cursor()
            cleanup_datetime = datetime.now(TZ_UTC_8) - timedelta(days=1)
            cleanup_timestamp_ms = int(cleanup_datetime.timestamp() * 1000)
            cleanup_time_str = cleanup_datetime.strftime('%Y-%m-%d %H:%M:%S')

            tables_to_clean = ['sensor_data', 'intensity_data', 'filtered_data']
            total_deleted_rows = 0
            batch_size = 10000  # 每次刪除的筆數

            if attempt == 0:
                print(f"\n[清理] 開始清理時間戳早於 {cleanup_time_str} (UTC+8) 的舊資料 (批次大小: {batch_size})...")

            for table in tables_to_clean:
                table_total_deleted = 0
                while True:
                    # 執行批次刪除
                    sql = f"DELETE FROM {table} WHERE timestamp_ms < %s LIMIT %s"
                    cursor.execute(sql, (cleanup_timestamp_ms, batch_size))
                    deleted_count = cursor.rowcount
                    conn.commit()

                    if deleted_count > 0:
                        table_total_deleted += deleted_count
                        total_deleted_rows += deleted_count
                        # 短暫休眠以降低資料庫負載
                        time.sleep(0.1)

                    # 如果刪除的筆數小於批次大小，表示這個表已經清完了
                    if deleted_count < batch_size:
                        if table_total_deleted > 0:
                            print(f"[清理] ✓ 已從 {table} 刪除 {table_total_deleted} 筆舊資料。")
                        break

            if total_deleted_rows > 0:
                print(f"[清理] ✓ 清理完畢，總共刪除 {total_deleted_rows} 筆舊資料。")
            else:
                print("[清理] ✓ 無舊資料需要清理。")

            cursor.close()
            return
        except mysql.connector.Error as err:
            last_exception = err
            print(f"[清理] ✗ 清理時發生錯誤 (嘗試 {attempt + 1}/{retries}): {err}")
            try:
                conn.rollback()
            except mysql.connector.Error as rb_err:
                print(f"  - 回滾失敗: {rb_err}")

            is_connection_error = err.errno in (
                mysql.connector.errorcode.CR_SERVER_GONE_ERROR,
                mysql.connector.errorcode.CR_SERVER_LOST,
                mysql.connector.errorcode.ER_CON_COUNT_ERROR
            ) or "Lost connection" in str(err) or "MySQL Connection not available" in str(err)

            if is_connection_error:
                print("  - 偵測到連接錯誤，嘗試重新連接...")
                try:
                    conn.reconnect(attempts=3, delay=2)
                    print("  - ✓ 重新連接成功")
                    continue
                except mysql.connector.Error as reconn_err:
                    print(f"  - ✗ 重新連接失敗: {reconn_err}")
                    raise last_exception from reconn_err
            time.sleep(delay)

    if last_exception is not None:
        raise last_exception


def list_serial_ports():
    """列出可用的串列埠"""
    ports = serial.tools.list_ports.comports()
    available_ports = []

    if not ports:
        print("未偵測到串列埠")
        return available_ports

    print("\n可用串列埠:")
    for i, port in enumerate(ports):
        available_ports.append(port.device)
        print(f"[{i}] {port.device} - {port.description}")

    return available_ports


def select_serial_port():
    """選擇串列埠"""
    available_ports = list_serial_ports()

    if not available_ports:
        return None

    if len(available_ports) == 1:
        print(f"自動選擇: {available_ports[0]}")
        return available_ports[0]

    while True:
        try:
            choice = input(
                f"\n請選擇 [0-{len(available_ports)-1}] 或 q 退出: ").strip()
            if choice.lower() == 'q':
                return None
            index = int(choice)
            if 0 <= index < len(available_ports):
                return available_ports[index]
            print(f"請輸入 0-{len(available_ports)-1}")
        except ValueError:
            print("請輸入數字")
        except KeyboardInterrupt:
            return None


def collecting_thread(ser_ref, port_name):
    """資料收集線程 (生產者)"""
    start_time = time.time()
    last_report_time = 0
    last_data_time = time.time()
    reconnect_count = 0

    # NTP 警告設定
    last_ntp_warning = 0
    ntp_warning_interval = 5.0

    print("[收集線程] 已啟動\n")

    while collecting_active.is_set():
        try:
            current_ser = ser_ref['ser']
            if current_ser is None or not current_ser.is_open:
                raise serial.SerialException("串列埠已關閉")

            # 批次處理封包
            result = parse_serial_data(current_ser)

            if result:
                last_data_time = time.time()
                reconnect_count = 0

                # 如果 timestamp 為 0，使用本地時間 (儘管這在生產者中不太理想，但為了保持邏輯)
                if result[1] == 0:
                    current_time = time.time()
                    if current_time - last_ntp_warning >= ntp_warning_interval:
                        print("[警告] 無 NTP 時間戳，使用本地時間計算 (可能不準確)")
                        last_ntp_warning = current_time
                    result = (result[0], get_timestamp_utc8(), *result[2:])

                # 將解析後的數據放入隊列
                try:
                    data_queue.put_nowait(result)
                except Full:
                    packet_count['dropped'] += 1

            # 超時檢查
            if result is None and time.time() - last_data_time > 5.0:
                print("[警告] 超過 5 秒未接收數據")
                raise serial.SerialException("超時未接收數據")

        except (serial.SerialException, OSError) as e:
            print(f"\n[錯誤] {e}")
            reconnect_count += 1

            if reconnect_count > 10:
                print("[錯誤] 重連失敗次數過多，線程終止")
                break

            if ser_ref['ser'] and ser_ref['ser'].is_open: ser_ref['ser'].close()
            time.sleep(2.0)

            try:
                if os.name == 'nt':  # Windows - 使用防重啟配置
                    s = serial.Serial()
                    s.port = port_name
                    s.baudrate = BAUD_RATE
                    s.timeout = 0.05
                    s.xonxoff = False
                    s.rtscts = False
                    s.dsrdtr = False
                    s.dtr = False
                    s.rts = False
                    s.open()

                    time.sleep(0.1)
                    s.reset_input_buffer()
                    s.reset_output_buffer()
                    ser_ref['ser'] = s
                else:  # macOS / Linux - 使用簡單配置
                    ser_ref['ser'] = serial.Serial(port_name, BAUD_RATE, timeout=1)

                print(f"[重連成功] {port_name}")
                last_data_time = time.time()
            except serial.SerialException as re:
                print(f"[重連失敗] {re}")

        # 定期統計
        current_time = time.time()
        if current_time - last_report_time >= 60 or last_report_time == 0:
            elapsed = current_time - start_time
            sensor_rate = packet_count['sensor'] / elapsed if elapsed > 0 else 0
            filtered_rate = packet_count['filtered'] / elapsed if elapsed > 0 else 0
            intensity_rate = packet_count['intensity'] / elapsed if elapsed > 0 else 0

            time_str = datetime.now(TZ_UTC_8).strftime('%H:%M:%S')
            print(f"[統計 {time_str}] "
                  f"原始:{packet_count['sensor']}({sensor_rate:.1f}/s) | "
                  f"濾波:{packet_count['filtered']}({filtered_rate:.1f}/s) | "
                  f"震度:{packet_count['intensity']}({intensity_rate:.1f}/s) | "
                  f"錯誤:{packet_count['error']} | "
                  f"丟棄:{packet_count['dropped']} | "
                  f"隊列:{data_queue.qsize()}")
            last_report_time = current_time

    print("\n[收集線程] 已停止")


def database_writer_thread(conn_ref):
    """資料庫寫入線程 (消費者)，包含定期清理功能"""
    print("[資料庫執行序] 已啟動")

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

    def flush_buffers(conn):
        """寫入所有緩衝區的數據"""
        if not conn or not conn.is_connected():
            print("[資料庫執行序] 連接無效，跳過寫入")
            # 將數據保留在緩衝區中，等待下次重連成功後寫入
            return

        try:
            if sensor_buffer:
                write_to_database(conn, 'sensor', sensor_buffer)
                sensor_buffer.clear()
            if filtered_buffer:
                write_to_database(conn, 'filtered', filtered_buffer)
                filtered_buffer.clear()
            if intensity_buffer:
                write_to_database(conn, 'intensity', intensity_buffer)
                intensity_buffer.clear()
        except mysql.connector.Error as db_err:
            print(f"\n[資料庫執行序] 寫入失敗: {db_err}")
            # 數據保留在緩衝區中，等待下次重試
            # 斷開連接，以便主循環可以嘗試重連
            if conn_ref['conn'] and conn_ref['conn'].is_connected():
                conn_ref['conn'].close()
            conn_ref['conn'] = None


    while collecting_active.is_set() or not data_queue.empty():
        conn = conn_ref['conn']
        current_time = time.time()

        # --- 定期清理 ---
        if conn and conn.is_connected() and (current_time - last_cleanup_time > CLEANUP_INTERVAL):
            cleanup_old_data(conn)
            last_cleanup_time = current_time
        # ----------------

        if conn is None or not conn.is_connected():
            print("[資料庫執行序] 資料庫未連接，等待 3 秒...")
            time.sleep(3)
            try:
                # 嘗試重新建立連接
                conn_ref['conn'] = init_database()
                if conn_ref['conn']:
                    print("[資料庫執行序] ✓ 資料庫重新連接成功")
                    # 連接成功後，立即觸發一次清理
                    last_cleanup_time = 0
            except mysql.connector.Error as err:
                print(f"[資料庫執行序] ✗ 資料庫重新連接失敗: {err}")
            continue # 無論成功失敗，都進入下一次循環

        items_fetched = 0
        try:
            # 批量從隊列中取出數據，直到達到批次大小或隊列為空
            while items_fetched < MAX_BATCH_SIZE:
                data = data_queue.get_nowait()
                data_type = data[0]

                if data_type == 'sensor':
                    sensor_buffer.append(data)
                elif data_type == 'filtered':
                    filtered_buffer.append(data)
                elif data_type == 'intensity':
                    intensity_buffer.append(data)

                data_queue.task_done()
                items_fetched += 1
        except Empty:
            # 隊列為空，正常現象，繼續執行下面的寫入邏輯
            pass

        # 條件：1. 剛才從隊列取出了數據 2. 或者，雖然沒數據但超過了寫入間隔且緩衝區有東西
        should_write = items_fetched > 0
        force_write = (current_time - last_write_time >= WRITE_INTERVAL) and \
                      (sensor_buffer or filtered_buffer or intensity_buffer)

        if should_write or force_write:
            flush_buffers(conn)
            last_write_time = current_time
        else:
            # 如果隊列為空且不需要寫入，短暫休眠以防止CPU空轉
            time.sleep(0.1)

    # 程式結束前，清空所有剩餘的數據
    print("[資料庫執行序] 正在寫入剩餘數據...")
    flush_buffers(conn_ref['conn'])
    print("[資料庫執行序] 已停止")


def main():
    print("QuakeWatch - ES-Net Serial Data Collector (MySQL Version)")
    print("="*60)

    # 資料庫連接現在由寫入線程管理
    conn_ref = {'conn': None}

    selected_port = select_serial_port()
    if not selected_port:
        print("未選擇串列埠，程式結束。")
        sys.exit(0)

    ser = None
    try:
        if os.name == 'nt':  # Windows - 使用防重啟配置
            ser = serial.Serial()
            ser.port = selected_port
            ser.baudrate = BAUD_RATE
            ser.timeout = 0.05
            ser.xonxoff = False
            ser.rtscts = False
            ser.dsrdtr = False
            ser.dtr = False  # 在打開前設定為 False
            ser.rts = False
            ser.open()

            time.sleep(0.1)
            ser.reset_input_buffer()
            ser.reset_output_buffer()
        else:  # macOS / Linux - 使用簡單配置
            ser = serial.Serial(selected_port, BAUD_RATE, timeout=1)

        print(f"\n✓ 已連接: {selected_port} @ {BAUD_RATE} baud")
    except serial.SerialException as e:
        print(f"\n✗ 錯誤: {e}")
        sys.exit(1)

    # 啟動資料庫寫入線程
    db_writer = Thread(target=database_writer_thread, args=(conn_ref,), daemon=True)
    db_writer.start()

    # 啟動串口收集線程
    ser_ref = {'ser': ser}
    collector = Thread(target=collecting_thread, args=(ser_ref, selected_port), daemon=True)
    collector.start()

    print("開始收集數據... (按 Ctrl+C 停止)")

    try:
        # 主線程只需等待，直到手動中斷
        while collecting_active.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\n正在關閉...")
    finally:
        collecting_active.clear()
        # 等待兩個線程正常結束
        collector.join(timeout=2.0)
        print("等待資料庫寫入完成...")
        db_writer.join(timeout=10.0) # 給予資料庫執行序更多時間來清空隊列

    if ser and ser.is_open: ser.close()
    if conn_ref['conn'] and conn_ref['conn'].is_connected(): conn_ref['conn'].close()

    print("\n" + "="*60)
    print(f"原始三軸封包: {packet_count['sensor']}")
    print(f"濾波三軸封包: {packet_count['filtered']}")
    print(f"震度封包: {packet_count['intensity']}")
    print(f"錯誤封包: {packet_count['error']}")
    print(f"丟棄封包: {packet_count['dropped']}")
    print(f"最終隊列大小: {data_queue.qsize()}")
    print("="*60)
    print(f"數據已保存到 MySQL 資料庫: {DB_CONFIG['database']}")

if __name__ == '__main__':
    main()
