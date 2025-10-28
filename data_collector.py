"""
QuakeWatch - ES-Net Serial Data Collector (診斷版)
地震 ESP32 資料收集器 - 加強診斷功能
"""

import serial
import serial.tools.list_ports
import struct
import sys
import time
import sqlite3
import signal
from datetime import datetime, timezone, timedelta
from threading import Thread, Event
from collections import defaultdict

BAUD_RATE = 115200
DB_FILE = 'earthquake_data.db'

packet_count = {'sensor': 0, 'intensity': 0, 'filtered': 0, 'error': 0}
error_details = defaultdict(int)  # 詳細錯誤統計
collecting_active = Event()
collecting_active.set()

TZ_UTC_8 = timezone(timedelta(hours=8))


def get_timestamp_utc8():
    """獲取 UTC+8 時間戳(毫秒)"""
    now_utc8 = datetime.now(TZ_UTC_8)
    epoch = datetime(1970, 1, 1, tzinfo=TZ_UTC_8)
    return int((now_utc8 - epoch).total_seconds() * 1000)


def init_database():
    """初始化資料庫"""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cursor = conn.cursor()

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
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_sensor_timestamp ON sensor_data(timestamp_ms)')

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
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_intensity_timestamp ON intensity_data(timestamp_ms)')

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
    cursor.execute(
        'CREATE INDEX IF NOT EXISTS idx_filtered_timestamp ON filtered_data(timestamp_ms)')

    conn.commit()
    print(f"✓ 數據庫已初始化: {DB_FILE}")
    return conn


def parse_serial_data(ser):
    """
    解析串列埠資料
    0x53 = Sensor (原始三軸)
    0x49 = Intensity (震度)
    0x46 = Filtered (濾波三軸)
    """
    try:
        header = ser.read(1)
        if len(header) != 1:
            error_details['header_timeout'] += 1
            return None

        header_byte = header[0]

        if header_byte not in [0x53, 0x49, 0x46]:
            error_details[f'invalid_header_0x{header_byte:02X}'] += 1
            packet_count['error'] += 1
            return None

        # Sensor data (0x53)
        if header_byte == 0x53:
            data_plus_checksum = ser.read(21)
            if len(data_plus_checksum) != 21:
                error_details['sensor_incomplete'] += 1
                packet_count['error'] += 1
                return None

            data = data_plus_checksum[:20]
            checksum = data_plus_checksum[20]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                error_details['sensor_checksum_fail'] += 1
                packet_count['error'] += 1
                return None

            timestamp, x, y, z = struct.unpack('<Qfff', data)
            packet_count['sensor'] += 1
            return ('sensor', timestamp, x, y, z)

        # Intensity data (0x49)
        elif header_byte == 0x49:
            data_plus_checksum = ser.read(17)
            if len(data_plus_checksum) != 17:
                error_details['intensity_incomplete'] += 1
                packet_count['error'] += 1
                return None

            data = data_plus_checksum[:16]
            checksum = data_plus_checksum[16]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                error_details['intensity_checksum_fail'] += 1
                packet_count['error'] += 1
                return None

            timestamp, intensity, a = struct.unpack('<Qff', data)
            if timestamp == 0:
                timestamp = get_timestamp_utc8()
            packet_count['intensity'] += 1
            return ('intensity', timestamp, intensity, a)

        # Filtered data (0x46)
        elif header_byte == 0x46:
            data_plus_checksum = ser.read(21)
            if len(data_plus_checksum) != 21:
                error_details['filtered_incomplete'] += 1
                packet_count['error'] += 1
                # 診斷: 顯示實際讀取的長度
                if len(data_plus_checksum) > 0:
                    error_details[f'filtered_len_{len(data_plus_checksum)}'] += 1
                return None

            data = data_plus_checksum[:20]
            checksum = data_plus_checksum[20]

            calculated_xor = header_byte ^ checksum
            for byte in data:
                calculated_xor ^= byte

            if calculated_xor != 0:
                error_details['filtered_checksum_fail'] += 1
                packet_count['error'] += 1
                # 診斷: 顯示前幾個 bytes
                if error_details['filtered_checksum_fail'] <= 5:
                    print(f"[DEBUG] Filtered checksum 失敗:")
                    print(f"  Header: 0x{header_byte:02X}")
                    print(
                        f"  Data[:8]: {' '.join(f'{b:02X}' for b in data[:8])}")
                    print(
                        f"  Checksum: 0x{checksum:02X}, Calc: 0x{calculated_xor:02X}")
                return None

            timestamp, x, y, z = struct.unpack('<Qfff', data)
            packet_count['filtered'] += 1
            return ('filtered', timestamp, x, y, z)

    except Exception as e:
        error_details[f'exception_{type(e).__name__}'] += 1
        packet_count['error'] += 1
        if packet_count['error'] % 100 == 0:
            print(f"解析錯誤: {e}")
        return None


def write_to_database(conn, data_type, data_list):
    """批次寫入資料庫"""
    if not data_list:
        return 0

    cursor = conn.cursor()
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

    conn.commit()
    return len(data_list)


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


def collecting_thread(ser_ref, conn, port_name):
    """資料收集線程"""
    start_time = time.time()
    last_report_time = time.time()
    last_write_time = time.time()
    last_data_time = time.time()
    last_diagnostic_time = time.time()
    reconnect_count = 0

    # 緩衝區設定 - 使用延遲寫入策略
    BUFFER_DELAY_MS = 500  # 只寫入 500ms 前的數據，確保晚到的數據能排序
    WRITE_INTERVAL = 0.5
    sensor_buffer = []
    filtered_buffer = []
    intensity_buffer = []

    # NTP 警告設定
    last_ntp_warning = 0
    ntp_warning_interval = 5.0

    print("[收集線程] 已啟動 (延遲寫入: 500ms)\n")

    while collecting_active.is_set():
        try:
            current_ser = ser_ref['ser']
            if current_ser is None or not current_ser.is_open:
                raise serial.SerialException("串列埠已關閉")

            # 批次處理封包
            timestamp_start = get_timestamp_utc8()
            batch_results = []

            for _ in range(20):
                result = parse_serial_data(current_ser)
                if result is None:
                    break
                batch_results.append(result)
                last_data_time = time.time()
                reconnect_count = 0

            # 計算時間間隔並分配時間戳
            current_time = time.time()
            if batch_results:
                timestamp_end = get_timestamp_utc8()
                batch_count = len(batch_results)
                time_interval = (timestamp_end - timestamp_start) / \
                    batch_count if batch_count > 1 else 0

                has_no_ntp = False
                for i, result in enumerate(batch_results):
                    # 如果 timestamp 為 0，使用平分的時間
                    if result[1] == 0:
                        has_no_ntp = True
                        calculated_timestamp = timestamp_start + \
                            int(i * time_interval)
                        result = (result[0], calculated_timestamp, *result[2:])

                    # 分類存入緩衝區
                    data_type = result[0]
                    if data_type == 'sensor':
                        sensor_buffer.append(result)
                    elif data_type == 'filtered':
                        filtered_buffer.append(result)
                    elif data_type == 'intensity':
                        intensity_buffer.append(result)

                # NTP 警告
                if has_no_ntp and (current_time - last_ntp_warning >= ntp_warning_interval):
                    print("[警告] 無 NTP 時間戳，使用本地時間計算 (可能不準確)")
                    last_ntp_warning = current_time

            # 延遲寫入機制：只寫入足夠舊的數據
            current_timestamp = get_timestamp_utc8()
            cutoff_timestamp = current_timestamp - BUFFER_DELAY_MS

            if current_time - last_write_time >= WRITE_INTERVAL:
                # 過濾出可以寫入的數據（時間戳小於 cutoff）
                sensor_to_write = [
                    d for d in sensor_buffer if d[1] < cutoff_timestamp]
                filtered_to_write = [
                    d for d in filtered_buffer if d[1] < cutoff_timestamp]
                intensity_to_write = [
                    d for d in intensity_buffer if d[1] < cutoff_timestamp]

                # 寫入並移除已寫入的數據
                wrote_sensor = write_to_database(
                    conn, 'sensor', sensor_to_write)
                wrote_filtered = write_to_database(
                    conn, 'filtered', filtered_to_write)
                wrote_intensity = write_to_database(
                    conn, 'intensity', intensity_to_write)

                # 保留未寫入的數據（保留在緩衝區中等待後續排序）
                sensor_buffer = [
                    d for d in sensor_buffer if d[1] >= cutoff_timestamp]
                filtered_buffer = [
                    d for d in filtered_buffer if d[1] >= cutoff_timestamp]
                intensity_buffer = [
                    d for d in intensity_buffer if d[1] >= cutoff_timestamp]

                last_write_time = current_time

            # 超時檢查
            if batch_results == [] and current_time - last_data_time > 5.0:
                print("[警告] 超過 5 秒未接收數據")
                raise serial.SerialException("超時未接收數據")

        except (serial.SerialException, OSError) as e:
            print(f"\n[錯誤] {e}")
            reconnect_count += 1

            if reconnect_count > 10:
                print("[錯誤] 重連失敗次數過多")
                break

            try:
                if ser_ref['ser'] and ser_ref['ser'].is_open:
                    ser_ref['ser'].close()
            except:
                pass

            time.sleep(2.0)
            try:
                ser_ref['ser'] = serial.Serial(port_name, BAUD_RATE, timeout=1)
                print(f"[重連成功] {port_name}")
                last_data_time = time.time()
            except serial.SerialException as re:
                print(f"[重連失敗] {re}")
                continue

        # 定期統計
        current_time = time.time()
        if current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            sensor_rate = packet_count['sensor'] / \
                elapsed if elapsed > 0 else 0
            filtered_rate = packet_count['filtered'] / \
                elapsed if elapsed > 0 else 0
            intensity_rate = packet_count['intensity'] / \
                elapsed if elapsed > 0 else 0

            time_str = datetime.now(TZ_UTC_8).strftime('%H:%M:%S')
            print(f"[統計 {time_str}] "
                  f"原始:{packet_count['sensor']}({sensor_rate:.1f}/s) | "
                  f"濾波:{packet_count['filtered']}({filtered_rate:.1f}/s) | "
                  f"震度:{packet_count['intensity']}({intensity_rate:.1f}/s) | "
                  f"錯誤:{packet_count['error']}")
            last_report_time = current_time

        # 每 15 秒顯示詳細錯誤診斷
        if current_time - last_diagnostic_time >= 15.0:
            if error_details:
                print("\n=== 錯誤診斷 ===")
                for error_type, count in sorted(error_details.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {error_type}: {count}")
                print("================\n")
            last_diagnostic_time = current_time

    # 寫入剩餘資料
    write_to_database(conn, 'sensor', sensor_buffer)
    write_to_database(conn, 'filtered', filtered_buffer)
    write_to_database(conn, 'intensity', intensity_buffer)

    print("\n[收集線程] 已停止")


def signal_handler(sig, frame):
    """中斷處理"""
    print("\n\n正在關閉...")
    collecting_active.clear()


def main():
    print("QuakeWatch - ES-Net Serial Data Collector (診斷版)")
    print("="*60)

    conn = init_database()

    selected_port = select_serial_port()
    if not selected_port:
        print("未選擇串列埠")
        conn.close()
        sys.exit(0)

    try:
        ser = serial.Serial(selected_port, BAUD_RATE, timeout=1)
        print(f"\n✓ 已連接: {selected_port} @ {BAUD_RATE} baud\n")
    except serial.SerialException as e:
        print(f"\n✗ 錯誤: {e}")
        conn.close()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)

    ser_ref = {'ser': ser}
    collector = Thread(target=collecting_thread, args=(
        ser_ref, conn, selected_port), daemon=True)
    collector.start()

    print("開始收集數據... (按 Ctrl+C 停止)")
    print("提示: 將每 15 秒顯示詳細錯誤統計\n")

    try:
        while collecting_active.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        signal_handler(None, None)

    collector.join(timeout=2.0)

    try:
        if ser_ref['ser'] and ser_ref['ser'].is_open:
            ser_ref['ser'].close()
    except:
        pass
    conn.close()

    print("\n" + "="*60)
    print(f"原始三軸封包: {packet_count['sensor']}")
    print(f"濾波三軸封包: {packet_count['filtered']}")
    print(f"震度封包: {packet_count['intensity']}")
    print(f"錯誤封包: {packet_count['error']}")

    if error_details:
        print("\n=== 最終錯誤統計 ===")
        for error_type, count in sorted(error_details.items(), key=lambda x: x[1], reverse=True):
            print(f"  {error_type}: {count}")

    print("="*60)
    print("數據已保存到:", DB_FILE)


if __name__ == '__main__':
    main()
