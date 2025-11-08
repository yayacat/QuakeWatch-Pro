import io
import struct
import sys
import time
import serial
import serial.tools.list_ports
import os
from datetime import datetime, timezone, timedelta
from threading import Thread, Event
from queue import Queue, Empty, Full
from dotenv import load_dotenv
import socket
from obspy import Trace, Stream, UTCDateTime
import numpy as np
import requests
import json

# --- SeedLink Server Settings ---
SEEDLINK_HOST = '0.0.0.0'
SEEDLINK_PORT = 18000
NETWORK_CODE = "TW"
STATION_CODE = "ES01"
# ------------------------------------

BAUD_RATE = 115200

packet_count = {'sensor': 0, 'intensity': 0, 'filtered': 0, 'error': 0, 'dropped': 0}
collecting_active = Event()
collecting_active.set()

# Thread-safe queue for passing MiniSEED data from collector to server thread
data_queue = Queue(maxsize=10000)

# Thread-safe queue for passing data to the HTTP poster thread
http_data_queue = Queue(maxsize=10000)

TZ_UTC_8 = timezone(timedelta(hours=8))

client_queues = []

def get_timestamp_utc8():
    """Get UTC+8 timestamp in milliseconds"""
    now_utc8 = datetime.now(TZ_UTC_8)
    epoch = datetime(1970, 1, 1, tzinfo=TZ_UTC_8)
    return int((now_utc8 - epoch).total_seconds() * 1000)


def read_exact_bytes(ser, num_bytes, timeout_ms=100):
    """
    Windows compatible: read exact number of bytes
    """
    if os.name != 'nt':  # macOS / Linux - use direct read
        data = ser.read(num_bytes)
        if len(data) != num_bytes:
            return None
        return data

    # Windows - use loop read
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
    Parse serial port data
    0x53 = Sensor (raw tri-axis)
    0x49 = Intensity
    0x46 = Filtered (tri-axis)
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


def list_serial_ports():
    """List available serial ports"""
    ports = serial.tools.list_ports.comports()
    available_ports = []

    if not ports:
        print("未偵測到任何串列埠")
        return available_ports

    print("\n可用的串列埠:")
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
                f"\n請選擇 [0-{len(available_ports)-1}] 或 q 離開: ").strip()
            if choice.lower() == 'q':
                return None
            index = int(choice)
            if 0 <= index < len(available_ports):
                return available_ports[index]
            print(f"請輸入 0 到 {len(available_ports)-1} 之間的數字")
        except ValueError:
            print("請輸入一個數字")
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

            result = parse_serial_data(current_ser)

            if result:
                try:
                    # Put raw data into the queue for the HTTP poster
                    http_data_queue.put_nowait(result)
                except Full:
                    # This could be logged if needed, but for now, we'll just pass
                    pass

                last_data_time = time.time()
                reconnect_count = 0

                if result[1] == 0:
                    current_time = time.time()
                    if current_time - last_ntp_warning >= ntp_warning_interval:
                        print("[警告] 沒有 NTP 時間戳，使用本地時間 (可能不準確)")
                        last_ntp_warning = current_time
                    result = (result[0], get_timestamp_utc8(), *result[2:])

                # 建立 MiniSEED 封包
                data_type, timestamp_ms, *values = result

                # 將 ms 時間戳轉換為 UTCDateTime
                dt = UTCDateTime(timestamp_ms / 1000.0)

                traces = []
                if data_type == 'sensor':
                    # 為每個通道 (X, Y, Z) 建立一個 trace
                    for i, channel in enumerate(['E', 'N', 'Z']):
                        stats = {
                            'network': NETWORK_CODE,
                            'station': STATION_CODE,
                            'location': '00',
                            'channel': f'BH{channel}',
                            'starttime': dt,
                            'sampling_rate': 50, # 假設 50Hz，如有需要請調整
                            'npts': 1,
                            'calib': 1.0
                        }
                        trace = Trace(data=np.array([values[i]], dtype=np.float32), header=stats)
                        traces.append(trace)

                elif data_type == 'filtered':
                    # 為每個通道 (H1, H2, V) 建立一個 trace
                    for i, channel in enumerate(['1', '2', 'Z']):
                        stats = {
                            'network': NETWORK_CODE,
                            'station': STATION_CODE,
                            'location': '00',
                            'channel': f'BD{channel}',
                            'starttime': dt,
                            'sampling_rate': 50, # 假設 50Hz，如有需要請調整
                            'npts': 1,
                            'calib': 1.0
                        }
                        trace = Trace(data=np.array([values[i]], dtype=np.float32), header=stats)
                        traces.append(trace)

                if traces:
                    stream = Stream(traces=traces)
                    try:
                        # 將 stream 放入隊列，供伺服器線程使用
                        data_queue.put_nowait(stream)
                    except Full:
                        packet_count['dropped'] += 1


            # 逾時檢查
            if result is None and time.time() - last_data_time > 5.0:
                print("[警告] 超過 5 秒未收到資料")
                raise serial.SerialException("接收資料逾時")

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

                print(f"[重新連線] {port_name}")
                last_data_time = time.time()
            except serial.SerialException as re:
                print(f"[重新連線失敗] {re}")

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

def data_distribution_thread():
    """Distributes data from the main queue to all client queues."""
    while collecting_active.is_set():
        try:
            stream = data_queue.get(timeout=1.0)
            for q in client_queues[:]:
                try:
                    q.put_nowait(stream)
                except Full:
                    # If a client queue is full, we just drop the data for that client
                    pass
            data_queue.task_done()
        except Empty:
            pass

# class LineReader:
#     """Helper class to read CRLF-terminated lines from a socket."""
#     def __init__(self, conn):
#         self.conn = conn
#         self.buffer = b''

#     def readline(self):
#         while b'\r\n' not in self.buffer:
#             try:
#                 data = self.conn.recv(1024)
#                 if not data:
#                     return None # Connection closed
#                 self.buffer += data
#             except socket.timeout:
#                 # If a timeout occurs, return any data currently in the buffer
#                 # This handles clients that don't send CRLF correctly
#                 if self.buffer:
#                     line = self.buffer
#                     self.buffer = b''
#                     return line.strip().decode()
#                 return ""
#         line, self.buffer = self.buffer.split(b'\r\n', 1)
#         return line.decode()

# def client_handler_thread(conn, addr, q):
#     """Handles a single client connection with proper SeedLink protocol handling."""
#     time_str = datetime.now(TZ_UTC_8).strftime('%H:%M:%S')
#     print(f"[SeedLink 伺服器 {time_str}] 來自 {addr} 的新連線")
#     try:
#         conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
#         conn.settimeout(10.0) # 10 second timeout for commands

#         # --- Handshake ---
#         # Use a simple recv for the first HELLO message for robustness
#         initial_data = conn.recv(1024)
#         if not initial_data:
#             return # Client disconnected immediately

#         # Manually parse the first line and handle leftovers
#         buffer = initial_data
#         if b'\r\n' in buffer:
#             hello_cmd, buffer = buffer.split(b'\r\n', 1)
#         else:
#             hello_cmd = buffer
#             buffer = b''

#         if b'HELLO' in hello_cmd:
#             conn.sendall(b"OK\r\n")
#         else:
#             return # Did not receive HELLO

#         # --- Command Loop ---
#         # Use a line reader for subsequent commands, seeded with any leftover data
#         reader = LineReader(conn)
#         reader.buffer = buffer

#         while collecting_active.is_set():
#             command = reader.readline()
#             if command is None: # Connection closed
#                 break
#             if not command: # Timeout or empty line
#                 continue

#             if command.startswith("INFO"):
#                 if "STREAMS" in command:
#                     stream_info = (
#                         f"{NETWORK_CODE} {STATION_CODE} 00 BHE,BHN,BHZ\r\n"
#                         f"{NETWORK_CODE} {STATION_CODE} 00 BD1,BD2,BDZ\r\n"
#                     )
#                     conn.sendall(stream_info.encode())
#                 else: # Respond to other INFO requests with a generic ID
#                     conn.sendall(b"QuakeWatch-Pro SeedLink Server\r\n")
#                 conn.sendall(b"END\r\n")
#             elif command.startswith("SELECT"):
#                 conn.sendall(b"OK\r\n")
#             elif command == "DATA":
#                 break # Exit command loop and start streaming
#             elif command == "END":
#                 return # Client finished

#         # --- Streaming Loop ---
#         conn.settimeout(None) # Disable timeout for streaming
#         while collecting_active.is_set():
#             try:
#                 stream = q.get(timeout=1.0)
#                 buffer = io.BytesIO()
#                 stream.write(buffer, format="MSEED", reclen=512)
#                 buffer.seek(0)
#                 mseed_data = buffer.read()
#                 conn.sendall(mseed_data)
#             except Empty:
#                 conn.sendall(b'\r\n') # Heartbeat
#                 continue

#     except (socket.error, BrokenPipeError, ConnectionResetError):
#         pass # Silently handle client disconnection
#     finally:
#         time_str = datetime.now(TZ_UTC_8).strftime('%H:%M:%S')
#         print(f"[SeedLink 伺服器 {time_str}] 客戶端 {addr} 已斷線")
#         if q in client_queues:
#             client_queues.remove(q)
#         conn.close()

def http_poster_thread():
    """
    Thread to collect data and post it to an HTTP server.
    """
    buffer = {
        'Timestamp': None,
        'Channel_1': [],
        'Channel_2': [],
        'Channel_3': []
    }
    last_post_time = time.time()
    POST_INTERVAL = 1.0  # Post every 1 second

    print("[HTTP Poster] Thread started\n")

    while collecting_active.is_set():
        try:
            # Get data from the queue, with a timeout to allow checking the post interval
            data_type, timestamp_ms, *values = http_data_queue.get(timeout=0.1)

            if data_type in ('sensor', 'filtered'):
                if buffer['Timestamp'] is None:
                    buffer['Timestamp'] = timestamp_ms

                # Assuming values are [x, y, z]
                if len(values) == 3:
                    buffer['Channel_1'].append(int(values[0]))
                    buffer['Channel_2'].append(int(values[1]))
                    buffer['Channel_3'].append(int(values[2]))

        except Empty:
            # This is expected when the queue is empty.
            # We'll proceed to check if it's time to post any buffered data.
            pass

        current_time = time.time()
        # Check if it's time to post and if there's data in the buffer
        if (current_time - last_post_time >= POST_INTERVAL) and buffer['Channel_1']:
            payload = {
                "SampleRate": 50,  # As specified
                "Timestamp": buffer['Timestamp'],
                "Channel_1": buffer['Channel_1'],
                "Channel_2": buffer['Channel_2'],
                "Channel_3": buffer['Channel_3'],
            }
            try:
                response = requests.post("http://localhost:18080/data", json=payload, timeout=2.0)
                if response.status_code >= 400:
                    print(f"[HTTP Poster] Error: Server returned status {response.status_code} - {response.text}")
            except requests.RequestException as e:
                print(f"[HTTP Poster] Error sending data: {e}")

            # Reset buffer for the next batch
            buffer = {
                'Timestamp': None,
                'Channel_1': [],
                'Channel_2': [],
                'Channel_3': []
            }
            last_post_time = current_time

    print("[HTTP Poster] Thread stopped")


# def seedlink_server_thread():
#     """SeedLink 伺服器線程 (消費者)"""
#     print("[SeedLink 伺服器] 已啟動")

#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#         server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#         server_socket.bind((SEEDLINK_HOST, SEEDLINK_PORT))
#         server_socket.listen()
#         server_socket.settimeout(1.0) # 接受連線的逾時
#         print(f"[SeedLink 伺服器] 正在監聽 {SEEDLINK_HOST}:{SEEDLINK_PORT}")

#         while collecting_active.is_set():
#             try:
#                 conn, addr = server_socket.accept()
#                 q = Queue(maxsize=100) # Create a queue for this client
#                 client_queues.append(q)
#                 client_thread = Thread(target=client_handler_thread, args=(conn, addr, q), daemon=True)
#                 client_thread.start()
#             except socket.timeout:
#                 pass # 正常逾時，繼續檢查資料

#     print("[SeedLink 伺服器] 已停止")


def main():
    print("QuakeWatch - ES-Net 串列資料收集器 (SeedLink 伺服器版本)")
    print("="*60)

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
            ser.dtr = False
            ser.rts = False
            ser.open()

            time.sleep(0.1)
            ser.reset_input_buffer()
            ser.reset_output_buffer()
        else:  # macOS / Linux - 使用簡單配置
            ser = serial.Serial(selected_port, BAUD_RATE, timeout=1)

        print(f"\n✓ 已連線: {selected_port} @ {BAUD_RATE} baud")
    except serial.SerialException as e:
        print(f"\n✗ 錯誤: {e}")
        sys.exit(1)

    # 啟動 SeedLink 伺服器線程
    # server = Thread(target=seedlink_server_thread, daemon=True)
    # server.start()

    # 啟動串列收集線程
    ser_ref = {'ser': ser}
    collector = Thread(target=collecting_thread, args=(ser_ref, selected_port), daemon=True)
    collector.start()

    # 啟動資料分發線程
    distributor = Thread(target=data_distribution_thread, daemon=True)
    distributor.start()

    # Start the HTTP posting thread
    http_thread = Thread(target=http_poster_thread, daemon=True)
    http_thread.start()

    print("開始收集資料... (按 Ctrl+C 停止)")

    try:
        while collecting_active.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n\n正在關閉...")
    finally:
        collecting_active.clear()
        collector.join(timeout=2.0)
        # server.join(timeout=2.0)
        distributor.join(timeout=2.0)
        http_thread.join(timeout=2.0)

    if ser and ser.is_open: ser.close()

    print("\n" + "="*60)
    print(f"原始三軸封包: {packet_count['sensor']}")
    print(f"濾波三軸封包: {packet_count['filtered']}")
    print(f"震度封包: {packet_count['intensity']}")
    print(f"錯誤封包: {packet_count['error']}")
    print(f"丟棄封包: {packet_count['dropped']}")
    print(f"最終隊列大小: {data_queue.qsize()}")
    print("="*60)

if __name__ == '__main__':
    main()
