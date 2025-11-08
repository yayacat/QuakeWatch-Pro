import serial
import serial.tools.list_ports
import struct
import os
import time
from threading import Thread
from queue import Full
from .utils import Utils

class SerialDataCollector:
    def __init__(self, data_queue, collecting_active, packet_count, args):
        self.utils = Utils()
        self.BAUD_RATE = 115200
        self.packet_count = packet_count
        self.collecting_active = collecting_active
        self.ser_ref = {'ser': None}
        self.data_queue = data_queue
        self.args = args

        self.WRITE_INTERVAL = 0.5
        self.sensor_buffer = []
        self.filtered_buffer = []
        self.intensity_buffer = []

        self.last_ntp_warning = 0
        self.ntp_warning_interval = 5.0

    def _get_timestamp_utc8(self):
        """獲取 UTC+8 時間戳(毫秒)"""
        return self.utils.get_timestamp_utc8()

    def _read_exact_bytes(self, ser, num_bytes, timeout_ms=100):
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

    def _parse_serial_data(self, ser):
        """
        解析串列埠資料
        0x53 = Sensor (原始三軸)
        0x49 = Intensity (震度)
        0x46 = Filtered (濾波三軸)
        """
        try:
            header = self._read_exact_bytes(ser, 1, timeout_ms=50)
            if header is None:
                return None

            header_byte = header[0]

            if header_byte not in [0x53, 0x49, 0x46]:
                self.packet_count['error'] += 1
                return None

            # Sensor data (0x53)
            if header_byte == 0x53:
                data_plus_checksum = self._read_exact_bytes(ser, 21, timeout_ms=100)
                if data_plus_checksum is None:
                    self.packet_count['error'] += 1
                    return None

                data = data_plus_checksum[:20]
                checksum = data_plus_checksum[20]

                calculated_xor = header_byte ^ checksum
                for byte in data:
                    calculated_xor ^= byte

                if calculated_xor != 0:
                    self.packet_count['error'] += 1
                    return None

                timestamp, x, y, z = struct.unpack('<Qfff', data)
                self.packet_count['sensor'] += 1
                return ('sensor', timestamp, x, y, z)

            # Intensity data (0x49)
            elif header_byte == 0x49:
                data_plus_checksum = self._read_exact_bytes(ser, 17, timeout_ms=100)
                if data_plus_checksum is None:
                    self.packet_count['error'] += 1
                    return None

                data = data_plus_checksum[:16]
                checksum = data_plus_checksum[16]

                calculated_xor = header_byte ^ checksum
                for byte in data:
                    calculated_xor ^= byte

                if calculated_xor != 0:
                    self.packet_count['error'] += 1
                    return None

                timestamp, intensity, a = struct.unpack('<Qff', data)
                if timestamp == 0:
                    timestamp = self._get_timestamp_utc8()
                self.packet_count['intensity'] += 1
                return ('intensity', timestamp, intensity, a)

            # Filtered data (0x46)
            elif header_byte == 0x46:
                data_plus_checksum = self._read_exact_bytes(ser, 21, timeout_ms=100)
                if data_plus_checksum is None:
                    self.packet_count['error'] += 1
                    return None

                data = data_plus_checksum[:20]
                checksum = data_plus_checksum[20]

                calculated_xor = header_byte ^ checksum
                for byte in data:
                    calculated_xor ^= byte

                if calculated_xor != 0:
                    self.packet_count['error'] += 1
                    return None

                timestamp, x, y, z = struct.unpack('<Qfff', data)
                self.packet_count['filtered'] += 1
                return ('filtered', timestamp, x, y, z)

        except Exception as e:
            self.packet_count['error'] += 1
            if self.packet_count['error'] % 100 == 0:
                print(f"解析錯誤: {e}")
            return None

    def list_serial_ports(self):
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

    def select_serial_port(self):
        """選擇串列埠"""
        available_ports = self.list_serial_ports()

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
            except (KeyboardInterrupt, EOFError):
                return None

    def find_serial_port_auto(self):
        """Automatically find the serial port."""
        ports = serial.tools.list_ports.comports()
        for port in ports:
            if "Silicon Labs CP210x USB to UART Bridge" in port.description:
                print(f"自動選擇: {port.device}")
                return port.device
        print("未找到指定的串列埠")
        return None

    def open_serial_port(self, port_name):
        """開啟串列埠"""
        try:
            if os.name == 'nt':  # Windows - 使用防重啟配置
                s = serial.Serial()
                s.port = port_name
                s.baudrate = self.BAUD_RATE
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
                self.ser_ref['ser'] = s
            else:  # macOS / Linux - 使用簡單配置
                self.ser_ref['ser'] = serial.Serial(port_name, self.BAUD_RATE, timeout=1)

            print(f"✓ 已開啟串列埠: {port_name}")
            return True
        except serial.SerialException as e:
            print(f"[錯誤] 無法開啟串列埠 {port_name}: {e}")
            return False

    def _collection_loop(self, port_name):
        """資料收集線程的核心循環"""
        start_time = time.time()
        last_report_time = 0
        last_data_time = time.time()
        reconnect_count = 0
        ntp_warning_interval = 5.0
        last_ntp_warning = 0
        last_write_time = time.time()


        print("[收集線程] 已啟動\n")

        while self.collecting_active.is_set():
            try:
                current_ser = self.ser_ref['ser']
                if current_ser is None or not current_ser.is_open:
                    raise serial.SerialException("串列埠已關閉")

                # 批次處理封包
                timestamp_start = self._get_timestamp_utc8()
                batch_results = []

                for _ in range(20):
                    result = self._parse_serial_data(current_ser)
                    if result is None:
                        break
                    batch_results.append(result)
                    last_data_time = time.time()
                    reconnect_count = 0

                # 計算時間間隔並分配時間戳
                current_time = time.time()
                if batch_results:
                    timestamp_end = self._get_timestamp_utc8()
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
                            self.sensor_buffer.append(result)
                        elif data_type == 'filtered':
                            self.filtered_buffer.append(result)
                        elif data_type == 'intensity':
                            self.intensity_buffer.append(result)

                    # NTP 警告
                    if has_no_ntp and (current_time - last_ntp_warning >= ntp_warning_interval):
                        print("[警告] 無 NTP 時間戳，使用本地時間計算 (可能不準確)")
                        last_ntp_warning = current_time

                # 定期寫入緩衝區數據到隊列
                current_time = time.time()
                if current_time - last_write_time >= self.WRITE_INTERVAL:
                    last_write_time = current_time
                    if self.sensor_buffer or self.filtered_buffer or self.intensity_buffer:
                        sensor_data, filtered_data, intensity_data = self.get_and_clear_buffers()

                        # 將資料放入佇列，使用帶有短暫超時的 put
                        # 如果佇列已滿，等待最多 100 毫秒
                        # 如果仍然滿，則記錄丟棄並繼續，避免無限期阻塞
                        put_timeout = 0.1 # 100ms

                        try:
                            if sensor_data:
                                if self.args.http:
                                    self.data_queue['http'].put(('sensor', sensor_data), timeout=put_timeout)
                                if self.args.sqlite3:
                                    self.data_queue['sqlite3'].put(('sensor', sensor_data), timeout=put_timeout)
                                if self.args.mysql:
                                    self.data_queue['mysql'].put(('sensor', sensor_data), timeout=put_timeout)
                            if filtered_data:
                                if self.args.sqlite3:
                                    self.data_queue['sqlite3'].put(('filtered', filtered_data), timeout=put_timeout)
                                if self.args.mysql:
                                    self.data_queue['mysql'].put(('filtered', filtered_data), timeout=put_timeout)
                            if intensity_data:
                                if self.args.sqlite3:
                                    self.data_queue['sqlite3'].put(('intensity', intensity_data), timeout=put_timeout)
                                if self.args.mysql:
                                    self.data_queue['mysql'].put(('intensity', intensity_data), timeout=put_timeout)
                        except Full:
                            # 計算此批次丟棄的總點數
                            dropped_count = len(sensor_data) + len(filtered_data) + len(intensity_data)
                            self.packet_count['dropped'] += dropped_count
                            # 只有在佇列滿時才印出警告，避免洗版
                            q_sizes = {
                                'http': self.data_queue['http'].qsize(),
                                'sqlite3': self.data_queue['sqlite3'].qsize(),
                                'mysql': self.data_queue['mysql'].qsize()
                            }
                            print(f"[警告] 佇列已滿，資料被丟棄! 當前大小: {q_sizes}")


                # 超時檢查
                if result is None and time.time() - last_data_time > 5.0:
                    print("[警告] 超過 5 秒未接收數據")
                    raise serial.SerialException("超時未接收數據")

            except (serial.SerialException, OSError) as e:
                print(f"\n[錯誤] {e}")
                reconnect_count += 1

                if reconnect_count > 10:
                    print("[錯誤] 重連失敗次數過多")
                    break

                try:
                    if self.ser_ref['ser'] and self.ser_ref['ser'].is_open:
                        self.ser_ref['ser'].close()
                except:
                    pass

                time.sleep(2.0)
                try:
                    if os.name == 'nt':  # Windows - 使用防重啟配置
                        s = serial.Serial()
                        s.port = port_name
                        s.baudrate = self.BAUD_RATE
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
                        self.ser_ref['ser'] = s
                    else:  # macOS / Linux - 使用簡單配置
                        self.ser_ref['ser'] = serial.Serial(port_name, self.BAUD_RATE, timeout=1)

                    print(f"[重連成功] {port_name}")
                    last_data_time = time.time()
                except serial.SerialException as re:
                    print(f"[重連失敗] {re}")
                    continue

            # 定期統計
            current_time = time.time()
            if current_time - last_report_time >= 60 or last_report_time == 0:
                elapsed = current_time - start_time
                sensor_rate = self.packet_count['sensor'] / \
                    elapsed if elapsed > 0 else 0
                filtered_rate = self.packet_count['filtered'] / \
                    elapsed if elapsed > 0 else 0
                intensity_rate = self.packet_count['intensity'] / \
                    elapsed if elapsed > 0 else 0
                filtered_rate = self.packet_count['filtered'] / \
                    elapsed if elapsed > 0 else 0
                error_rate = self.packet_count['error'] / \
                    elapsed if elapsed > 0 else 0

                time_str = self.utils.get_now_utc8().strftime('%H:%M:%S')
                print(f"[統計 {time_str}] "
                      f"原始:{self.packet_count['sensor']}({sensor_rate:.1f}/s) | "
                      f"濾波:{self.packet_count['filtered']}({filtered_rate:.1f}/s) | "
                      f"震度:{self.packet_count['intensity']}({intensity_rate:.1f}/s) | "
                      f"錯誤:{self.packet_count['error']}({error_rate:.1f}/s) | "
                      f"丟棄:{self.packet_count['dropped']} | "
                      f"隊列:{self.data_queue['sqlite3'].qsize()}/{self.data_queue['mysql'].qsize()}/{self.data_queue['http'].qsize()}")
                last_report_time = current_time

        # Removed: write remaining data to database
        print("\n[收集線程] 已停止")

    def start_collection_thread(self, port_name):
        """啟動資料收集線程"""
        collector_thread = Thread(target=self._collection_loop, args=(port_name,), daemon=True)
        collector_thread.start()
        return collector_thread

    def stop_collection(self):
        """停止資料收集"""
        self.collecting_active.clear()

    def close_serial_port(self):
        """關閉串列埠"""
        try:
            if self.ser_ref['ser'] and self.ser_ref['ser'].is_open:
                self.ser_ref['ser'].close()
        except:
            pass

    # Removed: close_database method

    def get_and_clear_buffers(self):
        """獲取並清空緩衝區中的數據"""
        sensor_data = self.sensor_buffer
        filtered_data = self.filtered_buffer
        intensity_data = self.intensity_buffer

        self.sensor_buffer = []
        self.filtered_buffer = []
        self.intensity_buffer = []

        return sensor_data, filtered_data, intensity_data