import time
import requests
import queue
from threading import Thread, Event
from queue import Queue, Empty

class DataHttpPoster:
    """
    Thread to collect data from a queue and post it to an HTTP server.
    """
    def __init__(self, data_queue: Queue, active_event: Event, post_url: str, post_interval: float = 1.0, sample_rate: int = 50, station: str = "ESPRO", timeout: float = 10.0):
        super().__init__()
        self.data_queue = data_queue
        self.active_event = active_event
        self.post_url = post_url
        self.post_interval = post_interval
        self.sample_rate = sample_rate
        self.timeout = timeout
        self.daemon = True
        # self._buffer = {
        #     'Timestamp': None,
        #     'SeedName': '',
        #     'Channel_1': [],
        #     'Channel_2': [],
        #     'Channel_3': [],
        # }
        # self._last_post_time = time.time()
        self.station = station

        self.session = requests.Session()

    def http_poster_thread(self):
        """
        The main loop of the thread.
        """
        print("[HTTP Poster] Thread started\n")

        # 緩衝區設定
        WRITE_INTERVAL = 0.0  # 每 0.5 秒寫入一次
        MAX_BATCH_SIZE = 10000  # 或緩衝區達到 10000 筆數據時寫入
        last_write_time = time.time()

        sensor_buffer = []

        def flush_buffers():
            if not sensor_buffer:
                return

            # 只需要拿第一筆資料的時間即可
            start_timestamp = sensor_buffer[0][1]

            batched_data = {
                'ELN': [],
                'ELE': [],
                'ELZ': []
            }

            # 對應 index: 2->ELN, 3->ELE, 4->ELZ
            SEED_CONFIG = {2: 'ELN', 3: 'ELE', 4: 'ELZ'}

            # 資料分類
            for value in sensor_buffer:
                for index, seed_name in SEED_CONFIG.items():
                    batched_data[seed_name].append(float(value[index]))

            # 發送資料
            all_success = True
            for seed_name, data_points in batched_data.items():
                if data_points:
                    # 傳送起始時間與資料列表
                    if not self._post_data(start_timestamp, seed_name, data_points):
                        all_success = False

            # 注意：這裡是一個簡單的重試邏輯。如果失敗，資料會留在 sensor_buffer 中
            # 下次迴圈會再次嘗試發送 (連同新資料一起)
            # 若要更嚴謹，可能需要將失敗的資料獨立暫存，避免 buffer 無限膨脹
            if all_success:
                sensor_buffer.clear()
            else:
                print("[HTTP Poster] Warning: Upload failed, keeping data in buffer.")
                time.sleep(1) # 失敗時稍微暫停，避免瘋狂重試

        while self.active_event.is_set() or not self.data_queue['http'].empty():
            current_time = time.time()
            items_fetched = 0
            try:
                # 批量從隊列中取出數據，直到達到批次大小或隊列為空
                while items_fetched < MAX_BATCH_SIZE:
                    # 使用 get_nowait() 避免阻塞
                    data_type, data_list = self.data_queue['http'].get_nowait()

                    # data_list 預期是一個包含多個數據點的列表
                    if data_type == 'sensor':
                        sensor_buffer.extend(data_list)

                    self.data_queue['http'].task_done()
                    items_fetched += len(data_list) # 根據實際取出的項目數增加
            except queue.Empty:
                # 隊列為空，正常現象，繼續執行下面的寫入邏輯
                pass

            # 檢查是否滿足任一寫入條件
            force_write = (current_time - last_write_time >= WRITE_INTERVAL) and \
                          (sensor_buffer)

            buffer_full = (len(sensor_buffer) >= MAX_BATCH_SIZE)

            if force_write or buffer_full:
                flush_buffers()
                last_write_time = current_time
            else:
                # 如果隊列為空且不需要寫入，短暫休眠以防止CPU空轉
                time.sleep(0.01)

        print("[HTTP Poster] Thread stopped")

    def _post_data(self, timestamp, seed_name, data):
        """
        回傳 True 代表成功，False 代表失敗
        """
        payload = {
            "Station": self.station,
            "SampleRate": self.sample_rate,
            "Timestamp": timestamp, # 這裡是起始時間
            "SeedName": seed_name,
            "Data": data,
        }
        try:
            response = self.session.post(self.post_url, json=payload, timeout=self.timeout)
            if response.status_code >= 400:
                print(f"[HTTP Poster] Error: Server returned {response.status_code}")
                return False
            return True
        except requests.RequestException as e:
            print(f"[HTTP Poster] Network Error: {e}")
            return False

    def start_http_thread(self):
        """啟動HTTP資料發送線程"""
        http_thread = Thread(target=self.http_poster_thread, daemon=True)
        http_thread.start()
        return http_thread
