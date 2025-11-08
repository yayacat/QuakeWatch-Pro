import time
import requests
import queue
from threading import Thread, Event
from queue import Queue, Empty

class DataHttpPoster:
    """
    Thread to collect data from a queue and post it to an HTTP server.
    """
    def __init__(self, data_queue: Queue, active_event: Event, post_url: str, post_interval: float = 1.0, sample_rate: int = 50, station: str = "ESPRO"):
        super().__init__()
        self.data_queue = data_queue
        self.active_event = active_event
        self.post_url = post_url
        self.post_interval = post_interval
        self.sample_rate = sample_rate
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
            """寫入所有緩衝區的數據"""
            batched_data = {
                'SHN': {'data': [], 'timestamp': None},
                'SHE': {'data': [], 'timestamp': None},
                'SHZ': {'data': [], 'timestamp': None}
            }

            SEED_CONFIG = {2: 'SHN', 3: 'SHE', 4: 'SHZ'}

            for value in sensor_buffer:
                timestamp = value[1]
                for index, seed_name in SEED_CONFIG.items():
                    component_value = value[index]
                    batched_data[seed_name]['data'].append(float(component_value))
                    batched_data[seed_name]['timestamp'] = timestamp

            # Post the batched data for each channel.
            total_posted = 0
            for seed_name, batch in batched_data.items():
                if batch['data']:
                    # The _post_data method resets the buffer, so we populate it for each batch.
                    self._post_data(batch['timestamp'], seed_name, batch['data'])
                    # print(f"[HTTP Poster] Posted {len(batch['data'])} sensor data points for channel {seed_name}")
                    total_posted += len(batch['data'])

            # if total_posted > 0:
                # print(f"[HTTP Poster] Totally posted {total_posted} sensor data points")
            sensor_buffer.clear()

            # try:
            #     # current_time = time.time()
            #     # if (current_time - self._last_post_time >= WRITE_INTERVAL) and self._buffer['Timestamp'] and self._buffer['SeedName'] and self._buffer['Data']:
            #     #     self._post_data()
            # except Empty:
            #     # This is expected when the queue is empty.
            #     # We'll proceed to check if it's time to post any buffered data.
            #     pass

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
        Posts the buffered data to the server.
        """
        payload = {
            "Station": self.station,
            "SampleRate": self.sample_rate,
            "Timestamp": timestamp,
            "SeedName": seed_name,
            "Data": data,
        }
        try:
            response = requests.post(self.post_url, json=payload, timeout=2.0)
            if response.status_code >= 400:
                print(f"[HTTP Poster] Error: Server returned status {response.status_code} - {response.text}")
        except requests.RequestException as e:
            print(f"[HTTP Poster] Error sending data: {e}")

        # Reset buffer for the next batch
        # self._buffer = {
        #     'Timestamp': None,
        #     'SeedName': '',
        #     'Data': [],
        # }
        # self._last_post_time = time.time()

    def start_http_thread(self):
        """啟動HTTP資料發送線程"""
        http_thread = Thread(target=self.http_poster_thread, daemon=True)
        http_thread.start()
        return http_thread
