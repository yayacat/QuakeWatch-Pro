"""
QuakeWatch - 匯出至 miniSEED 格式
將 earthquake_data_202511101300.db 轉換成標準 miniSEED 地震資料格式
"""

import sqlite3
import numpy as np
from datetime import datetime, timezone

# ObsPy 匯入（用於 miniSEED 格式）
from obspy import Trace, Stream, UTCDateTime

# 資料庫檔案
DB_FILE = 'earthquake_data_202511101300.db'

# ========== 時間範圍設定 ==========
START_TIME = 60        # 開始時間（秒，相對於第一筆資料）
DURATION = 80          # 持續時間（秒），None = 匯出所有資料
# =================================

# 輸出檔案前綴
OUTPUT_PREFIX = f'swarm_{START_TIME}_{START_TIME+DURATION}s'


def load_sensor_data():
    """從資料庫載入感測器資料"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT timestamp_ms, x, y, z, received_time
        FROM sensor_data
        ORDER BY timestamp_ms ASC
    ''')
    sensor_rows = cursor.fetchall()
    conn.close()

    return sensor_rows


def filter_data_by_time(sensor_rows):
    """根據時間範圍過濾資料"""
    if not sensor_rows:
        return []

    timestamps = np.array([row[0] for row in sensor_rows])
    first_timestamp = timestamps[0]
    time_data = (timestamps - first_timestamp) / 1000.0

    # 過濾資料
    if DURATION is not None:
        end_time = START_TIME + DURATION
        mask = (time_data >= START_TIME) & (time_data <= end_time)
    else:
        mask = time_data >= START_TIME

    filtered_rows = [sensor_rows[i]
                     for i in range(len(sensor_rows)) if mask[i]]
    return filtered_rows


def export_to_miniseed(sensor_rows, output_file='seismic_data.mseed'):
    """
    匯出為 miniSEED 格式（三軸合併成一個檔案）
    使用 ObsPy 函式庫
    """
    if not sensor_rows:
        print("沒有資料可匯出！")
        return

    # 準備資料
    timestamps = np.array([row[0] for row in sensor_rows])
    x_data = np.array([row[1] for row in sensor_rows])
    y_data = np.array([row[2] for row in sensor_rows])
    z_data = np.array([row[3] for row in sensor_rows])

    # 數值放大 10000 倍並轉換成整數 (Counts)
    x_data = np.round(x_data * 10000).astype(np.int32)
    y_data = np.round(y_data * 10000).astype(np.int32)
    z_data = np.round(z_data * 10000).astype(np.int32)

    # 計算採樣率（假設均勻採樣）
    if len(timestamps) > 1:
        dt = (timestamps[-1] - timestamps[0]) / (len(timestamps) - 1)
        sampling_rate = 1000.0 / dt  # ms 轉 Hz
    else:
        sampling_rate = 50.0  # 預設 50 Hz

    # 開始時間（Unix timestamp 轉 UTCDateTime）
    starttime = UTCDateTime(timestamps[0] / 1000.0)

    # 建立 Stream（包含三個 Trace）
    stream = Stream()

    # 設定站點資訊
    network = 'ES'      # ES-Net
    station = 'KHH01'   # QuakeWatch-Pro
    location = 'TW'

    # 三個分量
    components = [
        ('EHZ', x_data),  # E-W (East-West) - X 軸
        ('EHN', y_data),  # N-S (North-South) - Y 軸
        ('EHE', z_data)   # Vertical - Z 軸
    ]

    for channel, data in components:
        stats = {
            'network': network,
            'station': station,
            'location': location,
            'channel': channel,
            'npts': len(data),
            'sampling_rate': sampling_rate,
            'starttime': starttime,
            'mseed': {'dataquality': 'D'}  # D = Data of undefined quality
        }

        trace = Trace(data=data, header=stats)
        stream.append(trace)

    # 寫入 miniSEED 檔案
    # encoding=11: Steim-2 compression (適合地震資料)
    # encoding=10: Steim-1 compression
    # encoding=3: 32-bit integer
    stream.write(output_file, format='MSEED', encoding=11, reclen=512)

    print(f"✓ miniSEED 已匯出至: {output_file}")
    print(f"  包含 3 個 Trace (EHE, EHN, EHZ)")
    print(f"  採樣率: {sampling_rate:.2f} Hz")
    print(f"  資料點數: {len(x_data)}")
    print(f"  開始時間: {starttime}")


def main():
    """主程式"""
    print("QuakeWatch - 匯出至 miniSEED 格式")
    print("="*60)
    print(f"資料庫: {DB_FILE}")
    print(f"時間範圍: {START_TIME} 秒 ~ {START_TIME + DURATION} 秒")
    print("="*60)

    # 載入資料
    print("\n正在載入資料...")
    sensor_rows = load_sensor_data()
    print(f"✓ 已載入 {len(sensor_rows)} 筆原始資料")

    # 過濾資料
    print(f"\n正在過濾資料 ({START_TIME}~{START_TIME + DURATION} 秒)...")
    filtered_rows = filter_data_by_time(sensor_rows)
    print(f"✓ 過濾後剩餘 {len(filtered_rows)} 筆資料")

    if len(filtered_rows) == 0:
        print("\n⚠ 警告: 過濾後沒有資料！")
        return

    # 匯出為 miniSEED 格式
    print(f"\n正在匯出為 miniSEED 格式...")
    mseed_filename = f'seismic_data_{START_TIME}_{START_TIME+DURATION}s.mseed'
    export_to_miniseed(filtered_rows, mseed_filename)

    print("\n" + "="*60)
    print("匯出完成！")
    print(f"已產生: {mseed_filename}")
    print("="*60)


if __name__ == '__main__':
    main()
