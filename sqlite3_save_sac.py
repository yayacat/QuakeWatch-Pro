"""
QuakeWatch - 匯出至 miniSEED 格式
將 earthquake_data.db 轉換成標準 miniSEED 地震資料格式

用法:
    python sqlite3_save_mseed.py <start_time> <end_time> [-o <output_file>]

範例:
    python sqlite3_save_mseed.py "2025-12-01T14:00:00" "2025-12-01T14:05:00" -o custom_name.mseed
"""

import argparse
import sqlite3
import numpy as np
from datetime import datetime, timezone, timedelta

# ObsPy 匯入（用於 miniSEED 格式）
from obspy import Trace, Stream, UTCDateTime

# 資料庫檔案
DB_FILE = 'earthquake_data.db'


def parse_time_to_ms_utc(time_str):
    """
    將本地時間字串 (UTC+8) 轉換為 UTC 時間的毫秒時間戳
    """
    # 解析時間字串
    local_time = datetime.fromisoformat(time_str)
    # 設定時區為 UTC+8
    local_tz = timezone(timedelta(hours=8))
    local_time = local_time.replace(tzinfo=local_tz)
    # 轉換為 UTC 時間
    utc_time = local_time.astimezone(timezone.utc)
    # 回傳毫秒時間戳
    return int(utc_time.timestamp() * 1000)


def load_sensor_data(start_ms, end_ms):
    """從資料庫載入指定時間範圍的感測器資料"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT timestamp_ms, x, y, z, received_time
        FROM sensor_data
        WHERE timestamp_ms >= ? AND timestamp_ms <= ?
        ORDER BY timestamp_ms ASC
    ''', (start_ms, end_ms))
    sensor_rows = cursor.fetchall()
    conn.close()

    return sensor_rows


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
        # 計算時間差（毫秒）
        dt_ms = (timestamps[-1] - timestamps[0]) / (len(timestamps) - 1)
        sampling_rate = 1000.0 / dt_ms  # ms 轉 Hz
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
        ('EHE', x_data),  # E-W (East-West) - X 軸
        ('EHN', y_data),  # N-S (North-South) - Y 軸
        ('EHZ', z_data)   # Vertical - Z 軸
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
    # encoding=11: Steim-2 compression
    stream.write(output_file, format='MSEED', encoding=11, reclen=512)

    print(f"✓ miniSEED 已匯出至: {output_file}")
    print(f"  包含 3 個 Trace (EHE, EHN, EHZ)")
    print(f"  採樣率: {sampling_rate:.2f} Hz")
    print(f"  資料點數: {len(x_data)}")
    print(f"  開始時間 (UTC): {starttime.isoformat()}")


def main():
    """主程式"""
    parser = argparse.ArgumentParser(
        description='將一段時間區間的地震資料從一個 SQLite 資料庫輸出 miniSEED 格式。',
        epilog='時間格式為 YYYY-MM-DDTHH:MM:SS，並假設為 UTC+8 時區。'
    )
    parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-t', '--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')
    parser.add_argument('-o', '--output', default=None, help='輸出 miniSEED 檔案名稱。預設會自動生成。')

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

    print("QuakeWatch - 匯出至 miniSEED 格式")
    print("="*60)
    print(f"資料庫: {DB_FILE}")
    print(f"指定時間 (UTC+8): {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} -> {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
    print("="*60)

    try:
        start_ms = parse_time_to_ms_utc(start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S'))
        end_ms = parse_time_to_ms_utc(end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S'))
    except ValueError:
        print("\n錯誤: 時間格式不正確，請使用 YYYY-MM-DDTHH:MM:SS 格式。")
        return

    # 載入資料
    print(f"\n正在從資料庫載入資料...")
    sensor_rows = load_sensor_data(start_ms, end_ms)
    print(f"✓ 已載入 {len(sensor_rows)} 筆資料")

    if not sensor_rows:
        print("\n⚠ 警告: 在指定的時間範圍內找不到任何資料！")
        return

    # 決定輸出檔名
    if args.output:
        mseed_filename = args.output
    else:
        start_dt_utc = UTCDateTime(start_ms / 1000)
        mseed_filename = f'seismic_data_{start_dt_utc.strftime("%Y%m%dT%H%M%S")}.mseed'

    # 匯出為 miniSEED 格式
    print(f"\n正在匯出為 miniSEED 格式...")
    export_to_miniseed(sensor_rows, mseed_filename)

    print("\n" + "="*60)
    print("匯出完成！")
    print(f"已產生: {mseed_filename}")
    print("="*60)


if __name__ == '__main__':
    main()
