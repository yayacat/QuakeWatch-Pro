
import os
import argparse
from datetime import datetime, timezone, timedelta
from obspy import Trace, Stream
from obspy.core import UTCDateTime
import numpy as np
from dotenv import load_dotenv
from module.mysql_connector import mysql_connector


# --- MySQL 資料庫設定 (從 .env 檔案讀取) ---
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}
# ------------------------------------

def fetch_data(mysql: mysql_connector, start_time_ms, end_time_ms, data_type='raw', tz_utc_8=timezone(timedelta(hours=8))):
    """從資料庫獲取指定時間範圍內的資料"""

    if data_type == 'raw':
        data_type_name = '原始資料'
        table_name = 'sensor_data'
        columns = 'x, y, z'
    else: # 預設為 'filtered'
        data_type_name = '濾波後資料'
        table_name = 'filtered_data'
        columns = 'h1, h2, v'

    query = f"""
        SELECT timestamp_ms, {columns}
        FROM {table_name}
        WHERE timestamp_ms >= %s AND timestamp_ms <= %s
        ORDER BY timestamp_ms ASC;
    """
    start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
    print(f"執行查詢: {data_type_name} from {start_time_str} to {end_time_str} (UTC+8)")
    return mysql.execute_query(query, (start_time_ms, end_time_ms))

def export_to_miniseed(sensor_rows, output_file='seismic_data.mseed', station_name='KHH01'):
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
    # station = 'KHH01'   # QuakeWatch-Pro
    location = 'TW'

    # 三個分量
    components = [
        ('ELE', x_data),  # E-W (East-West) - X 軸
        ('ELN', y_data),  # N-S (North-South) - Y 軸
        ('ELZ', z_data)   # Vertical - Z 軸
    ]

    for channel, data in components:
        stats = {
            'network': network,
            'station': station_name,
            'location': location,
            'channel': channel,
            'npts': len(data),
            'sampling_rate': sampling_rate,
            'starttime': starttime,
            'mseed': {'dataquality': 'D'}  # D = Data of undefined quality
        }

        trace = Trace(data=data, header=stats)
        stream.append(trace)

    output_file = f'seismic_data_{station_name}_{timestamps[0]}.mseed'

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
    parser = argparse.ArgumentParser(description='從 MySQL 資料庫查詢地震資料並匯出為 SAC 或 miniSEED 檔案。')
    parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-o', '--output', default='seismic_data.mseed', help='輸出的檔案名稱')
    parser.add_argument('-t', '--type', choices=['raw', 'filtered'], default='raw', help='資料類型: raw (原始) 或 filtered (濾波)')
    parser.add_argument('-s', '--station', default='KHH01', help='測站名稱')
    parser.add_argument('--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')

    args = parser.parse_args()

    tz_utc_8 = timezone(timedelta(hours=8))

    # o_time 自動定出前後5 分鐘為開始與結束時間，優先級比 start_time 和 end_time 高
    if args.o_time:
        try:
            o_time_dt_naive = datetime.strptime(args.o_time, '%Y-%m-%dT%H:%M:%S')
            o_time_dt_aware = o_time_dt_naive.replace(tzinfo=tz_utc_8)
            start_dt_aware = o_time_dt_aware - timedelta(minutes=args.time)
            end_dt_aware = o_time_dt_aware + timedelta(minutes=args.time)
            print(f"已提供 o_time，自動設定時間範圍為: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
        except ValueError:
            parser.error("錯誤的 o_time 格式。請使用 YYYY-MM-DDTHH:MM:SS。")
    elif args.start_time is None or args.end_time is None:
        end_dt_aware = datetime.now(tz_utc_8)
        start_dt_aware = end_dt_aware - timedelta(minutes=args.time)
        print(f"未提供時間範圍，自動使用最近 {args.time} 分鐘: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
    else:
        try:
            start_dt_naive = datetime.strptime(args.start_time, '%Y-%m-%dT%H:%M:%S')
            end_dt_naive = datetime.strptime(args.end_time, '%Y-%m-%dT%H:%M:%S')
            start_dt_aware = start_dt_naive.replace(tzinfo=tz_utc_8)
            end_dt_aware = end_dt_naive.replace(tzinfo=tz_utc_8)
        except ValueError:
            parser.error("錯誤的時間格式。請使用 YYYY-MM-DDTHH:MM:SS。")


    mysql = mysql_connector(
        host=DB_CONFIG['host'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database']
    )

    if mysql.conn is None:
        return None

    try:
        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

        data = fetch_data(mysql, start_time_ms, end_time_ms, args.type, tz_utc_8)
        if data:
            print("\n準備匯出為 miniSEED 格式...")
            export_to_miniseed(data, args.output, args.station)

    except Exception as e:
        print(f"✗ 處理資料時發生未預期錯誤: {e}")
    finally:
        mysql.disconnect()

if __name__ == '__main__':
    main()
