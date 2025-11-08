
import os
import argparse
import mysql.connector
from datetime import datetime, timezone, timedelta
from obspy import Trace, Stream
from obspy.core import UTCDateTime
import numpy as np
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

def get_db_connection():
    """建立並返回資料庫連接"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print(f"✓ 已連接到 MySQL 資料庫: {DB_CONFIG['database']}")
        return conn
    except mysql.connector.Error as err:
        print(f"✗ 資料庫連接錯誤: {err}")
        return None

def fetch_data(conn, start_time_ms, end_time_ms, data_type='filtered'):
    """從資料庫獲取指定時間範圍內的資料"""
    if not conn:
        return None

    if data_type == 'raw':
        table_name = 'sensor_data'
        columns = 'x, y, z'
    else: # 預設為 'filtered'
        table_name = 'filtered_data'
        columns = 'h1, h2, v'

    query = f"""
        SELECT timestamp_ms, {columns}
        FROM {table_name}
        WHERE timestamp_ms >= %s AND timestamp_ms <= %s
        ORDER BY timestamp_ms ASC;
    """
    try:
        cursor = conn.cursor()
        print(f"執行查詢: {table_name} from {start_time_ms} to {end_time_ms}")
        cursor.execute(query, (start_time_ms, end_time_ms))
        results = cursor.fetchall()
        cursor.close()
        print(f"✓ 查詢到 {len(results)} 筆資料")
        return results
    except mysql.connector.Error as err:
        print(f"✗ 查詢錯誤: {err}")
        return None

def create_sac_file(data, output_file, station_name='QWPRO'):
    """根據查詢到的資料建立 SAC 檔案"""
    if not data:
        print("沒有資料可供寫入 SAC 檔案")
        return

    timestamps = np.array([row[0] for row in data])
    x_data = np.array([row[1] for row in data], dtype=np.float32)
    y_data = np.array([row[2] for row in data], dtype=np.float32)
    z_data = np.array([row[3] for row in data], dtype=np.float32)

    if len(timestamps) < 2:
        print("資料點不足，無法計算採樣率")
        return

    # 計算採樣率 (Hz)
    sampling_rate = 1000.0 / np.mean(np.diff(timestamps))
    if np.isnan(sampling_rate) or sampling_rate == 0:
        print(f"無法計算有效的採樣率。採樣率為: {sampling_rate}")
        return

    print(f"計算出的採樣率: {sampling_rate:.2f} Hz")


    # 獲取開始時間
    start_timestamp_s = timestamps[0] / 1000.0
    starttime = UTCDateTime(start_timestamp_s)

    # 建立 Traces
    trace_x = Trace(data=x_data)
    trace_x.stats.sampling_rate = sampling_rate
    trace_x.stats.starttime = starttime
    trace_x.stats.station = station_name
    trace_x.stats.channel = 'BHE' # X -> East

    trace_y = Trace(data=y_data)
    trace_y.stats.sampling_rate = sampling_rate
    trace_y.stats.starttime = starttime
    trace_y.stats.station = station_name
    trace_y.stats.channel = 'BHN' # Y -> North

    trace_z = Trace(data=z_data)
    trace_z.stats.sampling_rate = sampling_rate
    trace_z.stats.starttime = starttime
    trace_z.stats.station = station_name
    trace_z.stats.channel = 'BHZ' # Z -> Vertical

    # 分別將每個分量寫入獨立的 SAC 檔案
    try:
        base, ext = os.path.splitext(output_file)
        # 如果使用者未提供 .sac 副檔名，或副檔名不正確，則將整個名稱視為 base
        if ext.lower() != '.sac':
            base = output_file
            ext = '.sac'

        # 寫入 X (E) 分量
        file_x = f"{base}.{trace_x.stats.channel}{ext}"
        trace_x.write(file_x, format='SAC')
        print(f"✓ X-component (E) SAC 檔案已儲存至: {file_x}")

        # 寫入 Y (N) 分量
        file_y = f"{base}.{trace_y.stats.channel}{ext}"
        trace_y.write(file_y, format='SAC')
        print(f"✓ Y-component (N) SAC 檔案已儲存至: {file_y}")

        # 寫入 Z (V) 分量
        file_z = f"{base}.{trace_z.stats.channel}{ext}"
        trace_z.write(file_z, format='SAC')
        print(f"✓ Z-component (V) SAC 檔案已儲存至: {file_z}")

    except Exception as e:
        print(f"✗ 儲存 SAC 檔案時發生錯誤: {e}")


def main():
    parser = argparse.ArgumentParser(description='從 MySQL 資料庫查詢地震資料並匯出為 SAC 檔案。')
    parser.add_argument('start_time', type=str, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', type=str, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-o', '--output', type=str, default='output.sac', help='輸出的 SAC 檔案名稱 (預設: output.sac)')
    parser.add_argument('-t', '--type', type=str, choices=['raw', 'filtered'], default='filtered', help='資料類型: raw (原始) 或 filtered (濾波) (預設: filtered)')
    parser.add_argument('-s', '--station', type=str, default='QWPRO', help='測站名稱 (預設: QWPRO)')

    args = parser.parse_args()

    # 解析時間字串
    try:
        # 定義 UTC+8 時區
        tz_utc_8 = timezone(timedelta(hours=8))

        # 將輸入的 naive datetime 字串解析
        start_dt_naive = datetime.strptime(args.start_time, '%Y-%m-%dT%H:%M:%S')
        end_dt_naive = datetime.strptime(args.end_time, '%Y-%m-%dT%H:%M:%S')

        # 附加 UTC+8 時區資訊
        start_dt_aware = start_dt_naive.replace(tzinfo=tz_utc_8)
        end_dt_aware = end_dt_naive.replace(tzinfo=tz_utc_8)

        # 將 aware datetime 轉換為 UTC 時間戳 (毫秒)
        # .timestamp() 會自動處理時區轉換
        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

    except ValueError:
        print("✗ 錯誤的時間格式。請使用 YYYY-MM-DDTHH:MM:SS。")
        return

    conn = get_db_connection()
    if conn:
        data = fetch_data(conn, start_time_ms, end_time_ms, args.type)
        conn.close()
        print("資料庫連接已關閉")

        if data:
            create_sac_file(data, args.output, args.station)

if __name__ == '__main__':
    main()
