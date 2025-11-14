
import os
import argparse
import mysql.connector
from datetime import datetime, timezone, timedelta
from obspy import Trace, Stream
from obspy.core import UTCDateTime
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from module.mysql_connector import mysql_connector

import sys

# 中文字體設定
import matplotlib
if sys.platform.startswith('win'):
    # Windows 中文字體設定
    matplotlib.rcParams['font.sans-serif'] = ['Microsoft JhengHei',
                                              'Microsoft YaHei', 'SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False
elif sys.platform == 'darwin':
    # macOS 中文字體設定
    matplotlib.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS',
                                              'Hiragino Sans GB', 'STHeiti']
    matplotlib.rcParams['axes.unicode_minus'] = False

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

def fetch_data(conn, start_time_ms, end_time_ms, data_type='raw'):
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
        start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        print(f"執行查詢: {table_name} from {start_time_str} to {end_time_str} (UTC+8)")
        cursor.execute(query, (start_time_ms, end_time_ms))
        results = cursor.fetchall()
        cursor.close()
        print(f"✓ 查詢到 {len(results)} 筆資料")
        return results
    except mysql.connector.Error as err:
        print(f"✗ 查詢錯誤: {err}")
        return None

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

    output_file = f'seismic_data_{station_name}.mseed'

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


def fetch_intensity_data(mysql: mysql_connector, start_time_ms=None, end_time_ms=None, tz_utc_8=timezone(timedelta(hours=8))):
    """從資料庫獲取並顯示 intensity_data 表的內容"""

    params = []

    try:
        # 查詢指定時間範圍內的資料
        query = """
            SELECT * FROM intensity_data
            WHERE timestamp_ms >= %s AND timestamp_ms <= %s
            ORDER BY timestamp_ms ASC;
        """
        params = (start_time_ms, end_time_ms)
        start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n執行查詢: intensity_data from {start_time_str} to {end_time_str} (UTC+8)")
        # 使用 dictionary=True 可以讓結果以字典形式呈現，方便閱讀
        return mysql.execute_query(query, params, dictionary=True)

        if not results:
            print("在指定條件下，`intensity_data` 表中沒有資料。")
            return

        # --- 新增統計分析 (僅針對震度 >= 0.6) ---
        filtered_results = [row for row in results if row['intensity'] >= 0.6]

        if not filtered_results:
            print("在指定時間範圍內，沒有計測震度 >= 0.6 的資料。")
            return

        intensity_counts = {}
        max_intensity = -1
        max_intensity_time = None
        max_pga = -1
        max_pga_time = None

        for row in filtered_results:
            intensity = row['intensity']
            pga = row['a']
            timestamp_ms = row['timestamp_ms']

            # 統計各震度持續時間 (每筆資料代表 1 秒)
            level = round(intensity, 1)
            intensity_counts[level] = intensity_counts.get(level, 0) + 1

            # 找到最大計測震度
            if intensity > max_intensity:
                max_intensity = intensity
                max_intensity_time = timestamp_ms

            # 找到最大PGA
            if pga > max_pga:
                max_pga = pga
                max_pga_time = timestamp_ms

        print("\n--- 地震事件統計 (計測震度 >= 0.6) ---")
        if intensity_counts:
            print("各計測震度持續時間:")
            for level, count in sorted(intensity_counts.items()):
                print(f"  - 震度 {level} 級: {count} 秒")

        if max_intensity_time:
            max_intensity_dt = datetime.fromtimestamp(max_intensity_time / 1000.0, tz=timezone(timedelta(hours=8)))
            print(f"最大計測震度: {max_intensity:.1f} 級 (發生於 {max_intensity_dt.strftime('%Y-%m-%d %H:%M:%S')})")

        if max_pga_time:
            max_pga_dt = datetime.fromtimestamp(max_pga_time / 1000.0, tz=timezone(timedelta(hours=8)))
            print(f"最大PGA: {max_pga:.4f} gal (發生於 {max_pga_dt.strftime('%Y-%m-%d %H:%M:%S')})")
        print("------------------------------------\n")
        # --- 統計分析結束 ---

        # print(f"--- 地震儀強度資料 (計測震度 >= 0.6，共 {len(filtered_results)} 筆) ---")
        # for row in filtered_results:
        #     # 將 timestamp_ms 轉換為 UTC+8 時間
        #     ts_ms = row['timestamp_ms']
        #     ts_s = ts_ms / 1000.0
        #     utc8_time = datetime.fromtimestamp(ts_s, tz=timezone(timedelta(hours=8)))
        #     formatted_time = utc8_time.strftime('%Y-%m-%d %H:%M:%S')
        #     # 格式化輸出，使其更易讀
        #     print(
        #         # f"ID: {row['id']}, "
        #         f"觸發時間: {formatted_time}, "
        #         f"PGA: {row['a']:.4f}, "
        #         f"計測震度: {row['intensity']:.1f}"
        #     )
        # print("-----------------------------------------------------------")

    except mysql.mysql_connector.Error as err:
        print(f"✗ 查詢 `intensity_data` 時發生錯誤: {err}")


def intensity_analyze_print(results):
    if not results:
        print("在指定條件下，`intensity_data` 表中沒有資料。")
        return None

    # --- 新增統計分析 (使用 intensity 平均值當作過濾值) ---
    filter_intensity = sum(row['intensity'] for row in results) / len(results)
    filtered_results = [row for row in results if row['intensity'] > filter_intensity]


    if not filtered_results:
        print(f"在指定時間範圍內，沒有計測震度 > {filter_intensity:.1f} 的資料。")
        return None

    intensity_counts = {}
    max_intensity = -1
    max_intensity_time = None
    max_pga = -1
    max_pga_time = None

    for row in filtered_results:
        intensity = row['intensity']
        pga = row['a']
        timestamp_ms = row['timestamp_ms']

        # 統計各震度持續時間 (每筆資料代表 1 秒)
        level = round(intensity, 1)
        intensity_counts[level] = intensity_counts.get(level, 0) + 1

        # 找到最大計測震度
        if intensity > max_intensity:
            max_intensity = intensity
            max_intensity_time = timestamp_ms

        # 找到最大PGA
        if pga > max_pga:
            max_pga = pga
            max_pga_time = timestamp_ms

    print(f"\n--- 地震事件統計 (計測震度 > {filter_intensity:.1f}) ---")
    if intensity_counts:
        print("各計測震度持續時間:")
        for level, count in sorted(intensity_counts.items()):
            print(f"  - 震度 {level} 級: {count} 秒")

    if max_intensity_time:
        max_intensity_dt = datetime.fromtimestamp(max_intensity_time / 1000.0, tz=timezone(timedelta(hours=8)))
        print(f"最大計測震度: {max_intensity:.1f} 級 (發生於 {max_intensity_dt.strftime('%Y-%m-%d %H:%M:%S')})")

    if max_pga_time:
        max_pga_dt = datetime.fromtimestamp(max_pga_time / 1000.0, tz=timezone(timedelta(hours=8)))
        print(f"最大PGA: {max_pga:.4f} gal (發生於 {max_pga_dt.strftime('%Y-%m-%d %H:%M:%S')})")
    print("------------------------------------\n")
    return intensity_counts


def plot_charts(results, intensity_counts, start_time_ms, end_time_ms, tz_utc_8):
    """使用 Matplotlib 將震度資料和持續時間繪製成圖表"""
    if not results and not intensity_counts:
        print("沒有資料可供繪圖。")
        return

    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), gridspec_kw={'height_ratios': [2, 1]})

    # --- Plot 1: Intensity over time ---
    plot1_has_data = False
    if results:
        filter_intensity = sum(row['intensity'] for row in results) / len(results)
        filtered_results = [row for row in results if row['intensity'] > filter_intensity]
        if filtered_results:
            plot1_has_data = True
            timestamps = [row['timestamp_ms'] for row in filtered_results]
            intensities = [row['intensity'] for row in filtered_results]
            dates = [datetime.fromtimestamp(ts / 1000.0, tz=tz_utc_8) for ts in timestamps]

            ax1.plot(dates, intensities, '#ffd93d', markerfacecolor='#ffd93d', marker='o', linestyle='-', markersize=4, label='計測震度')

            start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
            ax1.set_title(f'Earthquake Intensity from {start_time_str} to {end_time_str} (UTC+8)')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('JMA Seismic Intensity Scale')
            ax1.grid(True, which='both', linestyle='--', linewidth=0.5)
            ax1.legend()
            fig.autofmt_xdate()

    if not plot1_has_data:
        ax1.set_title('Earthquake Intensity (No data to display)')
        ax1.text(0.5, 0.5, 'No data available for this time range.', horizontalalignment='center', verticalalignment='center', transform=ax1.transAxes, color='gray')


    # --- Plot 2: Intensity duration ---
    if intensity_counts:
        levels = [str(level) for level in sorted(intensity_counts.keys())]
        durations = [intensity_counts[float(level)] for level in levels]

        bars = ax2.bar(levels, durations, color='#ff6b6b')

        ax2.set_title('各計測震度持續時間')
        ax2.set_xlabel('計測震度 (級)')
        ax2.set_ylabel('持續時間 (秒)')
        ax2.grid(axis='y', linestyle='--', alpha=0.7)

        for bar in bars:
            yval = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.5, f'{yval}s', ha='center', va='bottom', color='white')
    else:
        ax2.set_title('各計測震度持續時間 (No data)')
        ax2.text(0.5, 0.5, 'No intensity duration data to display.', horizontalalignment='center', verticalalignment='center', transform=ax2.transAxes, color='gray')


    plt.tight_layout(pad=3.0)
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='從 MySQL 資料庫查詢地震資料並匯出為 SAC 或 miniSEED 檔案。')
    parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='開始時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-t', '--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')

    args = parser.parse_args()

    tz_utc_8 = timezone(timedelta(hours=8))

    # o_time 自動定出前後5 分鐘為開始與結束時間，優先級比 start_time 和 end_time 高
    if args.o_time:
        try:
            o_time_dt_naive = datetime.strptime(args.o_time, '%Y-%m-%dT%H:%M:%S')
            o_time_dt_aware = o_time_dt_naive.replace(tzinfo=tz_utc_8)
            start_dt_aware = o_time_dt_aware - timedelta(minutes=5)
            end_dt_aware = o_time_dt_aware + timedelta(minutes=5)
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

        results = fetch_intensity_data(mysql, start_time_ms, end_time_ms, tz_utc_8)
        mysql.disconnect()
        intensity_counts = intensity_analyze_print(results)
        plot_charts(results, intensity_counts, start_time_ms, end_time_ms, tz_utc_8)

    except Exception as e:
        print(f"✗ 處理資料時發生未預期錯誤: {e}")
    except KeyboardInterrupt:
        print("\n程式被用戶中斷。正在退出…")

if __name__ == '__main__':
    main()
