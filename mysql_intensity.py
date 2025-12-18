
import os
import argparse
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from module.mysql_connector import mysql_connector

import sys

import numpy as np

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




def format_intensity(intensity_val):
    """Converts a float intensity value to its JMA string representation."""
    if intensity_val < 0.5:
        return "0級"
    elif intensity_val < 1.5:
        return "1級"
    elif intensity_val < 2.5:
        return "2級"
    elif intensity_val < 3.5:
        return "3級"
    elif intensity_val < 4.5:
        return "4級"
    elif intensity_val < 5.0:
        return "5弱"
    elif intensity_val < 5.5:
        return "5強"
    elif intensity_val < 6.0:
        return "6弱"
    elif intensity_val < 6.5:
        return "6強"
    else: # >= 6.5
        return "7級"

def fetch_intensity_data(mysql: mysql_connector, station_id: str, start_time_ms=None, end_time_ms=None, tz_utc_8=timezone(timedelta(hours=8))):
    """從資料庫獲取並顯示 intensity_data 表的內容"""

    params = []
    query = ''

    try:
        if station_id == 'null':
            query = """
                SELECT * FROM intensity_data
                WHERE timestamp_ms >= %s AND timestamp_ms <= %s
                ORDER BY timestamp_ms ASC;
            """
            params = (start_time_ms, end_time_ms)
            station_id = "All Stations"
        else:
            # 查詢指定時間範圍內的資料
            query = """
                SELECT * FROM intensity_data
                WHERE station = %s AND timestamp_ms >= %s AND timestamp_ms <= %s
                ORDER BY timestamp_ms ASC;
            """
            params = (station_id, start_time_ms, end_time_ms)
        start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n執行查詢: intensity_data from {start_time_str} to {end_time_str} (UTC+8) 測站id: {station_id}")
        # 使用 dictionary=True 可以讓結果以字典形式呈現，方便閱讀
        results = mysql.execute_query(query, params, dictionary=True)

        if not results:
            print("在指定條件下，`intensity_data` 表中沒有資料。")
            return None

        return results

    except mysql.mysql_connector.Error as err:
        print(f"✗ 查詢 `intensity_data` 時發生錯誤: {err}")
        return None


def intensity_analyze_print(results):
    if not results:
        print("在指定條件下，`intensity_data` 表中沒有資料。")
        return None

    # --- Group data by station ---
    station_data = {}
    for row in results:
        s_id = row.get('station', 'N/A')
        if s_id not in station_data:
            station_data[s_id] = []
        station_data[s_id].append(row)

    is_multi_station = len(station_data) > 1

    # --- Overall Statistics ---
    station_intensity_counts = {}
    max_intensity = -1
    max_intensity_time = None
    max_pga = -1
    max_pga_time = None
    max_intensity_station = None
    max_pga_station = None

    avg_filter_intensity = sum(row['intensity'] for row in results) / len(results)
    print(f"\n--- 地震事件統計 (計測震度 > {avg_filter_intensity:.1f}) ---")

    if is_multi_station:
        print("各測站的震度持續時間:")
    else:
        print("各震度持續時間:")

    jma_order = ["0級", "1級", "2級", "3級", "4級", "5弱", "5強", "6弱", "6強", "7級"]

    # --- Per-Station Analysis ---
    for station, station_rows in station_data.items():
        filter_intensity = sum(row['intensity'] for row in station_rows) / len(station_rows)
        filtered_results = [row for row in station_rows if row['intensity'] > filter_intensity]

        if not filtered_results:
            continue

        intensity_counts = {}
        for row in filtered_results:
            intensity = row['intensity']
            pga = row['a']
            timestamp_ms = row['timestamp_ms']

            level = format_intensity(intensity)
            intensity_counts[level] = intensity_counts.get(level, 0) + 1

            if intensity > max_intensity:
                max_intensity = intensity
                max_intensity_time = timestamp_ms
                max_intensity_station = station
            if pga > max_pga:
                max_pga = pga
                max_pga_time = timestamp_ms
                max_pga_station = station

        if intensity_counts:
            station_intensity_counts[station] = intensity_counts
            if is_multi_station:
                print(f"  測站 {station}:")

            sorted_levels = sorted(intensity_counts.keys(), key=lambda x: jma_order.index(x) if x in jma_order else len(jma_order))
            for level in sorted_levels:
                count = intensity_counts[level]
                indent = "    - " if is_multi_station else "  - "
                print(f"{indent}震度 {level}: {count} 秒")

    if not station_intensity_counts:
        print(f"在指定時間範圍內，沒有計測震度 > {avg_filter_intensity:.1f} 的資料。")
        return None

    # --- Print overall max values ---
    if max_intensity_time:
        max_intensity_dt = datetime.fromtimestamp(max_intensity_time / 1000.0, tz=timezone(timedelta(hours=8)))
        max_intensity_str = format_intensity(max_intensity)
        print(f"最大震度: {max_intensity_str} (最大計測震度: {max_intensity:.1f}) (發生於 {max_intensity_dt.strftime('%Y-%m-%d %H:%M:%S')}) (測站: {max_intensity_station})")

    if max_pga_time:
        max_pga_dt = datetime.fromtimestamp(max_pga_time / 1000.0, tz=timezone(timedelta(hours=8)))
        print(f"最大PGA: {max_pga:.4f} gal (發生於 {max_pga_dt.strftime('%Y-%m-%d %H:%M:%S')}) (測站: {max_pga_station})")

    print("------------------------------------\n")
    return station_intensity_counts


def plot_charts(results, plot_station_id, station_intensity_counts, start_time_ms, end_time_ms, tz_utc_8):
    """使用 Matplotlib 將震度資料和持續時間繪製成圖表"""
    if not results and not station_intensity_counts:
        print("沒有資料可供繪圖。")
        return

    plt.style.use('dark_background')
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12), num='地震震度分析圖表', gridspec_kw={'height_ratios': [2, 1]})

    # --- Group data by station and create a consistent color map ---
    station_data = {}
    if results:
        for row in results:
            s_id = row.get('station', 'N/A')
            if s_id not in station_data:
                station_data[s_id] = []
            station_data[s_id].append(row)

    # Get all unique station IDs from both data sources
    all_station_ids = set(station_data.keys())
    if station_intensity_counts:
        all_station_ids.update(station_intensity_counts.keys())

    sorted_station_ids = sorted(list(all_station_ids))
    is_multi_station = len(sorted_station_ids) > 1

    colors = plt.colormaps.get_cmap('tab10')
    station_colors = {s_id: colors(i % 10) for i, s_id in enumerate(sorted_station_ids)}


    # --- Plot 1: Intensity over time ---
    plot1_has_data = False
    if station_data:
        for s_id in sorted_station_ids:
            station_rows = station_data.get(s_id)
            if not station_rows:
                continue

            # Filter data to make plot cleaner, but ensure we plot something
            avg_intensity = sum(row['intensity'] for row in station_rows) / len(station_rows)
            filtered_results = [row for row in station_rows if row['intensity'] > avg_intensity]
            if not filtered_results: # If filtering removes everything, show all data for this station
                filtered_results = station_rows

            if filtered_results:
                plot1_has_data = True
                timestamps = [row['timestamp_ms'] for row in filtered_results]
                intensities = [row['intensity'] for row in filtered_results]
                dates = [datetime.fromtimestamp(ts / 1000.0, tz=tz_utc_8) for ts in timestamps]

                label = f'測站 {s_id}' if is_multi_station else '計測震度'
                ax1.plot(dates, intensities, color=station_colors.get(s_id), marker='o', linestyle='-', markersize=4, label=label)

    if plot1_has_data:
        start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')

        title_station_part = '各測站' if is_multi_station else f'測站 {plot_station_id}'
        ax1.set_title(f'{title_station_part} 計測震度圖 (時間: {start_time_str} 到 {end_time_str} UTC+8)')
        ax1.set_xlabel('時間')
        ax1.set_ylabel('計測震度')
        ax1.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax1.legend()
        fig.autofmt_xdate()
    else:
        title_station_part = '各測站' if is_multi_station else f'測站 {plot_station_id}'
        ax1.set_title(f'{title_station_part} 計測震度圖 (無資料)')
        ax1.text(0.5, 0.5, '指定時間範圍內沒有可顯示的資料。', horizontalalignment='center', verticalalignment='center', transform=ax1.transAxes, color='gray')

    # --- Plot 2: Intensity duration ---
    if station_intensity_counts:
        jma_order = ["0級", "1級", "2級", "3級", "4級", "5弱", "5強", "6弱", "6強", "7級"]

        # Collect all levels and stations
        all_levels = set()
        for counts in station_intensity_counts.values():
            all_levels.update(counts.keys())

        sorted_levels = sorted(list(all_levels), key=lambda x: jma_order.index(x) if x in jma_order else len(jma_order))

        num_stations = len(sorted_station_ids)
        x = np.arange(len(sorted_levels))
        total_width = 0.8
        bar_width = total_width / num_stations

        for i, station_id in enumerate(sorted_station_ids):
            counts = station_intensity_counts.get(station_id, {})
            durations = [counts.get(level, 0) for level in sorted_levels]

            # Calculate position for each station's bar
            position = x - (total_width / 2) + (i * bar_width) + (bar_width / 2)

            bars = ax2.bar(position, durations, bar_width, label=f'測站 {station_id}', color=station_colors.get(station_id))
            for bar in bars:
                yval = bar.get_height()
                if yval > 0:
                    ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.1, f'{yval}', ha='center', va='bottom', color='white', fontsize=9)

        title_station_part = '各測站' if is_multi_station else f'測站 {plot_station_id}'
        ax2.set_title(f'{title_station_part} 各震度級別持續時間')
        ax2.set_xlabel('震度級別')
        ax2.set_ylabel('持續時間 (秒)')
        ax2.set_xticks(x)
        ax2.set_xticklabels(sorted_levels)
        ax2.grid(axis='y', linestyle='--', alpha=0.7)
        if is_multi_station:
            ax2.legend()

    else:
        title_station_part = '所有測站' if is_multi_station else f'測站 {plot_station_id}'
        ax2.set_title(f'{title_station_part} 各震度級別持續時間 (無資料)')
        ax2.text(0.5, 0.5, '沒有震度持續時間資料可顯示。', horizontalalignment='center', verticalalignment='center', transform=ax2.transAxes, color='gray')

    plt.tight_layout(pad=3.0)
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='從 MySQL 資料庫查詢地震資料並分析震度。')
    # parser.add_argument('o_time', nargs='?', default=None, help='發震時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('start_time', nargs='?', default=None, help='發震(開始)時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('end_time', nargs='?', default=None, help='結束時間 (UTC+8, 格式: YYYY-MM-DDTHH:MM:SS)')
    parser.add_argument('-t', '--time', type=int, default=5, help='時間區間長度（分鐘），預設為 5 分鐘')
    parser.add_argument('-s', '--station', type=str, default=os.getenv("station", "ESPRO"), help=f'指定單一測站 ID (預設: {os.getenv("station", "ESPRO")})')
    parser.add_argument('--all-stations', action='store_true', help='處理所有可用的測站')

    args = parser.parse_args()

    tz_utc_8 = timezone(timedelta(hours=8))

    # start_time 自動定出前後5 分鐘為開始與結束時間，優先級比 start_time 和 end_time 高
    if args.start_time and args.end_time is None:
        try:
            start_time_dt_naive = datetime.strptime(args.start_time, '%Y-%m-%dT%H:%M:%S')
            start_time_dt_aware = start_time_dt_naive.replace(tzinfo=tz_utc_8)
            start_dt_aware = start_time_dt_aware - timedelta(minutes=args.time)
            end_dt_aware = start_time_dt_aware + timedelta(minutes=args.time)
            print(f"已提供 start_time，自動設定時間範圍為: {start_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')} to {end_dt_aware.strftime('%Y-%m-%dT%H:%M:%S')}")
        except ValueError:
            parser.error("錯誤的 start_time 格式。請使用 YYYY-MM-DDTHH:MM:SS。")
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
        return

    # 統一處理要查詢的測站
    if args.all_stations:
        print("正在查詢所有可用測站的資料...")
        station_id = 'null'
        plot_station_id = "All Stations"
    else:
        print(f"正在查詢測站 {args.station} 的資料...")
        station_id = args.station
        plot_station_id = args.station


    try:
        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

        # 統一的獲取和繪圖邏輯
        results = fetch_intensity_data(mysql, station_id, start_time_ms, end_time_ms, tz_utc_8)

        station_intensity_counts = intensity_analyze_print(results)

        plot_charts(results, plot_station_id, station_intensity_counts, start_time_ms, end_time_ms, tz_utc_8)

    except Exception as e:
        print(f"✗ 處理資料時發生未預期錯誤: {e}")
    except KeyboardInterrupt:
        print("\n程式被用戶中斷。正在退出…")
    finally:
        mysql.disconnect()

if __name__ == '__main__':
    main()
