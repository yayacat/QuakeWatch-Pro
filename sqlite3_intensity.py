
import os
import argparse
import sqlite3
from datetime import datetime, timezone, timedelta
from obspy import Trace, Stream
from obspy.core import UTCDateTime
import numpy as np
import matplotlib.pyplot as plt
import sys

# --- Database Configuration ---
DB_FILE = 'earthquake_data.db'
# -----------------------------

# 中文字體設定
import matplotlib
if sys.platform.startswith('win'):
    # Windows 中文字體設定
    matplotlib.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Microsoft YaHei', 'SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False
elif sys.platform == 'darwin':
    # macOS 中文字體設定
    matplotlib.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS', 'Hiragino Sans GB', 'STHeiti']
    matplotlib.rcParams['axes.unicode_minus'] = False

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

def _convert_to_dict(cursor, rows):
    """Helper function to convert tuple-based query results to a list of dictionaries."""
    if not rows:
        return []
    column_names = [description[0] for description in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]

def fetch_intensity_data(start_time_ms=None, end_time_ms=None, tz_utc_8=timezone(timedelta(hours=8))):
    """從 SQLite 資料庫獲取並顯示 intensity_data 表的內容"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 查詢指定時間範圍內的資料
        query = """
            SELECT * FROM intensity_data
            WHERE timestamp_ms >= ? AND timestamp_ms <= ?
            ORDER BY timestamp_ms ASC;
        """
        params = (start_time_ms, end_time_ms)
        
        start_time_str = datetime.fromtimestamp(start_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = datetime.fromtimestamp(end_time_ms / 1000.0, tz=tz_utc_8).strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n執行查詢: intensity_data from {start_time_str} to {end_time_str} (UTC+8)")

        cursor.execute(query, params)
        results_tuples = cursor.fetchall()
        
        # Convert results to list of dictionaries
        results_dicts = _convert_to_dict(cursor, results_tuples)
        
        conn.close()
        
        print(f"✓ 查詢到 {len(results_dicts)} 筆資料")
        return results_dicts

    except sqlite3.Error as err:
        print(f"✗ 查詢 `intensity_data` 時發生 SQLite 錯誤: {err}")
        return None


def intensity_analyze_print(results):
    if not results:
        print("在指定條件下，`intensity_data` 表中沒有資料。")
        return None

    # --- 新增統計分析 (使用 intensity 平均值當作過濾值) ---
    try:
        filter_intensity = sum(row['intensity'] for row in results) / len(results)
        filtered_results = [row for row in results if row['intensity'] > filter_intensity]
    except (ZeroDivisionError, TypeError):
        filtered_results = []


    if not filtered_results:
        print(f"在指定時間範圍內，沒有計測震度高於平均值的資料。")
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
        level = format_intensity(intensity)
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
        print("各震度持續時間:")
        jma_order = ["0級", "1級", "2級", "3級", "4級", "5弱", "5強", "6弱", "6強", "7級"]
        sorted_levels = sorted(intensity_counts.keys(), key=lambda x: jma_order.index(x) if x in jma_order else len(jma_order))
        for level in sorted_levels:
            count = intensity_counts[level]
            print(f"  - 震度 {level}: {count} 秒")

    if max_intensity_time:
        max_intensity_dt = datetime.fromtimestamp(max_intensity_time / 1000.0, tz=timezone(timedelta(hours=8)))
        max_intensity_str = format_intensity(max_intensity)
        print(f"最大震度: {max_intensity_str} (最大計測震度: {max_intensity:.1f}) (發生於 {max_intensity_dt.strftime('%Y-%m-%d %H:%M:%S')})")

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
        try:
            filter_intensity = sum(row['intensity'] for row in results) / len(results)
            filtered_results = [row for row in results if row['intensity'] > filter_intensity]
        except (ZeroDivisionError, TypeError):
            filtered_results = []
            
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
        jma_order = ["0級", "1級", "2級", "3級", "4級", "5弱", "5強", "6弱", "6強", "7級"]
        levels = sorted(intensity_counts.keys(), key=lambda x: jma_order.index(x) if x in jma_order else len(jma_order))
        durations = [intensity_counts[level] for level in levels]

        bars = ax2.bar(levels, durations, color='#ff6b6b')

        ax2.set_title('各震度級別持續時間')
        ax2.set_xlabel('震度級別')
        ax2.set_ylabel('持續時間 (秒)')
        ax2.grid(axis='y', linestyle='--', alpha=0.7)

        for bar in bars:
            yval = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2.0, yval + 0.5, f'{yval}s', ha='center', va='bottom', color='white')
    else:
        ax2.set_title('各震度級別持續時間 (無資料)')
        ax2.text(0.5, 0.5, '沒有震度持續時間資料可顯示。', horizontalalignment='center', verticalalignment='center', transform=ax2.transAxes, color='gray')


    plt.tight_layout(pad=3.0)
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='從 SQLite 資料庫查詢地震強度資料並繪製圖表。')
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

    try:
        start_time_ms = int(start_dt_aware.timestamp() * 1000)
        end_time_ms = int(end_dt_aware.timestamp() * 1000)

        results = fetch_intensity_data(start_time_ms, end_time_ms, tz_utc_8)
        intensity_counts = intensity_analyze_print(results)
        plot_charts(results, intensity_counts, start_time_ms, end_time_ms, tz_utc_8)

    except Exception as e:
        print(f"✗ 處理資料時發生未預期錯誤: {e}")
    except KeyboardInterrupt:
        print("\n程式被用戶中斷。正在退出…")

if __name__ == '__main__':
    main()
