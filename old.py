"""
QuakeWatch - ES-Net Data Visualization
地震 ESP32 資料視覺化 - 從 SQLite 讀取數據並顯示圖表
"""

import sqlite3
import sys
import time
import threading
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
from datetime import datetime, timezone, timedelta

DB_FILE = 'earthquake_data.db'
TZ_UTC_8 = timezone(timedelta(hours=8))
DATA_WINDOW_LENGTH = 60
MAX_SAMPLES_SENSOR = int(DATA_WINDOW_LENGTH * 50 * 1.2)
MAX_SAMPLES_INTENSITY = int(DATA_WINDOW_LENGTH * 2 * 1.2)

x_data = deque(maxlen=MAX_SAMPLES_SENSOR)
y_data = deque(maxlen=MAX_SAMPLES_SENSOR)
z_data = deque(maxlen=MAX_SAMPLES_SENSOR)
time_data = deque(maxlen=MAX_SAMPLES_SENSOR)
timestamp_data = deque(maxlen=MAX_SAMPLES_SENSOR)

x_filtered = deque(maxlen=MAX_SAMPLES_SENSOR)
y_filtered = deque(maxlen=MAX_SAMPLES_SENSOR)
z_filtered = deque(maxlen=MAX_SAMPLES_SENSOR)

pga_raw = deque(maxlen=MAX_SAMPLES_SENSOR)
pga_filtered = deque(maxlen=MAX_SAMPLES_SENSOR)

intensity_history = deque(maxlen=MAX_SAMPLES_INTENSITY)
a_history = deque(maxlen=MAX_SAMPLES_INTENSITY)
intensity_time = deque(maxlen=MAX_SAMPLES_INTENSITY)
intensity_timestamp = deque(maxlen=MAX_SAMPLES_INTENSITY)

h1_data = deque(maxlen=MAX_SAMPLES_SENSOR)
h2_data = deque(maxlen=MAX_SAMPLES_SENSOR)
v_data = deque(maxlen=MAX_SAMPLES_SENSOR)
filtered_time = deque(maxlen=MAX_SAMPLES_SENSOR)
filtered_timestamp = deque(maxlen=MAX_SAMPLES_SENSOR)

packet_count = {'sensor': 0, 'intensity': 0, 'filtered': 0, 'error': 0}
start_time = time.time()
first_timestamp = None
first_received_time = None

data_lock = threading.Lock()
parsing_active = threading.Event()
parsing_active.set()

parse_stats = {
    'total_parsed': 0,
    'last_report_time': time.time(),
    'last_report_count': 0
}

FFT_SIZE = 1024
FFT_FS = 50
FFT_N = FFT_SIZE
FFT_FREQS_POS = np.fft.rfftfreq(FFT_N, d=1.0/FFT_FS)
FFT_WINDOW = np.hanning(FFT_SIZE).astype(np.float32)
FFT_PSD_SCALE = 1.0 / (FFT_FS * FFT_N)
N_HALF_PLUS_ONE = FFT_N // 2 + 1

NPAD = 4096
FILTER_FREQS = np.fft.fftfreq(NPAD, d=1.0/FFT_FS)
FILTER_COEFFS = None


def jma_low_cut_filter(f):
    """JMA Low-cut 濾波器: sqrt(1 - exp(-(f/0.5)^3))"""
    ratio = f / 0.5
    return np.sqrt(1.0 - np.exp(-np.power(ratio, 3.0)))


def jma_high_cut_filter(f):
    """JMA High-cut 濾波器"""
    y = f / 10.0
    y2 = y * y
    y4 = y2 * y2
    y6 = y4 * y2
    y8 = y6 * y2
    y10 = y8 * y2
    y12 = y10 * y2

    denominator = (1.0 + 0.694 * y2 + 0.241 * y4 + 0.0557 * y6 +
                   0.009664 * y8 + 0.00134 * y10 + 0.000155 * y12)

    return np.power(denominator, -0.5)


def jma_period_effect_filter(f):
    """JMA 周期效果濾波器: sqrt(1/f)"""
    with np.errstate(divide='ignore', invalid='ignore'):
        result = np.sqrt(1.0 / f)
        result[f == 0] = 0.0
    return result


def jma_combined_filter(f):
    """JMA 綜合濾波器"""
    with np.errstate(divide='ignore', invalid='ignore'):
        result = jma_low_cut_filter(
            f) * jma_high_cut_filter(f) * jma_period_effect_filter(f)
        result[f == 0] = 0.0
    return result


def compute_psd_db(fft_data):
    """計算功率譜密度並轉換為 dB"""
    # 只取正頻率部分
    dft = fft_data[:N_HALF_PLUS_ONE]

    # 計算 PSD
    psd = FFT_PSD_SCALE * np.abs(dft)**2

    # 對於非直流和奈奎斯特頻率，功率需要乘以 2
    psd[1:-1] *= 2

    # 轉換為 dB 並限制範圍
    psd_db = 10 * np.log10(psd + 1e-20)
    return np.clip(psd_db, -110, 0)


def init_filter_coeffs():
    """預先計算濾波器係數"""
    global FILTER_COEFFS
    FILTER_COEFFS = jma_combined_filter(
        np.abs(FILTER_FREQS)).astype(np.float32)


def apply_jma_filter(data, sampling_rate=50):
    """
    應用 JMA 綜合濾波器 (使用 FFT 頻域濾波)
    參考: ES-Net intensity.cpp 實現
    """
    if len(data) < 50:
        return data

    data_array = np.array(data, dtype=np.float32)
    n = len(data_array)

    padded_data = np.pad(data_array, (0, NPAD - n), mode='constant')
    fft_data = np.fft.fft(padded_data)
    filtered_fft = fft_data * FILTER_COEFFS
    filtered_data = np.fft.ifft(filtered_fft).real

    return filtered_data[:n]


def clean_old_data():
    """清理超過時間窗口的舊資料"""
    if len(time_data) == 0:
        return

    current_rel_time = time_data[-1]
    cutoff_time = current_rel_time - DATA_WINDOW_LENGTH

    if len(time_data) > 0:
        remove_count = 0
        for t in time_data:
            if t >= cutoff_time:
                break
            remove_count += 1

        if remove_count > 0:
            for _ in range(remove_count):
                time_data.popleft()
                x_data.popleft()
                y_data.popleft()
                z_data.popleft()
                timestamp_data.popleft()

    if len(intensity_time) > 0:
        remove_count = 0
        for t in intensity_time:
            if t >= cutoff_time:
                break
            remove_count += 1

        if remove_count > 0:
            for _ in range(remove_count):
                intensity_time.popleft()
                intensity_history.popleft()
                a_history.popleft()
                intensity_timestamp.popleft()

    if len(filtered_time) > 0:
        remove_count = 0
        for t in filtered_time:
            if t >= cutoff_time:
                break
            remove_count += 1

        if remove_count > 0:
            for _ in range(remove_count):
                filtered_time.popleft()
                h1_data.popleft()
                h2_data.popleft()
                v_data.popleft()
                filtered_timestamp.popleft()


def parsing_thread():
    """獨立的資料解析線程 - 從 SQLite 讀取數據"""
    global first_timestamp, first_received_time

    first_received_time = None

    print(f"[解析線程] 已啟動 (時間窗口: {DATA_WINDOW_LENGTH} 秒)\n")
    report_interval = 1.0
    clean_interval = 2.0
    filter_interval = 0.5
    last_clean_time = time.time()
    last_filter_time = time.time()
    last_rx_sensor = None
    last_rx_intensity = None
    last_rx_filtered = None

    while parsing_active.is_set():
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        cursor = conn.cursor()

        try:
            cutoff_time = time.time() - 60

            if last_rx_sensor is None:
                cursor.execute(
                    'SELECT timestamp_ms, x, y, z, received_time FROM sensor_data WHERE received_time > ? ORDER BY received_time ASC',
                    (cutoff_time,))
                sensor_rows = cursor.fetchall()
                if sensor_rows:
                    last_rx_sensor = sensor_rows[-1][4]
            else:
                cursor.execute(
                    'SELECT timestamp_ms, x, y, z, received_time FROM sensor_data WHERE received_time > ? AND received_time > ? ORDER BY received_time ASC',
                    (max(last_rx_sensor, cutoff_time), cutoff_time))
                sensor_rows = cursor.fetchall()

            if sensor_rows:
                with data_lock:
                    for row in sensor_rows:
                        timestamp, x, y, z, received_time = row
                        last_rx_sensor = received_time
                        if first_received_time is None:
                            first_received_time = received_time
                        if first_timestamp is None and timestamp > 0:
                            first_timestamp = timestamp

                        x_data.append(x)
                        y_data.append(y)
                        z_data.append(z)

                        if timestamp > 0:
                            if timestamp > 1000000000000:
                                if first_timestamp is None or timestamp < first_timestamp:
                                    first_timestamp = timestamp
                            else:
                                if first_timestamp is None:
                                    first_timestamp = timestamp

                            adjusted_time = (
                                timestamp - first_timestamp) / 1000.0
                        else:
                            adjusted_time = received_time - first_received_time

                        time_data.append(adjusted_time)
                        timestamp_data.append(timestamp)
                        parse_stats['total_parsed'] += 1

            if last_rx_intensity is None:
                cursor.execute(
                    'SELECT timestamp_ms, intensity, a, received_time FROM intensity_data WHERE received_time > ? ORDER BY received_time ASC',
                    (cutoff_time,))
                intensity_rows = cursor.fetchall()
                if intensity_rows:
                    last_rx_intensity = intensity_rows[-1][3]
            else:
                cursor.execute(
                    'SELECT timestamp_ms, intensity, a, received_time FROM intensity_data WHERE received_time > ? AND received_time > ? ORDER BY received_time ASC',
                    (max(last_rx_intensity, cutoff_time), cutoff_time))
                intensity_rows = cursor.fetchall()

            if intensity_rows:
                with data_lock:
                    for row in intensity_rows:
                        timestamp, intensity, a, received_time = row
                        last_rx_intensity = received_time
                        if first_received_time is None:
                            first_received_time = received_time
                        if first_timestamp is None and timestamp > 0:
                            first_timestamp = timestamp
                        intensity_history.append(intensity)
                        a_history.append(a)
                        intensity_time.append(
                            received_time - first_received_time)
                        intensity_timestamp.append(timestamp)
                        parse_stats['total_parsed'] += 1

            if last_rx_filtered is None:
                cursor.execute(
                    'SELECT timestamp_ms, h1, h2, v, received_time FROM filtered_data WHERE received_time > ? ORDER BY received_time ASC',
                    (cutoff_time,))
                filtered_rows = cursor.fetchall()
                if filtered_rows:
                    last_rx_filtered = filtered_rows[-1][4]
            else:
                cursor.execute(
                    'SELECT timestamp_ms, h1, h2, v, received_time FROM filtered_data WHERE received_time > ? AND received_time > ? ORDER BY received_time ASC',
                    (max(last_rx_filtered, cutoff_time), cutoff_time))
                filtered_rows = cursor.fetchall()

            if filtered_rows:
                with data_lock:
                    for row in filtered_rows:
                        timestamp, h1, h2, v, received_time = row
                        last_rx_filtered = received_time
                        if first_received_time is None:
                            first_received_time = received_time
                        if first_timestamp is None and timestamp > 0:
                            first_timestamp = timestamp

                        h1_data.append(h1)
                        h2_data.append(h2)
                        v_data.append(v)

                        if timestamp > 0:
                            if timestamp > 1000000000000:
                                if first_timestamp is None or timestamp < first_timestamp:
                                    first_timestamp = timestamp
                            else:
                                if first_timestamp is None:
                                    first_timestamp = timestamp

                            adjusted_time_filt = (
                                timestamp - first_timestamp) / 1000.0
                        else:
                            adjusted_time_filt = received_time - first_received_time

                        filtered_time.append(adjusted_time_filt)
                        filtered_timestamp.append(timestamp)
                        parse_stats['total_parsed'] += 1

            conn.close()

        except Exception as e:
            if conn:
                conn.close()

        time.sleep(0.1)

        current_check_time = time.time()
        if current_check_time - last_clean_time >= clean_interval:
            with data_lock:
                clean_old_data()
            last_clean_time = current_check_time

        if current_check_time - last_filter_time >= filter_interval:
            if len(x_data) >= 50:
                with data_lock:
                    try:
                        x_arr = np.array(x_data, dtype=np.float32)
                        y_arr = np.array(y_data, dtype=np.float32)
                        z_arr = np.array(z_data, dtype=np.float32)
                        pga_raw_arr = np.sqrt(x_arr**2 + y_arr**2 + z_arr**2)

                        pga_raw.clear()
                        pga_raw.extend(pga_raw_arr)

                        x_filt = apply_jma_filter(list(x_data))
                        y_filt = apply_jma_filter(list(y_data))
                        z_filt = apply_jma_filter(list(z_data))

                        if len(x_filt) == len(x_data):
                            x_filtered.clear()
                            x_filtered.extend(x_filt)
                            y_filtered.clear()
                            y_filtered.extend(y_filt)
                            z_filtered.clear()
                            z_filtered.extend(z_filt)

                            pga_filt_arr = np.sqrt(
                                x_filt**2 + y_filt**2 + z_filt**2)
                            pga_filtered.clear()
                            pga_filtered.extend(pga_filt_arr)
                    except Exception as e:
                        pass

            last_filter_time = current_check_time

        if current_check_time - parse_stats['last_report_time'] >= report_interval:
            with data_lock:
                parsed_count = parse_stats['total_parsed'] - \
                    parse_stats['last_report_count']
                rate = parsed_count / report_interval
                time_span = time_data[-1] - \
                    time_data[0] if len(time_data) > 1 else 0

    print("[解析線程] 已停止")


_last_fft_update_time = 0
_fft_update_interval = 0.3


def update_plot(frame):
    """更新圖表 - 僅從緩衝區讀取資料"""
    global _last_fft_update_time

    current_time = time.time() - start_time
    should_update_fft = (
        current_time - _last_fft_update_time) >= _fft_update_interval

    with data_lock:
        if len(time_data) > 0:
            current_rel_time = time_data[-1]
            x_min = max(0, current_rel_time - DATA_WINDOW_LENGTH)
            x_max = current_rel_time
        else:
            x_min = 0
            x_max = DATA_WINDOW_LENGTH

        data_len = len(time_data)
        if data_len > 0:
            time_list = list(time_data)
            x_list = list(x_data)
            y_list = list(y_data)
            z_list = list(z_data)
        else:
            time_list = x_list = y_list = z_list = []

        if data_len > 0:
            line_x.set_data(time_list, x_list)
            line_y.set_data(time_list, y_list)
            line_z.set_data(time_list, z_list)
            ax1.set_xlim(x_min, x_max)

        x_filt_list = []
        y_filt_list = []
        z_filt_list = []
        if len(x_filtered) == data_len:
            x_filt_list = list(x_filtered)
            y_filt_list = list(y_filtered)
            z_filt_list = list(z_filtered)
            line_x_filt.set_data(time_list, x_filt_list)
            line_y_filt.set_data(time_list, y_filt_list)
            line_z_filt.set_data(time_list, z_filt_list)
        ax2.set_xlim(x_min, x_max)

        if should_update_fft and data_len >= FFT_SIZE:
            x_arr = np.array(x_list[-FFT_SIZE:], dtype=np.float32)
            y_arr = np.array(y_list[-FFT_SIZE:], dtype=np.float32)
            z_arr = np.array(z_list[-FFT_SIZE:], dtype=np.float32)

            fft_x = np.fft.fft(x_arr * FFT_WINDOW)
            fft_y = np.fft.fft(y_arr * FFT_WINDOW)
            fft_z = np.fft.fft(z_arr * FFT_WINDOW)

            psd_x = compute_psd_db(fft_x)
            psd_y = compute_psd_db(fft_y)
            psd_z = compute_psd_db(fft_z)

            line_fft_x.set_data(FFT_FREQS_POS, psd_x)
            line_fft_y.set_data(FFT_FREQS_POS, psd_y)
            line_fft_z.set_data(FFT_FREQS_POS, psd_z)

            if len(x_filtered) >= FFT_SIZE and len(x_filt_list) > 0:
                x_filt_arr = np.array(x_filt_list[-FFT_SIZE:], dtype=np.float32)
                y_filt_arr = np.array(y_filt_list[-FFT_SIZE:], dtype=np.float32)
                z_filt_arr = np.array(z_filt_list[-FFT_SIZE:], dtype=np.float32)

                fft_x_filt = np.fft.fft(x_filt_arr * FFT_WINDOW)
                fft_y_filt = np.fft.fft(y_filt_arr * FFT_WINDOW)
                fft_z_filt = np.fft.fft(z_filt_arr * FFT_WINDOW)

                psd_x_filt = compute_psd_db(fft_x_filt)
                psd_y_filt = compute_psd_db(fft_y_filt)
                psd_z_filt = compute_psd_db(fft_z_filt)

                line_fft_x_filt.set_data(FFT_FREQS_POS, psd_x_filt)
                line_fft_y_filt.set_data(FFT_FREQS_POS, psd_y_filt)
                line_fft_z_filt.set_data(FFT_FREQS_POS, psd_z_filt)

            _last_fft_update_time = current_time

        if data_len > 0 and len(pga_raw) == data_len:
            pga_raw_list = list(pga_raw)
            line_pga_raw_5.set_data(time_list, pga_raw_list)

        if len(intensity_time) > 0:
            intensity_time_list = list(intensity_time)
            a_history_list = list(a_history)
            intensity_history_list = list(intensity_history)
            line_pga_filt_5.set_data(intensity_time_list, a_history_list)
            line_i.set_data(intensity_time_list, intensity_history_list)

        ax5.set_xlim(x_min, x_max)

        filtered_len = len(filtered_time)
        if filtered_len > 0:
            filtered_time_list = list(filtered_time)
            h1_list = list(h1_data)
            h2_list = list(h2_data)
            v_list = list(v_data)
            line_h1.set_data(filtered_time_list, h1_list)
            line_h2.set_data(filtered_time_list, h2_list)
            line_v.set_data(filtered_time_list, v_list)
            ax6.set_xlim(x_min, x_max)

    for fig in [fig1, fig2, fig3, fig4, fig5, fig6]:
        fig.canvas.draw_idle()
    fig6.canvas.flush_events()


def print_statistics():
    """顯示統計"""
    from datetime import datetime

    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"執行時間: {elapsed:.1f} 秒")

    if elapsed > 0:
        print(
            f"感測器封包: {packet_count['sensor']} ({packet_count['sensor']/elapsed:.1f} Hz)")
        print(
            f"強度封包: {packet_count['intensity']} ({packet_count['intensity']/elapsed:.1f} Hz)")
        print(
            f"過濾封包: {packet_count['filtered']} ({packet_count['filtered']/elapsed:.1f} Hz)")
        print(f"錯誤封包: {packet_count['error']}")

    if first_timestamp is not None and first_timestamp > 0:
        dt = datetime.fromtimestamp(first_timestamp / 1000.0, tz=timezone.utc)
        print(f"\n首次時間戳記: {dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")

        if len(timestamp_data) > 0:
            latest = timestamp_data[-1]
            if latest > 0:
                dt_latest = datetime.fromtimestamp(
                    latest / 1000.0, tz=timezone.utc)
                print(
                    f"最新時間戳記: {dt_latest.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
                duration_ms = latest - first_timestamp
                print(f"資料時間跨度: {duration_ms / 1000.0:.2f} 秒")
    else:
        print("\n⚠ 未接收到有效的 NTP 時間戳記")

    print("="*60)


def main():
    """主程式"""
    global ax1, ax2, ax3, ax4, ax5, ax6, fig1, fig2, fig3, fig4, fig5, fig6
    global line_x, line_y, line_z, line_x_filt, line_y_filt, line_z_filt
    global line_fft_x, line_fft_y, line_fft_z, line_fft_x_filt, line_fft_y_filt, line_fft_z_filt
    global line_pga_raw_5, line_pga_filt_5, line_i, line_h1, line_h2, line_v

    print("QuakeWatch - ES-Net Data Visualization")
    print("="*60)

    import os
    if not os.path.exists(DB_FILE):
        print(f"\n✗ 錯誤: 找不到數據庫文件 {DB_FILE}")
        print("請先運行 python3 data_collector.py 收集數據")
        sys.exit(1)

    print(f"\n✓ 數據庫文件: {DB_FILE}")

    print("初始化濾波器係數...")
    init_filter_coeffs()
    print("✓ 濾波器係數已預先計算")

    try:
        plt.rcParams['font.sans-serif'] = ['Arial Unicode MS',
                                           'Heiti TC', 'SimHei']
        plt.rcParams['axes.unicode_minus'] = False
    except:
        pass

    plt.style.use('dark_background')

    fig1 = plt.figure(num='圖表1: 三軸加速度', figsize=(10, 5))
    fig2 = plt.figure(num='圖表2: 三軸濾波', figsize=(10, 5))
    fig3 = plt.figure(num='圖表3: 三軸頻譜', figsize=(10, 5))
    fig4 = plt.figure(num='圖表4: 三軸濾波頻譜', figsize=(10, 5))
    fig5 = plt.figure(num='圖表5: PGA + 震度', figsize=(12, 5))
    fig6 = plt.figure(num='圖表6: Serial 三軸濾波', figsize=(10, 5))

    for fig in [fig1, fig2, fig3, fig4, fig5, fig6]:
        fig.patch.set_facecolor('#0d1117')

    ax1 = fig1.add_subplot(111)
    ax2 = fig2.add_subplot(111)
    ax3 = fig3.add_subplot(111)
    ax4 = fig4.add_subplot(111)
    ax5 = fig5.add_subplot(111)
    ax6 = fig6.add_subplot(111)

    ax1.set_facecolor('#161b22')
    ax1.set_title('三軸加速度', fontsize=14, fontweight='bold',
                  color='#58a6ff', pad=12)
    ax1.set_xlabel('時間 (秒)', fontsize=11)
    ax1.set_ylabel('加速度 (Gal)', fontsize=11)
    ax1.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    line_x, = ax1.plot([], [], '#ff6b6b', label='X 軸',
                       linewidth=1.3, alpha=0.85)
    line_y, = ax1.plot([], [], '#4ecdc4', label='Y 軸',
                       linewidth=1.3, alpha=0.85)
    line_z, = ax1.plot([], [], '#45b7d1', label='Z 軸',
                       linewidth=1.3, alpha=0.85)

    ax1.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax1.set_ylim(-1, 1)
    ax1.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig1.tight_layout()

    ax2.set_facecolor('#161b22')
    ax2.set_title('三軸濾波', fontsize=14, fontweight='bold',
                  color='#58a6ff', pad=12)
    ax2.set_xlabel('時間 (秒)', fontsize=11)
    ax2.set_ylabel('加速度 (Gal)', fontsize=11)
    ax2.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    line_x_filt, = ax2.plot(
        [], [], '#ff6b6b', label='X 軸', linewidth=1.3, alpha=0.85)
    line_y_filt, = ax2.plot(
        [], [], '#4ecdc4', label='Y 軸', linewidth=1.3, alpha=0.85)
    line_z_filt, = ax2.plot(
        [], [], '#45b7d1', label='Z 軸', linewidth=1.3, alpha=0.85)

    ax2.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax2.set_ylim(-1, 1)
    ax2.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig2.tight_layout()

    ax3.set_facecolor('#161b22')
    ax3.set_title('三軸頻譜 (未濾波, 0-25Hz)', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax3.set_xlabel('頻率 (Hz)', fontsize=10)
    ax3.set_ylabel('功率譜密度 (dB)', fontsize=10)
    ax3.grid(True, alpha=0.2, linestyle='--', linewidth=0.6, which='both')
    ax3.set_xlim(0, 25)
    ax3.set_ylim(-110, 0)

    line_fft_x, = ax3.plot([], [], '#ff6b6b', label='X 軸',
                           linewidth=1.2, alpha=0.8)
    line_fft_y, = ax3.plot([], [], '#4ecdc4', label='Y 軸',
                           linewidth=1.2, alpha=0.8)
    line_fft_z, = ax3.plot([], [], '#45b7d1', label='Z 軸',
                           linewidth=1.2, alpha=0.8)
    ax3.legend(loc='upper right', fontsize=9, framealpha=0.7)

    ax4.set_facecolor('#161b22')
    ax4.set_title('三軸頻譜 (濾波後, 0-25Hz)', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax4.set_xlabel('頻率 (Hz)', fontsize=10)
    ax4.set_ylabel('功率譜密度 (dB)', fontsize=10)
    ax4.grid(True, alpha=0.2, linestyle='--', linewidth=0.6, which='both')
    ax4.set_xlim(0, 25)
    ax4.set_ylim(-110, 0)

    line_fft_x_filt, = ax4.plot(
        [], [], '#ff6b6b', label='X 軸', linewidth=1.2, alpha=0.8)
    line_fft_y_filt, = ax4.plot(
        [], [], '#4ecdc4', label='Y 軸', linewidth=1.2, alpha=0.8)
    line_fft_z_filt, = ax4.plot(
        [], [], '#45b7d1', label='Z 軸', linewidth=1.2, alpha=0.8)
    ax4.legend(loc='upper right', fontsize=9, framealpha=0.7)

    ax5.set_facecolor('#161b22')
    ax5_twin = ax5.twinx()
    ax5_twin.set_facecolor('#161b22')

    ax5.set_title('PGA + 計測震度 + 震度階級', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax5.set_xlabel('時間 (秒)', fontsize=10)
    ax5.set_ylabel('PGA (Gal)', fontsize=10, color='white')
    ax5_twin.set_ylabel('震度', fontsize=10, color='#ffd93d')
    ax5.grid(True, alpha=0.25, linestyle='--', linewidth=0.6)

    line_pga_raw_5, = ax5.plot(
        [], [], '#ff9500', label='PGA 未濾波', linewidth=1.8, alpha=0.85)
    line_pga_filt_5, = ax5.plot(
        [], [], '#6bcf7f', label='PGA 濾波 (a)', linewidth=2, alpha=0.95)

    line_i, = ax5_twin.plot([], [], '#ffd93d', label='計測震度', linewidth=2.5, marker='o',
                            markersize=5, markerfacecolor='#ffd93d', markeredgecolor='white',
                            markeredgewidth=0.6, alpha=0.95)

    ax5.set_ylim(-1, 30)
    ax5_twin.set_ylim(-0.5, 7)
    ax5.axhline(y=0, color='gray', linestyle='-', linewidth=0.6, alpha=0.3)

    for level in [1, 2, 3, 4, 5]:
        ax5_twin.axhline(y=level, color='gray', linestyle=':',
                         linewidth=0.5, alpha=0.25)

    lines_leg = [line_pga_raw_5, line_pga_filt_5, line_i]
    labels_leg = ['PGA 未濾波', 'PGA 濾波 (a)', '計測震度']
    ax5.legend(lines_leg, labels_leg, loc='upper right',
               fontsize=10, framealpha=0.8)
    fig3.tight_layout()
    fig4.tight_layout()
    fig5.tight_layout()

    ax6.set_facecolor('#161b22')
    ax6.set_title('Serial 三軸濾波 (ESP32)', fontsize=14,
                  fontweight='bold', color='#58a6ff', pad=12)
    ax6.set_xlabel('時間 (秒)', fontsize=11)
    ax6.set_ylabel('加速度 (Gal)', fontsize=11)
    ax6.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    line_h1, = ax6.plot([], [], '#ff6b6b', label='H1 軸',
                        linewidth=1.3, alpha=0.85)
    line_h2, = ax6.plot([], [], '#4ecdc4', label='H2 軸',
                        linewidth=1.3, alpha=0.85)
    line_v, = ax6.plot([], [], '#45b7d1', label='V 軸',
                       linewidth=1.3, alpha=0.85)

    ax6.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax6.set_ylim(-1, 1)
    ax6.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig6.tight_layout()

    print("\n開始接收資料...\n")

    parser = threading.Thread(target=parsing_thread, args=(), daemon=True)
    parser.start()

    ani = FuncAnimation(fig1, update_plot, interval=50,
                        blit=False, cache_frame_data=False)

    try:
        plt.show()
    except KeyboardInterrupt:
        print("\n程式終止")
    finally:
        parsing_active.clear()
        parser.join(timeout=2.0)
        print_statistics()


if __name__ == '__main__':
    main()
