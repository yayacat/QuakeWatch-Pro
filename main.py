"""
QuakeWatch - ES-Net Data Visualization
地震 ESP32 資料視覺化 - 從 WebSocket 接收數據並顯示圖表
"""

import sys
import time
import threading
import json
import asyncio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from collections import deque
from datetime import datetime, timezone, timedelta
from scipy import signal
from websockets.client import connect

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

WEBSOCKET_URL = 'ws://localhost:8765'
TZ_UTC_8 = timezone(timedelta(hours=8))
DATA_WINDOW_LENGTH = 10
MAX_SAMPLES_SENSOR = int(DATA_WINDOW_LENGTH * 50 * 1.2)
MAX_SAMPLES_INTENSITY = int(DATA_WINDOW_LENGTH * 2 * 1.2)

x_data = deque(maxlen=MAX_SAMPLES_SENSOR)
y_data = deque(maxlen=MAX_SAMPLES_SENSOR)
z_data = deque(maxlen=MAX_SAMPLES_SENSOR)
time_data = deque(maxlen=MAX_SAMPLES_SENSOR)
timestamp_data = deque(maxlen=MAX_SAMPLES_SENSOR)

pga_raw = deque(maxlen=MAX_SAMPLES_SENSOR)

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

# 聲譜圖參數
SPEC_NPERSEG = 50           # FFT 窗口大小：128 樣本 = 2.56 秒
SPEC_NOVERLAP = 50*0.85     # 重疊樣本數：85/128 = 66.4% 重疊，時間步進 = 0.86 秒
SPEC_FREQ_MIN = 1           # Hz 顯示的最小頻率
SPEC_FREQ_MAX = 10          # Hz 顯示的最大頻率（0-10 Hz）
SPEC_POWER_MIN = -40        # dB 色標最小值
SPEC_POWER_MAX = 0          # dB 色標最大值
# 如果太藍：調小 SPEC_POWER_MIN（例如 -50）
# 如果太紅：調大 SPEC_POWER_MAX（例如 10）


def compute_psd_db(fft_data):
    """計算功率譜密度並轉換為 dB"""
    dft = fft_data[:N_HALF_PLUS_ONE]
    psd = FFT_PSD_SCALE * np.abs(dft)**2
    psd[1:-1] *= 2
    psd_db = 10 * np.log10(psd + 1e-20)
    return np.clip(psd_db, -110, 0)


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
                pga_raw.popleft()

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


async def websocket_receiver():
    """WebSocket 接收線程 - 從 data_collector 接收數據"""
    global first_timestamp, first_received_time

    first_received_time = None

    print(f"[WebSocket] 正在連線到 {WEBSOCKET_URL}...\n")

    while parsing_active.is_set():
        try:
            async with connect(WEBSOCKET_URL) as websocket:
                print(f"[WebSocket] 已連線\n")

                while parsing_active.is_set():
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        data = json.loads(message)
                        data_type = data['type']
                        items = data['data']

                        received_time = time.time()
                        if first_received_time is None:
                            first_received_time = received_time

                        with data_lock:
                            if data_type == 'sensor':
                                for item in items:
                                    timestamp, x, y, z = item

                                    if timestamp >= 1000000000000:
                                        if first_timestamp is None or timestamp < first_timestamp:
                                            first_timestamp = timestamp

                                    x_data.append(x)
                                    y_data.append(y)
                                    z_data.append(z)

                                    pga_value = np.sqrt(x**2 + y**2 + z**2)
                                    pga_raw.append(pga_value)

                                    if timestamp >= 1000000000000 and first_timestamp is not None:
                                        adjusted_time = (
                                            timestamp - first_timestamp) / 1000.0
                                    else:
                                        adjusted_time = received_time - first_received_time

                                    time_data.append(adjusted_time)
                                    timestamp_data.append(timestamp)
                                    parse_stats['total_parsed'] += 1

                            elif data_type == 'filtered':
                                for item in items:
                                    timestamp, h1, h2, v = item

                                    if timestamp >= 1000000000000:
                                        if first_timestamp is None or timestamp < first_timestamp:
                                            first_timestamp = timestamp

                                    h1_data.append(h1)
                                    h2_data.append(h2)
                                    v_data.append(v)

                                    if timestamp >= 1000000000000 and first_timestamp is not None:
                                        adjusted_time_filt = (
                                            timestamp - first_timestamp) / 1000.0
                                    else:
                                        adjusted_time_filt = received_time - first_received_time

                                    filtered_time.append(adjusted_time_filt)
                                    filtered_timestamp.append(timestamp)
                                    parse_stats['total_parsed'] += 1

                            elif data_type == 'intensity':
                                for item in items:
                                    timestamp, intensity, a = item

                                    if timestamp >= 1000000000000:
                                        if first_timestamp is None or timestamp < first_timestamp:
                                            first_timestamp = timestamp

                                    intensity_history.append(intensity)
                                    a_history.append(a)

                                    if timestamp >= 1000000000000 and first_timestamp is not None:
                                        adjusted_time_int = (
                                            timestamp - first_timestamp) / 1000.0
                                    else:
                                        adjusted_time_int = received_time - first_received_time

                                    intensity_time.append(adjusted_time_int)
                                    intensity_timestamp.append(timestamp)
                                    parse_stats['total_parsed'] += 1

                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        print(f"[WebSocket] 接收錯誤: {e}")
                        break

        except Exception as e:
            print(f"[WebSocket] 連線失敗: {e}")
            if parsing_active.is_set():
                print("[WebSocket] 5秒後重新連線...")
                await asyncio.sleep(5)
            else:
                break

    print("[WebSocket] 已停止")


def parsing_thread():
    """包裝 asyncio WebSocket 接收器"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # 定期清理舊資料
    async def clean_old_data_periodically():
        clean_interval = 2.0
        while parsing_active.is_set():
            await asyncio.sleep(clean_interval)
            with data_lock:
                clean_old_data()

    # 同時執行 WebSocket 接收和定期清理
    async def run_both():
        await asyncio.gather(
            websocket_receiver(),
            clean_old_data_periodically()
        )

    try:
        loop.run_until_complete(run_both())
    finally:
        loop.close()


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

            all_values = x_list + y_list + z_list
            if all_values:
                y_min = min(all_values)
                y_max = max(all_values)
                y_range = y_max - y_min
                padding = y_range * 0.15 if y_range > 0 else 1
                ax1.set_ylim(y_min - padding, y_max + padding)

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

            if len(h1_data) >= FFT_SIZE:
                h1_arr = np.array(list(h1_data)[-FFT_SIZE:], dtype=np.float32)
                h2_arr = np.array(list(h2_data)[-FFT_SIZE:], dtype=np.float32)
                v_arr = np.array(list(v_data)[-FFT_SIZE:], dtype=np.float32)

                fft_h1 = np.fft.fft(h1_arr * FFT_WINDOW)
                fft_h2 = np.fft.fft(h2_arr * FFT_WINDOW)
                fft_v = np.fft.fft(v_arr * FFT_WINDOW)

                psd_h1 = compute_psd_db(fft_h1)
                psd_h2 = compute_psd_db(fft_h2)
                psd_v = compute_psd_db(fft_v)

                line_fft_h1.set_data(FFT_FREQS_POS, psd_h1)
                line_fft_h2.set_data(FFT_FREQS_POS, psd_h2)
                line_fft_v.set_data(FFT_FREQS_POS, psd_v)

            _last_fft_update_time = current_time

        # 更新聲譜圖 - 使用濾波後的 X 軸數據（h1）
        if should_update_fft and len(h1_data) >= SPEC_NPERSEG:
            # 使用濾波後的 X 軸數據
            h1_for_spec = np.array(list(h1_data), dtype=np.float32)
            filtered_time_list = list(filtered_time)
            spec_x_min = filtered_time_list[0] if len(
                filtered_time_list) > 0 else 0
            spec_x_max = filtered_time_list[-1] if len(
                filtered_time_list) > 0 else DATA_WINDOW_LENGTH

            # 計算 STFT
            freqs, times, Sxx = signal.spectrogram(
                h1_for_spec,
                fs=FFT_FS,
                nperseg=SPEC_NPERSEG,
                noverlap=SPEC_NOVERLAP,
                window='hann',
                scaling='density'
            )

            # 轉換為 dB
            Sxx_db = 10 * np.log10(Sxx + 1e-20)

            # 只顯示指定頻帶
            freq_mask = (freqs >= SPEC_FREQ_MIN) & (freqs <= SPEC_FREQ_MAX)
            freqs_plot = freqs[freq_mask]
            Sxx_plot = Sxx_db[freq_mask, :]

            # 更新圖像
            spectrogram_image.set_data(Sxx_plot)
            spectrogram_image.set_extent(
                [spec_x_min, spec_x_max, SPEC_FREQ_MIN, SPEC_FREQ_MAX])

            # 固定色標範圍（-40 到 0 dB）
            spectrogram_image.set_clim(
                vmin=SPEC_POWER_MIN, vmax=SPEC_POWER_MAX)

        if data_len > 0:
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

            all_filtered_values = h1_list + h2_list + v_list
            if all_filtered_values:
                y_min_filt = min(all_filtered_values)
                y_max_filt = max(all_filtered_values)
                y_range_filt = y_max_filt - y_min_filt
                padding_filt = y_range_filt * 0.15 if y_range_filt > 0 else 1
                ax6.set_ylim(y_min_filt - padding_filt,
                             y_max_filt + padding_filt)

    for fig in [fig1, fig3, fig4, fig5, fig6, fig7]:
        fig.canvas.draw_idle()
    fig7.canvas.flush_events()


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
    global ax1, ax3, ax4, ax5, ax6, ax7, fig1, fig3, fig4, fig5, fig6, fig7
    global line_x, line_y, line_z
    global line_fft_x, line_fft_y, line_fft_z
    global line_fft_h1, line_fft_h2, line_fft_v
    global line_pga_raw_5, line_pga_filt_5, line_i, line_h1, line_h2, line_v
    global spectrogram_image

    print("QuakeWatch - ES-Net Data Visualization")
    print("="*60)
    print(f"\n✓ WebSocket URL: {WEBSOCKET_URL}")
    print("提示: 請先運行 python3 data_collector.py 啟動資料收集器")

    # 字體已在檔案開頭設定,這裡只設定樣式
    plt.style.use('dark_background')

    fig1 = plt.figure(num='圖表1: 三軸加速度', figsize=(10, 5))
    fig3 = plt.figure(num='圖表2: 三軸頻譜', figsize=(10, 5))
    fig4 = plt.figure(num='圖表3: 三軸頻譜(濾波)', figsize=(10, 5))
    fig5 = plt.figure(num='圖表4: PGA + 計測震度', figsize=(12, 5))
    fig6 = plt.figure(num='圖表5: 三軸加速度(濾波)', figsize=(10, 5))
    fig7 = plt.figure(num='圖表6: 聲譜圖', figsize=(12, 6))

    for fig in [fig1, fig3, fig4, fig5, fig6, fig7]:
        fig.patch.set_facecolor('#0d1117')

    ax1 = fig1.add_subplot(111)
    ax3 = fig3.add_subplot(111)
    ax4 = fig4.add_subplot(111)
    ax5 = fig5.add_subplot(111)
    ax6 = fig6.add_subplot(111)
    ax7 = fig7.add_subplot(111)

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
    ax1.set_ylim(-5, 5)
    ax1.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig1.tight_layout()

    ax3.set_facecolor('#161b22')
    ax3.set_title('三軸頻譜 0-25Hz', fontsize=13,
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
    fig3.tight_layout()

    ax4.set_facecolor('#161b22')
    ax4.set_title('三軸頻譜 0-25Hz (濾波)', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax4.set_xlabel('頻率 (Hz)', fontsize=10)
    ax4.set_ylabel('功率譜密度 (dB)', fontsize=10)
    ax4.grid(True, alpha=0.2, linestyle='--', linewidth=0.6, which='both')
    ax4.set_xlim(0, 25)
    ax4.set_ylim(-110, 0)

    line_fft_h1, = ax4.plot(
        [], [], '#ff6b6b', label='X 軸', linewidth=1.2, alpha=0.8)
    line_fft_h2, = ax4.plot(
        [], [], '#4ecdc4', label='Y 軸', linewidth=1.2, alpha=0.8)
    line_fft_v, = ax4.plot([], [], '#45b7d1', label='Z 軸',
                           linewidth=1.2, alpha=0.8)
    ax4.legend(loc='upper right', fontsize=9, framealpha=0.7)
    fig4.tight_layout()

    ax5.set_facecolor('#161b22')
    ax5_twin = ax5.twinx()
    ax5_twin.set_facecolor('#161b22')

    ax5.set_title('PGA + 計測震度', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax5.set_xlabel('時間 (秒)', fontsize=10)
    ax5.set_ylabel('PGA (Gal)', fontsize=10, color='white')
    ax5_twin.set_ylabel('震度', fontsize=10, color='#ffd93d')
    ax5.grid(True, alpha=0.25, linestyle='--', linewidth=0.6)

    line_pga_raw_5, = ax5.plot(
        [], [], '#ff9500', label='PGA', linewidth=1.8, alpha=0.85)
    line_pga_filt_5, = ax5.plot(
        [], [], '#6bcf7f', label='PGA(濾波)', linewidth=2, alpha=0.95)

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
    labels_leg = ['PGA', 'PGA(濾波)', '計測震度']
    ax5.legend(lines_leg, labels_leg, loc='upper right',
               fontsize=10, framealpha=0.8)
    fig5.tight_layout()

    ax6.set_facecolor('#161b22')
    ax6.set_title('三軸加速度(濾波)', fontsize=14,
                  fontweight='bold', color='#58a6ff', pad=12)
    ax6.set_xlabel('時間 (秒)', fontsize=11)
    ax6.set_ylabel('加速度 (Gal)', fontsize=11)
    ax6.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    line_h1, = ax6.plot([], [], '#ff6b6b', label='X 軸',
                        linewidth=1.3, alpha=0.85)
    line_h2, = ax6.plot([], [], '#4ecdc4', label='Y 軸',
                        linewidth=1.3, alpha=0.85)
    line_v, = ax6.plot([], [], '#45b7d1', label='Z 軸',
                       linewidth=1.3, alpha=0.85)

    ax6.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax6.set_ylim(-1, 1)
    ax6.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig6.tight_layout()

    # 聲譜圖設定
    ax7.set_facecolor('#161b22')
    ax7.set_title('聲譜圖 (Spectrogram) - X軸濾波', fontsize=14,
                  fontweight='bold', color='#58a6ff', pad=12)
    ax7.set_xlabel('時間 (秒)', fontsize=11)
    ax7.set_ylabel('頻率 (Hz)', fontsize=11)

    # 初始化聲譜圖（稍後在 update 中更新）
    spectrogram_data = np.zeros((129, 100))  # 初始空白聲譜圖
    spectrogram_image = ax7.imshow(
        spectrogram_data,
        aspect='auto',
        origin='lower',
        cmap='jet',  # 藍色到紅色
        vmin=SPEC_POWER_MIN,
        vmax=SPEC_POWER_MAX,
        extent=[0, DATA_WINDOW_LENGTH, 0, SPEC_FREQ_MAX]
    )

    # 添加色條
    cbar = fig7.colorbar(spectrogram_image, ax=ax7, pad=0.01)
    cbar.set_label('功率 (dB)', fontsize=10, color='white')
    cbar.ax.tick_params(labelsize=9, colors='white')

    fig7.tight_layout()

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
