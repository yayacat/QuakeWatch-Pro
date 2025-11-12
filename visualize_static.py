"""
QuakeWatch - 靜態資料視覺化
直接讀取 earthquake_data_202511101300.db 並繪製所有圖表
"""

import sys
import sqlite3
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
from datetime import datetime, timezone

# 中文字體設定
import matplotlib
if sys.platform.startswith('win'):
    matplotlib.rcParams['font.sans-serif'] = ['Microsoft JhengHei',
                                              'Microsoft YaHei', 'SimHei']
    matplotlib.rcParams['axes.unicode_minus'] = False
elif sys.platform == 'darwin':
    matplotlib.rcParams['font.sans-serif'] = ['PingFang SC', 'Arial Unicode MS',
                                              'Hiragino Sans GB', 'STHeiti']
    matplotlib.rcParams['axes.unicode_minus'] = False

# 資料庫檔案
DB_FILE = 'earthquake_data_202511101300.db'

# ========== 時間範圍設定 ==========
# 設定顯示的資料範圍（秒）
START_TIME = 60        # 開始時間（秒，相對於第一筆資料）
DURATION = 80         # 持續時間（秒），None = 顯示所有資料
# 範例：
# START_TIME = 0, DURATION = 60  -> 顯示前 60 秒
# START_TIME = 100, DURATION = 30 -> 顯示 100-130 秒
# START_TIME = 0, DURATION = None -> 顯示所有資料
# =================================

# FFT 參數
FFT_SIZE = 1024
FFT_FS = 50
FFT_WINDOW = np.hanning(FFT_SIZE).astype(np.float32)
FFT_PSD_SCALE = 1.0 / (FFT_FS * FFT_SIZE)
N_HALF_PLUS_ONE = FFT_SIZE // 2 + 1
FFT_FREQS_POS = np.fft.rfftfreq(FFT_SIZE, d=1.0/FFT_FS)

# 聲譜圖參數
SPEC_NPERSEG = 50
SPEC_NOVERLAP = int(50 * 0.85)
SPEC_FREQ_MIN = 1
SPEC_FREQ_MAX = 10
SPEC_POWER_MIN = -40
SPEC_POWER_MAX = 0


def compute_psd_db(fft_data):
    """計算功率譜密度並轉換為 dB"""
    dft = fft_data[:N_HALF_PLUS_ONE]
    psd = FFT_PSD_SCALE * np.abs(dft)**2
    psd[1:-1] *= 2
    psd_db = 10 * np.log10(psd + 1e-20)
    return np.clip(psd_db, -110, 0)


def load_data_from_db():
    """從資料庫載入所有資料"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 載入 sensor_data
    cursor.execute('''
        SELECT timestamp_ms, x, y, z, received_time
        FROM sensor_data
        ORDER BY timestamp_ms ASC
    ''')
    sensor_rows = cursor.fetchall()

    # 載入 intensity_data
    cursor.execute('''
        SELECT timestamp_ms, intensity, a, received_time
        FROM intensity_data
        ORDER BY timestamp_ms ASC
    ''')
    intensity_rows = cursor.fetchall()

    # 載入 filtered_data
    cursor.execute('''
        SELECT timestamp_ms, h1, h2, v, received_time
        FROM filtered_data
        ORDER BY timestamp_ms ASC
    ''')
    filtered_rows = cursor.fetchall()

    conn.close()

    return sensor_rows, intensity_rows, filtered_rows


def process_sensor_data(sensor_rows):
    """處理感測器資料"""
    if not sensor_rows:
        return {}, None

    timestamps = np.array([row[0] for row in sensor_rows])
    x_data = np.array([row[1] for row in sensor_rows])
    y_data = np.array([row[2] for row in sensor_rows])
    z_data = np.array([row[3] for row in sensor_rows])

    # 計算相對時間（秒）
    first_timestamp = timestamps[0]
    time_data = (timestamps - first_timestamp) / 1000.0

    # 根據 START_TIME 和 DURATION 過濾資料
    if DURATION is not None:
        end_time = START_TIME + DURATION
        mask = (time_data >= START_TIME) & (time_data <= end_time)
        time_data = time_data[mask]
        x_data = x_data[mask]
        y_data = y_data[mask]
        z_data = z_data[mask]
        timestamps = timestamps[mask]
    elif START_TIME > 0:
        mask = time_data >= START_TIME
        time_data = time_data[mask]
        x_data = x_data[mask]
        y_data = y_data[mask]
        z_data = z_data[mask]
        timestamps = timestamps[mask]

    if len(x_data) == 0:
        return {}, first_timestamp

    # 計算 PGA
    pga_raw = np.sqrt(x_data**2 + y_data**2 + z_data**2)

    # 計算 FFT（使用最後 FFT_SIZE 個樣本）
    if len(x_data) >= FFT_SIZE:
        x_fft = np.fft.fft(x_data[-FFT_SIZE:] * FFT_WINDOW)
        y_fft = np.fft.fft(y_data[-FFT_SIZE:] * FFT_WINDOW)
        z_fft = np.fft.fft(z_data[-FFT_SIZE:] * FFT_WINDOW)

        psd_x = compute_psd_db(x_fft)
        psd_y = compute_psd_db(y_fft)
        psd_z = compute_psd_db(z_fft)
    else:
        psd_x = psd_y = psd_z = None

    return {
        'time': time_data,
        'x': x_data,
        'y': y_data,
        'z': z_data,
        'pga': pga_raw,
        'psd_x': psd_x,
        'psd_y': psd_y,
        'psd_z': psd_z,
        'timestamps': timestamps
    }, first_timestamp


def process_intensity_data(intensity_rows, first_timestamp):
    """處理震度資料"""
    if not intensity_rows or first_timestamp is None:
        return {}

    timestamps = np.array([row[0] for row in intensity_rows])
    intensity = np.array([row[1] for row in intensity_rows])
    a_values = np.array([row[2] for row in intensity_rows])

    # 計算相對時間
    time_data = (timestamps - first_timestamp) / 1000.0

    # 根據 START_TIME 和 DURATION 過濾資料
    if DURATION is not None:
        end_time = START_TIME + DURATION
        mask = (time_data >= START_TIME) & (time_data <= end_time)
        time_data = time_data[mask]
        intensity = intensity[mask]
        a_values = a_values[mask]
        timestamps = timestamps[mask]
    elif START_TIME > 0:
        mask = time_data >= START_TIME
        time_data = time_data[mask]
        intensity = intensity[mask]
        a_values = a_values[mask]
        timestamps = timestamps[mask]

    return {
        'time': time_data,
        'intensity': intensity,
        'a': a_values,
        'timestamps': timestamps
    }


def process_filtered_data(filtered_rows, first_timestamp):
    """處理濾波後資料"""
    if not filtered_rows or first_timestamp is None:
        return {}

    timestamps = np.array([row[0] for row in filtered_rows])
    h1_data = np.array([row[1] for row in filtered_rows])
    h2_data = np.array([row[2] for row in filtered_rows])
    v_data = np.array([row[3] for row in filtered_rows])

    # 計算相對時間
    time_data = (timestamps - first_timestamp) / 1000.0

    # 根據 START_TIME 和 DURATION 過濾資料
    if DURATION is not None:
        end_time = START_TIME + DURATION
        mask = (time_data >= START_TIME) & (time_data <= end_time)
        time_data = time_data[mask]
        h1_data = h1_data[mask]
        h2_data = h2_data[mask]
        v_data = v_data[mask]
        timestamps = timestamps[mask]
    elif START_TIME > 0:
        mask = time_data >= START_TIME
        time_data = time_data[mask]
        h1_data = h1_data[mask]
        h2_data = h2_data[mask]
        v_data = v_data[mask]
        timestamps = timestamps[mask]

    if len(h1_data) == 0:
        return {}

    # 計算 FFT
    if len(h1_data) >= FFT_SIZE:
        h1_fft = np.fft.fft(h1_data[-FFT_SIZE:] * FFT_WINDOW)
        h2_fft = np.fft.fft(h2_data[-FFT_SIZE:] * FFT_WINDOW)
        v_fft = np.fft.fft(v_data[-FFT_SIZE:] * FFT_WINDOW)

        psd_h1 = compute_psd_db(h1_fft)
        psd_h2 = compute_psd_db(h2_fft)
        psd_v = compute_psd_db(v_fft)
    else:
        psd_h1 = psd_h2 = psd_v = None

    # 計算聲譜圖（使用 h1）
    spectrogram_data = None
    if len(h1_data) >= SPEC_NPERSEG:
        freqs, times, Sxx = signal.spectrogram(
            h1_data,
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

        # 調整時間軸到相對時間
        times_adjusted = times + time_data[0]

        spectrogram_data = {
            'freqs': freqs_plot,
            'times': times_adjusted,
            'Sxx': Sxx_plot
        }

    return {
        'time': time_data,
        'h1': h1_data,
        'h2': h2_data,
        'v': v_data,
        'psd_h1': psd_h1,
        'psd_h2': psd_h2,
        'psd_v': psd_v,
        'spectrogram': spectrogram_data,
        'timestamps': timestamps
    }


def plot_all_data(sensor, intensity, filtered):
    """繪製所有圖表"""
    plt.style.use('dark_background')

    # 圖表1: 三軸加速度
    fig1 = plt.figure(num='圖表1: 三軸加速度', figsize=(10, 5))
    fig1.patch.set_facecolor('#0d1117')
    ax1 = fig1.add_subplot(111)
    ax1.set_facecolor('#161b22')
    ax1.set_title('三軸加速度', fontsize=14, fontweight='bold',
                  color='#58a6ff', pad=12)
    ax1.set_xlabel('時間 (秒)', fontsize=11)
    ax1.set_ylabel('加速度 (Gal)', fontsize=11)
    ax1.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    if 'time' in sensor:
        ax1.plot(sensor['time'], sensor['z'], '#45b7d1',
                 label='Z 軸', linewidth=1.3, alpha=0.85)
        ax1.plot(sensor['time'], sensor['y'], '#4ecdc4',
                 label='Y 軸', linewidth=1.3, alpha=0.85)
        ax1.plot(sensor['time'], sensor['x'], '#ff6b6b',
                 label='X 軸', linewidth=1.3, alpha=0.85)
        ax1.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax1.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig1.tight_layout()

    # 圖表2: 三軸頻譜
    fig2 = plt.figure(num='圖表2: 三軸頻譜', figsize=(10, 5))
    fig2.patch.set_facecolor('#0d1117')
    ax2 = fig2.add_subplot(111)
    ax2.set_facecolor('#161b22')
    ax2.set_title('三軸頻譜 0-25Hz', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax2.set_xlabel('頻率 (Hz)', fontsize=10)
    ax2.set_ylabel('功率譜密度 (dB)', fontsize=10)
    ax2.grid(True, alpha=0.2, linestyle='--', linewidth=0.6, which='both')
    ax2.set_xlim(0, 25)
    ax2.set_ylim(-110, 0)

    if sensor.get('psd_x') is not None:
        ax2.plot(FFT_FREQS_POS, sensor['psd_z'], '#45b7d1',
                 label='Z 軸', linewidth=1.2, alpha=0.8)
        ax2.plot(FFT_FREQS_POS, sensor['psd_y'], '#4ecdc4',
                 label='Y 軸', linewidth=1.2, alpha=0.8)
        ax2.plot(FFT_FREQS_POS, sensor['psd_x'], '#ff6b6b',
                 label='X 軸', linewidth=1.2, alpha=0.8)
        ax2.legend(loc='upper right', fontsize=9, framealpha=0.7)
    fig2.tight_layout()

    # 圖表3: 三軸頻譜(濾波)
    fig3 = plt.figure(num='圖表3: 三軸頻譜(濾波)', figsize=(10, 5))
    fig3.patch.set_facecolor('#0d1117')
    ax3 = fig3.add_subplot(111)
    ax3.set_facecolor('#161b22')
    ax3.set_title('三軸頻譜 0-25Hz (濾波)', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax3.set_xlabel('頻率 (Hz)', fontsize=10)
    ax3.set_ylabel('功率譜密度 (dB)', fontsize=10)
    ax3.grid(True, alpha=0.2, linestyle='--', linewidth=0.6, which='both')
    ax3.set_xlim(0, 25)
    ax3.set_ylim(-110, 0)

    if filtered.get('psd_h1') is not None:
        ax3.plot(FFT_FREQS_POS, filtered['psd_v'], '#45b7d1',
                 label='Z 軸', linewidth=1.2, alpha=0.8)
        ax3.plot(FFT_FREQS_POS, filtered['psd_h2'], '#4ecdc4',
                 label='Y 軸', linewidth=1.2, alpha=0.8)
        ax3.plot(FFT_FREQS_POS, filtered['psd_h1'], '#ff6b6b',
                 label='X 軸', linewidth=1.2, alpha=0.8)
        ax3.legend(loc='upper right', fontsize=9, framealpha=0.7)
    fig3.tight_layout()

    # 圖表4: PGA + 計測震度
    fig4 = plt.figure(num='圖表4: PGA + 計測震度', figsize=(12, 5))
    fig4.patch.set_facecolor('#0d1117')
    ax4 = fig4.add_subplot(111)
    ax4.set_facecolor('#161b22')
    ax4_twin = ax4.twinx()
    ax4_twin.set_facecolor('#161b22')

    ax4.set_title('PGA + 計測震度', fontsize=13,
                  fontweight='bold', color='#58a6ff', pad=10)
    ax4.set_xlabel('時間 (秒)', fontsize=10)
    ax4.set_ylabel('PGA (Gal)', fontsize=10, color='white')
    ax4_twin.set_ylabel('震度', fontsize=10, color='#ffd93d')
    ax4.grid(True, alpha=0.25, linestyle='--', linewidth=0.6)

    line_pga_raw = None
    line_pga_filt = None
    line_i = None

    if 'pga' in sensor:
        line_pga_raw, = ax4.plot(sensor['time'], sensor['pga'], '#ff9500',
                                 label='PGA', linewidth=1.8, alpha=0.85)

    if 'a' in intensity:
        line_pga_filt, = ax4.plot(intensity['time'], intensity['a'], '#6bcf7f',
                                  label='PGA(濾波)', linewidth=2, alpha=0.95)

    if 'intensity' in intensity:
        line_i, = ax4_twin.plot(intensity['time'], intensity['intensity'],
                                '#ffd93d', label='計測震度', linewidth=2.5,
                                marker='o', markersize=5,
                                markerfacecolor='#ffd93d',
                                markeredgecolor='white',
                                markeredgewidth=0.6, alpha=0.95)

    ax4.set_ylim(-1, 30)
    ax4_twin.set_ylim(-0.5, 7)
    ax4.axhline(y=0, color='gray', linestyle='-', linewidth=0.6, alpha=0.3)

    for level in [1, 2, 3, 4, 5]:
        ax4_twin.axhline(y=level, color='gray', linestyle=':',
                         linewidth=0.5, alpha=0.25)

    lines_leg = [l for l in [line_pga_raw,
                             line_pga_filt, line_i] if l is not None]
    labels_leg = ['PGA', 'PGA(濾波)', '計測震度'][:len(lines_leg)]
    ax4.legend(lines_leg, labels_leg, loc='upper right',
               fontsize=10, framealpha=0.8)
    fig4.tight_layout()

    # 圖表5: 三軸加速度(濾波)
    fig5 = plt.figure(num='圖表5: 三軸加速度(濾波)', figsize=(10, 5))
    fig5.patch.set_facecolor('#0d1117')
    ax5 = fig5.add_subplot(111)
    ax5.set_facecolor('#161b22')
    ax5.set_title('三軸加速度(濾波)', fontsize=14,
                  fontweight='bold', color='#58a6ff', pad=12)
    ax5.set_xlabel('時間 (秒)', fontsize=11)
    ax5.set_ylabel('加速度 (Gal)', fontsize=11)
    ax5.grid(True, alpha=0.25, linestyle='--', linewidth=0.7)

    if 'h1' in filtered:
        ax5.plot(filtered['time'], filtered['v'], '#45b7d1',
                 label='Z 軸', linewidth=1.3, alpha=0.85)
        ax5.plot(filtered['time'], filtered['h2'], '#4ecdc4',
                 label='Y 軸', linewidth=1.3, alpha=0.85)
        ax5.plot(filtered['time'], filtered['h1'], '#ff6b6b',
                 label='X 軸', linewidth=1.3, alpha=0.85)
        ax5.legend(loc='upper right', fontsize=10, framealpha=0.8)
    ax5.axhline(y=0, color='gray', linestyle='-', linewidth=0.7, alpha=0.3)
    fig5.tight_layout()

    # 圖表6: 聲譜圖
    fig6 = plt.figure(num='圖表6: 聲譜圖', figsize=(12, 6))
    fig6.patch.set_facecolor('#0d1117')
    ax6 = fig6.add_subplot(111)
    ax6.set_facecolor('#161b22')
    ax6.set_title('聲譜圖 (Spectrogram) - X軸濾波', fontsize=14,
                  fontweight='bold', color='#58a6ff', pad=12)
    ax6.set_xlabel('時間 (秒)', fontsize=11)
    ax6.set_ylabel('頻率 (Hz)', fontsize=11)

    if filtered.get('spectrogram') is not None:
        spec = filtered['spectrogram']
        im = ax6.imshow(
            spec['Sxx'],
            aspect='auto',
            origin='lower',
            cmap='jet',
            vmin=SPEC_POWER_MIN,
            vmax=SPEC_POWER_MAX,
            extent=[spec['times'][0], spec['times'][-1],
                    SPEC_FREQ_MIN, SPEC_FREQ_MAX]
        )
        cbar = fig6.colorbar(im, ax=ax6, pad=0.01)
        cbar.set_label('功率 (dB)', fontsize=10, color='white')
        cbar.ax.tick_params(labelsize=9, colors='white')

    fig6.tight_layout()

    # 顯示所有圖表
    plt.show()


def print_data_info(sensor, intensity, filtered):
    """顯示資料資訊"""
    print("="*60)
    print("資料庫: " + DB_FILE)
    print("="*60)
    print(f"\n時間範圍設定:")
    print(f"  開始時間: {START_TIME} 秒")
    if DURATION is not None:
        print(f"  持續時間: {DURATION} 秒")
        print(f"  結束時間: {START_TIME + DURATION} 秒")
    else:
        print(f"  持續時間: 全部資料")
    print("="*60)

    if 'timestamps' in sensor:
        print(f"\n感測器資料點數: {len(sensor['timestamps'])}")
        first_ts = sensor['timestamps'][0]
        last_ts = sensor['timestamps'][-1]
        dt_first = datetime.fromtimestamp(first_ts / 1000.0, tz=timezone.utc)
        dt_last = datetime.fromtimestamp(last_ts / 1000.0, tz=timezone.utc)
        print(f"時間範圍: {dt_first.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
        print(f"        至 {dt_last.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
        duration = (last_ts - first_ts) / 1000.0
        print(f"資料時間跨度: {duration:.2f} 秒")

    if 'intensity' in intensity:
        print(f"\n震度資料點數: {len(intensity['intensity'])}")
        if len(intensity['intensity']) > 0:
            max_intensity = np.max(intensity['intensity'])
            print(f"最大震度: {max_intensity:.2f}")

    if 'h1' in filtered:
        print(f"\n濾波資料點數: {len(filtered['h1'])}")

    print("\n" + "="*60)
    print("正在顯示圖表...")
    print("="*60 + "\n")


def main():
    """主程式"""
    print("QuakeWatch - 靜態資料視覺化")
    print("="*60)

    # 載入資料
    print("正在從資料庫載入資料...")
    sensor_rows, intensity_rows, filtered_rows = load_data_from_db()

    # 處理資料
    print("正在處理資料...")
    sensor, first_timestamp = process_sensor_data(sensor_rows)
    intensity = process_intensity_data(intensity_rows, first_timestamp)
    filtered = process_filtered_data(filtered_rows, first_timestamp)

    # 顯示資料資訊
    print_data_info(sensor, intensity, filtered)

    # 繪製圖表
    plot_all_data(sensor, intensity, filtered)


if __name__ == '__main__':
    main()
