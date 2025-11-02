# QuakeWatch

## 安裝

```bash
# 創建虛擬環境
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安裝依賴
pip install -r requirements.txt
```

## 使用方法

### 1. 收集數據

在終端 1 運行數據收集器：

```bash
python3 data_collector.py
```

選擇串列埠，程序將開始收集數據並存入 `earthquake_data.db`。

### 2. 顯示圖表

在終端 2 運行視覺化程序：

```bash
python3 main.py
```

## 數據格式

### 感測器數據（50Hz）

- 格式: `[0x53][timestamp: 8 bytes][X: 4 bytes][Y: 4 bytes][Z: 4 bytes][XOR: 1 byte]`
- 數據表: `sensor_data`
- 欄位: `timestamp_ms, x, y, z, received_time`

### 強度數據（2Hz）

- 格式: `[0x49][timestamp: 8 bytes][intensity: 4 bytes][a: 4 bytes][XOR: 1 byte]`
- 數據表: `intensity_data`
- 欄位: `timestamp_ms, intensity, a, received_time`

## 數據庫

所有數據保存在 `earthquake_data.db` SQLite 數據庫中。

### 自動清理

程序會自動清理超過 24 小時的舊數據。

### 查詢數據

您可以使用任何 SQLite 工具查詢數據：

```bash
sqlite3 earthquake_data.db
```

```sql
-- 查詢最近的感測器數據
SELECT * FROM sensor_data ORDER BY received_time DESC LIMIT 100;

-- 查詢最近的強度數據
SELECT * FROM intensity_data ORDER BY received_time DESC LIMIT 10;

-- 統計數據
SELECT COUNT(*) FROM sensor_data;
SELECT COUNT(*) FROM intensity_data;
```
