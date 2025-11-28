import requests
import argparse

parser = argparse.ArgumentParser(description='傳送連接到WiFi網路資訊到ES-Net裝置上。')
parser.add_argument('ssid', type=str, help='連接到WiFi網路的SSID。')
parser.add_argument('password', type=str, help='連接到WiFi網路的密碼。')
args = parser.parse_args()

ssid = args.ssid
password = args.password

if not ssid or not password:
    print("必須提供連接到WiFi網路的SSID和密碼。請使用--ssid和--password參數提供它們或直接在執行檔後方依序輸入連接到WiFi網路的SSID和密碼。")
else:
    print("即將傳送以下WiFi連接資訊到ES-Net裝置：")
    print(f"SSID: {ssid}")
    print(f"密碼: {password}")
    confirm = input("請確認SSID和密碼是否正確（y/N）: ")
    if confirm.lower() != 'y':
        print("操作已取消。")
        exit()
    url = "http://192.168.4.1/wifi"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "ssid": ssid,
        "password": password
    }
    try:
        response = requests.post(url, data=data, headers=headers, timeout=5)
        print(response.text)
    except requests.exceptions.ConnectTimeout:
        print("連線超時。無法連接到 'http://192.168.4.1'。請檢查：")
        print("1. 您是否已連接到目標裝置的WiFi網路？")
    except requests.exceptions.RequestException as e:
        print(f"發生未預期的錯誤：{e}")
