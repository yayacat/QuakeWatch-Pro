import asyncio
import json
from threading import Thread
from websockets.server import serve
from websockets.exceptions import ConnectionClosedError

class WebSocketServer:
    """
    一個可重用的 WebSocket 伺服器類，用於處理資料庫查詢和廣播。
    """
    def __init__(self, db_conn, port=8765):
        """
        初始化 WebSocket 伺服器。

        :param db_conn: 資料庫連線物件。
        :param port: WebSocket 伺服器運行的埠號。
        """
        self.db_conn = db_conn
        self.port = port
        self.clients = set()

    async def _handler(self, websocket):
        """
        WebSocket 連線處理 - 支援雙向通訊。
        """
        self.clients.add(websocket)
        print(f"[WebSocket] 新客戶端 {websocket.id} 連線 (總數: {len(self.clients)})")
        try:
            async for message in websocket:
                try:
                    request = json.loads(message)
                    request_type = request.get('type')

                    if request_type == 'query':
                        # 處理 SQL 查詢請求
                        query = request.get('query')
                        params = request.get('params', [])
                        request_id = request.get('id')

                        # 轉換 params 為 tuple
                        if isinstance(params, list):
                            params = tuple(params)

                        try:
                            if self.db_conn is None:
                                raise Exception("資料庫未初始化")

                            cursor = self.db_conn.cursor()
                            cursor.execute(query, params)
                            results = cursor.fetchall()

                            # 確保結果是可序列化的
                            if results is None:
                                results = []

                            response = {
                                'type': 'query_response',
                                'id': request_id,
                                'success': True,
                                'data': results
                            }

                        except Exception as e:
                            response = {
                                'type': 'query_response',
                                'id': request_id,
                                'success': False,
                                'error': str(e)
                            }

                        try:
                            await websocket.send(json.dumps(response))
                        except ConnectionClosedError:
                            # 在發送時客戶端斷線，直接中斷
                            print(f"[WebSocket] 客戶端 {websocket.id} 在發送回應時斷線")
                            break
                        except Exception as e:
                            print(f"[WebSocket] 發送回應失敗: {e}")

                except json.JSONDecodeError as e:
                    print(f"[WebSocket] JSON 解析錯誤: {e}")
                except Exception as e:
                    print(f"[WebSocket] 處理訊息錯誤: {e}")
        except ConnectionClosedError:
            # 客戶端非正常斷線
            print(f"[WebSocket] 客戶端 {websocket.id} 非正常斷線")
            pass
        finally:
            self.clients.discard(websocket)
            print(f"[WebSocket] 客戶端 {websocket.id} 斷線 (總數: {len(self.clients)})")

    async def broadcast_data(self, data_type, data):
        """
        廣播資料到所有 WebSocket 客戶端。
        """
        if not self.clients:
            return

        message = json.dumps({
            'type': data_type,
            'data': data
        })

        # 使用 asyncio.gather 並發送給所有客戶端
        disconnected_clients = set()
        for client in self.clients:
            try:
                await client.send(message)
            except ConnectionClosedError:
                disconnected_clients.add(client)

        # 移除斷線的客戶端
        for client in disconnected_clients:
            self.clients.discard(client)

    async def _start_server(self):
        """
        啟動 WebSocket 伺服器。
        """
        async with serve(self._handler, "0.0.0.0", self.port):
            print(f"✓ WebSocket 伺服器已啟動: ws://0.0.0.0:{self.port}")
            await asyncio.Future()  # 永久運行

    def start_in_thread(self):
        """
        在背景執行 WebSocket 伺服器。
        """
        def run_asyncio_loop():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._start_server())

        websocket_thread = Thread(target=run_asyncio_loop, daemon=True)
        websocket_thread.start()
        return websocket_thread
