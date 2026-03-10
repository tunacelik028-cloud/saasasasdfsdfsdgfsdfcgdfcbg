import asyncio
import websockets
import json
import os
import sys
import time
import random
import datetime
import threading
from threading import Thread, Semaphore
import tls_client

# --- KICK BOT LOGIC (Eski kick.py içeriği) ---
class KickBotLogic:
    def __init__(self):
        self.stop = False
        self.channel = ""
        self.channel_id = None
        self.stream_id = None
        self.max_threads = 0
        self.threads = []
        self.thread_limit = None
        self.connections = 0
        self.attempts = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.CLIENT_TOKEN = "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823"

    def clean_channel_name(self, name):
        if "kick.com/" in name:
            parts = name.split("kick.com/")
            channel = parts[1].split("/")[0].split("?")[0]
            return channel.lower()
        return name.lower()

    def get_channel_info(self, name):
        try:
            s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            s.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            })
            response = s.get(f'https://kick.com/api/v1/channels/{name}')
            if response.status_code == 200:
                data = response.json()
                self.channel_id = data.get("id")
                if 'livestream' in data and data['livestream']:
                    self.stream_id = data['livestream'].get('id')
                return self.channel_id
        except:
            pass
        return None

    def get_token(self):
        try:
            s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            s.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'X-CLIENT-TOKEN': self.CLIENT_TOKEN
            })
            response = s.get('https://websockets.kick.com/viewer/v1/token')
            if response.status_code == 200:
                return response.json().get("data", {}).get("token")
        except:
            pass
        return None

    async def websocket_handler(self, token):
        connected = False
        try:
            url = f"wss://websockets.kick.com/viewer/v1/connect?token={token}"
            async with websockets.connect(url) as ws:
                with self.lock:
                    self.connections += 1
                connected = True
                handshake = {"type": "channel_handshake", "data": {"message": {"channelId": self.channel_id}}}
                await ws.send(json.dumps(handshake))
                while not self.stop:
                    await ws.send(json.dumps({"type": "ping"}))
                    await asyncio.sleep(15 + random.randint(1, 5))
        except:
            pass
        finally:
            if connected:
                with self.lock:
                    if self.connections > 0: self.connections -= 1

    def send_connection(self):
        try:
            token = self.get_token()
            if not token or self.stop: return
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.websocket_handler(token))
            loop.close()
        except:
            pass
        finally:
            if self.thread_limit:
                self.thread_limit.release()

    def run_bot(self, thread_count, channel_name):
        self.stop = False
        self.max_threads = int(thread_count)
        self.channel = self.clean_channel_name(channel_name)
        self.thread_limit = Semaphore(self.max_threads)
        self.start_time = datetime.datetime.now()
        self.get_channel_info(self.channel)
        
        while not self.stop:
            if self.thread_limit.acquire():
                t = Thread(target=self.send_connection, daemon=True)
                t.start()
                time.sleep(0.35)

    def terminate(self):
        self.stop = True
        # Semaphore'ları serbest bırak ki döngü kırılsın
        if self.thread_limit:
            for _ in range(self.max_threads + 5):
                try: self.thread_limit.release()
                except: pass

# --- WORKER LOGIC ---
MASTER_SERVER_IP = "43.229.92.157" 
MASTER_SERVER_PORT = 8765

bot_logic = None
bot_thread = None

async def handler():
    global bot_logic, bot_thread
    uri = f"ws://{MASTER_SERVER_IP}:{MASTER_SERVER_PORT}"
    
    while True:
        try:
            print(f"🔄 Ana sunucuya bağlanılıyor ({uri})...")
            async with websockets.connect(uri) as websocket:
                print("✅ Ana sunucuya başarıyla bağlanıldı! Bekleniyor...")
                
                async for message in websocket:
                    try:
                        data = json.loads(message)
                        action = data.get("action")
                        
                        if action == "start":
                            channel = data.get("channel")
                            viewers = data.get("viewers")
                            
                            if bot_logic:
                                bot_logic.terminate()
                                
                            print(f"🚀 KICK BAŞLADI: Kanal={channel}, İzleyici={viewers}")
                            bot_logic = KickBotLogic()
                            bot_thread = Thread(target=bot_logic.run_bot, args=(viewers, channel), daemon=True)
                            bot_thread.start()
                            
                        elif action == "stop":
                            if bot_logic:
                                print("🛑 KICK DURDURULDU: Ana sunucudan stop emri geldi.")
                                bot_logic.terminate()
                                bot_logic = None
                            else:
                                print("⚠️ Çalışan işlem yok.")
                                
                    except Exception as e:
                        print(f"❌ Hata: {e}")
                        
        except Exception as e:
            print(f"❌ Bağlantı hatası: {e}. 5 saniye sonra tekrar denenecek...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("-" * 50)
    print("🚀 404 LOG SYSTEM - STANDALONE WORKER NODE 🚀")
    print("-" * 50)
    try:
        asyncio.run(handler())
    except KeyboardInterrupt:
        if bot_logic: bot_logic.terminate()
        print("Durduruldu.")
