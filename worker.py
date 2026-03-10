import asyncio
import websockets
import json
import subprocess
import os
import sys

# BURAYA ANA SUNUCUNUN IP'SİNİ YAZMALISIN
MASTER_SERVER_IP = "43.229.92.157" # Kendi sunucusunda test için default local koydum
MASTER_SERVER_PORT = 8765

active_process = None

async def handler():
    global active_process
    uri = f"ws://{MASTER_SERVER_IP}:{MASTER_SERVER_PORT}"
    
    while True: # Koparsa otomatik tekrar bağlansın
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
                            duration = data.get("duration")
                            
                            # Eğer zaten çalışan varsa kapat
                            if active_process:
                                try:
                                    active_process.terminate()
                                except:
                                    pass
                            
                            script_path = os.path.join(os.path.dirname(__file__), "kick.py")
                            if not os.path.exists(script_path):
                                print(f"❌ HATA: {script_path} dosyası bulunamadı!")
                                continue
                                
                            print(f"🚀 KICK BAŞLADI: Kanal={channel}, İzleyici={viewers}, Süre={duration}")
                            
                            # kick.py'yi CLI argümanlarıyla başlat
                            active_process = await asyncio.create_subprocess_exec(
                                sys.executable, script_path, '--json', '--channel', channel, '--threads', str(viewers),
                                stdout=asyncio.subprocess.DEVNULL, # Worker JSON okumaya ihtiyaç duymaz, arka planda sussun
                                stderr=asyncio.subprocess.DEVNULL
                            )
                            
                        elif action == "stop":
                            if active_process:
                                print("🛑 KICK DURDURULDU: Ana sunucudan stop emri geldi.")
                                try:
                                    active_process.terminate()
                                except:
                                    pass
                                active_process = None
                            else:
                                print("⚠️ Ana sunucudan stop emri geldi ama zaten çalışan işlem yok.")
                                
                    except json.JSONDecodeError:
                        print(f"⚠️ Geçersiz mesaj formatı: {message}")
                    except Exception as e:
                        print(f"❌ Mesaj işleme hatası: {e}")
                        
        except websockets.exceptions.ConnectionClosedError:
            print("❌ Bağlantı koptu. 5 saniye sonra tekrar denenecek...")
            await asyncio.sleep(5)
        except ConnectionRefusedError:
            print("❌ Ana sunucuya ulaşılamıyor (Bağlantı red). 5 saniye sonra tekrar denenecek...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ Beklenmeyen hata: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("-" * 50)
    print("🚀 404 LOG SYSTEM - KICK WORKER NODE 🚀")
    print("-" * 50)
    try:
        asyncio.run(handler())
    except KeyboardInterrupt:
        if active_process:
            try:
                active_process.terminate()
            except:
                pass
        print("Durduruldu.")
