"""
Парсер Telegram-каналов для мониторинга пожаров в Бурятии
Автоматически запускается на GitHub Actions каждый час
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError

# ============= НАСТРОЙКИ =============
# Каналы для мониторинга (публичные каналы Бурятии)
CHANNELS = [
    "mchsburyatia",      # МЧС Бурятии
    "gochs03",           # ГО и ЧС Бурятии
]

# Ключевые слова для фильтрации (только сообщения о пожарах)
KEYWORDS = [
    "пожар", "огонь", "возгорание", "горение", "пал", 
    "лес", "тайга", "степь", "трава", "сухая трава",
    "мчс", "чс", "спасатель", "огнеборц"
]

# Количество сообщений для проверки с канала
MESSAGES_LIMIT = 50

# Выходной файл
OUTPUT_FILE = "fires_data.json"

# Получаем API ключи из переменных окружения (установлены в GitHub Secrets)
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")

def is_fire_related(text: str) -> bool:
    """Проверяет, относится ли сообщение к пожарам"""
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in KEYWORDS)

def clean_text(text: str) -> str:
    """Очищает текст от лишних символов"""
    if not text:
        return ""
    # Удаляем лишние пробелы и переносы строк
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_category(text: str) -> str:
    """Определяет категорию пожара"""
    text_lower = text.lower()
    if "лес" in text_lower or "тайга" in text_lower or "лесной" in text_lower:
        return "лес"
    elif "мчс" in text_lower or "чс" in text_lower or "спас" in text_lower:
        return "чс"
    else:
        return "происшествие"

def get_title(text: str, channel: str) -> str:
    """Создаёт заголовок из текста сообщения"""
    # Берём первые 100 символов
    title = text[:100]
    if len(text) > 100:
        title = title.rsplit(' ', 1)[0] + "..."
    return f"🔥 {channel}: {title}"

async def parse_channel(client: TelegramClient, channel_username: str) -> List[Dict]:
    """Парсит один канал и возвращает список сообщений"""
    messages_data = []
    
    try:
        print(f"📡 Парсинг: {channel_username}")
        
        entity = await client.get_entity(channel_username)
        
        # Считаем сообщения за последние 3 дня
        since_date = datetime.now() - timedelta(days=3)
        
        async for message in client.iter_messages(
            entity, 
            limit=MESSAGES_LIMT,
            offset_date=since_date
        ):
            if not message.text:
                continue
            
            if not is_fire_related(message.text):
                continue
            
            clean_msg = clean_text(message.text)
            
            msg_data = {
                "id": message.id,
                "title": get_title(clean_msg, channel_username),
                "description": clean_msg[:500],
                "link": f"https://t.me/{channel_username}/{message.id}",
                "source": f"Telegram / {channel_username}",
                "date": message.date.strftime("%Y-%m-%d"),
                "category": get_category(clean_msg),
                "rawDate": message.date.isoformat(),
                "active": True
            }
            messages_data.append(msg_data)
            print(f"   ✅ Найдено сообщение: {msg_data['title'][:50]}...")
            
            # Небольшая задержка
            await asyncio.sleep(0.3)
            
    except FloodWaitError as e:
        print(f"   ⏳ Ожидание {e.seconds} секунд...")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        print(f"   ❌ Ошибка {channel_username}: {e}")
    
    return messages_data

async def load_existing_data() -> List[Dict]:
    """Загружает существующие данные, чтобы не дублировать"""
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def merge_and_deduplicate(new_messages: List[Dict], existing_messages: List[Dict]) -> List[Dict]:
    """Объединяет новые сообщения со старыми, удаляя дубликаты по ID"""
    existing_ids = {msg.get("id") for msg in existing_messages if msg.get("id")}
    unique_new = [msg for msg in new_messages if msg.get("id") not in existing_ids]
    
    # Объединяем и сортируем по дате
    all_messages = unique_new + existing_messages
    all_messages.sort(key=lambda x: x.get("rawDate", ""), reverse=True)
    
    # Оставляем только последние 100 сообщений
    return all_messages[:100]

async def main():
    print("\n" + "="*60)
    print("🔥 АВТОМАТИЧЕСКИЙ СБОР НОВОСТЕЙ О ПОЖАРАХ В БУРЯТИИ")
    print("="*60 + "\n")
    
    if API_ID == 0 or not API_HASH or not PHONE_NUMBER:
        print("❌ ОШИБКА: Не настроены API ключи Telegram!")
        print("\n📋 Добавьте секреты в GitHub:")
        print("   Settings → Secrets and variables → Actions")
        print("   Добавьте: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE")
        return
    
    print("🔐 Подключение к Telegram...")
    client = TelegramClient("fire_monitor_session", API_ID, API_HASH)
    await client.start(phone=PHONE_NUMBER)
    print("✅ Подключено!\n")
    
    # Загружаем существующие данные
    existing = await load_existing_data()
    print(f"📁 Существующих записей: {len(existing)}")
    
    # Собираем новые сообщения
    all_new_messages = []
    for channel in CHANNELS:
        messages = await parse_channel(client, channel)
        all_new_messages.extend(messages)
        await asyncio.sleep(1)
    
    print(f"\n📊 Найдено новых сообщений: {len(all_new_messages)}")
    
    # Объединяем и сохраняем
    merged = merge_and_deduplicate(all_new_messages, existing)
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Сохранено {len(merged)} сообщений в {OUTPUT_FILE}")
    
    await client.disconnect()
    print("\n✅ Готово!")

if __name__ == "__main__":
    asyncio.run(main())
