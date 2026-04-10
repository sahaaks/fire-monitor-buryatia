"""
Парсер спутниковых данных NASA FIRMS для мониторинга пожаров в Бурятии
Запускается автоматически на GitHub Actions каждый час
"""

import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict

# ============= НАСТРОЙКИ =============
# Ваш ключ NASA FIRMS
NASA_API_KEY = "c2b0a23afc43bb3fdab09c4eb2af2ca9"

# Координаты Бурятии (Запад, Юг, Восток, Север)
BURYATIA_BBOX = "98.0,49.0,116.0,57.0"

# Спутники: VIIRS_SNPP_NRT (самый чувствительный) или MODIS_NRT
SOURCE = "VIIRS_SNPP_NRT"

# Выходной файл для вашего сайта
OUTPUT_FILE = "fires_data.json"

def fetch_nasa_fires() -> List[Dict]:
    """Получает данные о пожарах из NASA FIRMS"""
    # Формируем URL для запроса CSV
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_API_KEY}/{SOURCE}/{BURYATIA_BBOX}/1"
    
    print(f"📡 Запрос к NASA FIRMS...")
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            # Сохраняем временный CSV
            temp_csv = "temp_fires.csv"
            with open(temp_csv, "wb") as f:
                f.write(response.content)
            
            # Читаем CSV через pandas
            df = pd.read_csv(temp_csv)
            os.remove(temp_csv)
            
            print(f"✅ Получено {len(df)} термоточек")
            return df.to_dict('records')
        else:
            print(f"❌ Ошибка NASA: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"⚠️ Ошибка запроса: {e}")
        return []

def format_for_website(fires: List[Dict]) -> List[Dict]:
    """Форматирует спутниковые данные для вашего сайта"""
    formatted = []
    
    for i, fire in enumerate(fires[:50]):  # Не более 50 точек
        # Определяем интенсивность пожара по FRP
        frp = fire.get('frp', 0)
        if frp > 100:
            intensity = "🔥 КРИТИЧЕСКИЙ"
            category = "лес"
        elif frp > 30:
            intensity = "⚠️ СИЛЬНЫЙ"
            category = "лес"
        elif frp > 10:
            intensity = "📍 СРЕДНИЙ"
            category = "происшествие"
        else:
            intensity = "🌿 СЛАБЫЙ"
            category = "происшествие"
        
        # Получаем дату
        acq_date = fire.get('acq_date', '')
        acq_time = fire.get('acq_time', '')
        if len(str(acq_time)) == 4:
            time_str = f"{str(acq_time)[:2]}:{str(acq_time)[2:]}"
        else:
            time_str = "00:00"
        
        # Формируем запись
        formatted.append({
            "id": 5000 + i,
            "title": f"{intensity} пожар в районе {fire.get('latitude', 0):.2f}, {fire.get('longitude', 0):.2f}",
            "description": f"Спутник зафиксировал термоточку. Интенсивность: {fire.get('frp', 0):.1f} МВт. "
                          f"Тип: {fire.get('type', 'неизвестно')}. "
                          f"Координаты: {fire.get('latitude', 0):.4f}, {fire.get('longitude', 0):.4f}",
            "link": f"https://firms.modaps.eosdis.nasa.gov/map/#d={acq_date};l=fire;x={fire.get('longitude', 0)};y={fire.get('latitude', 0)}",
            "source": "NASA FIRMS (спутник)",
            "date": acq_date,
            "category": category,
            "rawDate": f"{acq_date}T{time_str}:00",
            "active": True,
            "latitude": fire.get('latitude', 0),
            "longitude": fire.get('longitude', 0),
            "frp": fire.get('frp', 0)
        })
    
    return formatted

def save_to_json(data: List[Dict]):
    """Сохраняет данные в файл для сайта"""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 Сохранено {len(data)} пожаров в {OUTPUT_FILE}")

def main():
    print("\n" + "="*60)
    print("🛰️  СПУТНИКОВЫЙ МОНИТОРИНГ ПОЖАРОВ | БУРЯТИЯ")
    print("="*60 + "\n")
    
    # Получаем данные из NASA
    raw_fires = fetch_nasa_fires()
    
    if raw_fires:
        # Форматируем для сайта
        formatted_fires = format_for_website(raw_fires)
        
        # Добавляем информацию о времени обновления
        update_info = {
            "last_update": datetime.now().isoformat(),
            "source": "NASA FIRMS",
            "total_fires": len(formatted_fires),
            "message": f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        }
        
        # Сохраняем
        save_to_json({
            "metadata": update_info,
            "fires": formatted_fires
        })
        
        print(f"\n📊 Статистика:")
        print(f"   - Всего термоточек: {len(raw_fires)}")
        print(f"   - Отображено на сайте: {len(formatted_fires)}")
        
        critical = sum(1 for f in formatted_fires if f.get('frp', 0) > 100)
        strong = sum(1 for f in formatted_fires if 30 < f.get('frp', 0) <= 100)
        print(f"   - Критических пожаров (FRP > 100): {critical}")
        print(f"   - Сильных пожаров (FRP 30-100): {strong}")
        
    else:
        print("⚠️ Данные не получены. Создаю заглушку.")
        save_to_json({
            "metadata": {
                "last_update": datetime.now().isoformat(),
                "source": "NASA FIRMS",
                "total_fires": 0,
                "message": "Данные временно недоступны"
            },
            "fires": []
        })

if __name__ == "__main__":
    main()
