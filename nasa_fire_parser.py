import requests
import json
import os
from datetime import datetime

NASA_API_KEY = "c2b0a23afc43bb3fdab09c4eb2af2ca9"
BURYATIA_BBOX = "98.0,49.0,116.0,57.0"
SOURCE = "VIIRS_SNPP_NRT"
OUTPUT_FILE = "fires_data.json"

def fetch_nasa_fires():
    url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{NASA_API_KEY}/{SOURCE}/{BURYATIA_BBOX}/1"
    print(f"📡 Запрос к NASA...")
    
    response = requests.get(url, timeout=30)
    
    if response.status_code == 200:
        # Разбираем CSV вручную (без pandas, чтобы не было проблем)
        lines = response.text.strip().split('\n')
        if len(lines) <= 1:
            print("🌿 Пожаров не обнаружено")
            return []
        
        # Парсим строки (пропускаем заголовок)
        fires = []
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) >= 14:
                fires.append({
                    'latitude': float(parts[0]),
                    'longitude': float(parts[1]),
                    'acq_date': parts[6],
                    'acq_time': parts[7],
                    'frp': float(parts[13])
                })
        print(f"✅ Найдено {len(fires)} термоточек")
        return fires
    else:
        print(f"❌ Ошибка NASA: {response.status_code}")
        return []

def save_to_json(fires):
    # Форматируем для сайта
    formatted = []
    for i, fire in enumerate(fires[:50]):
        frp = fire.get('frp', 0)
        if frp > 100:
            intensity = "КРИТИЧЕСКИЙ"
        elif frp > 30:
            intensity = "СИЛЬНЫЙ"
        else:
            intensity = "СЛАБЫЙ"
        
        formatted.append({
            "id": i,
            "title": f"{intensity} пожар в районе {fire['latitude']:.2f}, {fire['longitude']:.2f}",
            "description": f"Интенсивность: {frp:.1f} МВт. Координаты: {fire['latitude']:.4f}, {fire['longitude']:.4f}",
            "link": f"https://firms.modaps.eosdis.nasa.gov/map/#d={fire['acq_date']};l=fire;x={fire['longitude']};y={fire['latitude']}",
            "source": "NASA FIRMS",
            "date": fire['acq_date'],
            "category": "лес" if frp > 30 else "происшествие",
            "rawDate": f"{fire['acq_date']}T{str(fire['acq_time']).zfill(4)}:00",
            "active": True,
            "latitude": fire['latitude'],
            "longitude": fire['longitude'],
            "frp": frp
        })
    
    output = {
        "metadata": {
            "last_update": datetime.now().isoformat(),
            "source": "NASA FIRMS",
            "total_fires": len(formatted),
            "message": f"Обновлено {datetime.now().strftime('%d.%m.%Y %H:%M')}"
        },
        "fires": formatted
    }
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Сохранено {len(formatted)} пожаров в {OUTPUT_FILE}")
    return output

def main():
    print("="*50)
    print("🛰️  МОНИТОРИНГ ПОЖАРОВ | БУРЯТИЯ")
    print("="*50)
    
    fires = fetch_nasa_fires()
    save_to_json(fires)
    
    if len(fires) == 0:
        print("\n🌿 Активных пожаров в Бурятии не обнаружено.")
        print("Сайт показывает корректное сообщение об отсутствии данных.")

if __name__ == "__main__":
    main()
