import requests
import json
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
        lines = response.text.strip().split('\n')
        if len(lines) <= 1:
            print("🌿 Пожаров не обнаружено")
            return []
        
        fires = []
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) >= 14:
                try:
                    fires.append({
                        'latitude': float(parts[0]),
                        'longitude': float(parts[1]),
                        'acq_date': parts[6],
                        'acq_time': parts[7],
                        'frp': float(parts[13])
                    })
                except:
                    pass
        print(f"✅ Найдено {len(fires)} термоточек")
        return fires
    else:
        print(f"❌ Ошибка NASA: {response.status_code}")
        return []

def save_to_json(fires):
    formatted = []
    for i, fire in enumerate(fires[:50]):
        frp = fire.get('frp', 0)
        if frp > 100:
            intensity = "КРИТИЧЕСКИЙ"
            category = "лес"
        elif frp > 30:
            intensity = "СИЛЬНЫЙ"
            category = "лес"
        else:
            intensity = "СЛАБЫЙ"
            category = "происшествие"
        
        formatted.append({
            "id": i,
            "title": f"{intensity} пожар",
            "description": f"Интенсивность: {frp:.1f} МВт. Координаты: {fire['latitude']:.4f}, {fire['longitude']:.4f}",
            "link": f"https://firms.modaps.eosdis.nasa.gov/map/#d={fire['acq_date']};l=fire;x={fire['longitude']};y={fire['latitude']}",
            "source": "NASA FIRMS",
            "date": fire['acq_date'],
            "category": category,
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
    
    print(f"💾 Сохранено {len(formatted)} пожаров")

def main():
    print("="*50)
    print("🛰️ МОНИТОРИНГ ПОЖАРОВ | БУРЯТИЯ")
    print("="*50)
    fires = fetch_nasa_fires()
    save_to_json(fires)

if __name__ == "__main__":
    main()
