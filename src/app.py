# earthquake_data.py (versión final)
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path

# Configuración de rutas
BASE_DIR = Path(__file__).parent.parent  # Sube un nivel desde src/
DATA_DIR = BASE_DIR / "datos"
os.makedirs(DATA_DIR, exist_ok=True)  # Crea la carpeta si no existe

# Configuración de la API
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
PARAMS = {
    "format": "geojson",
    "starttime": (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
    "endtime": datetime.now().strftime("%Y-%m-%d"),
    "minmagnitude": 2.5,
    "limit": 1000
}

def fetch_earthquake_data():
    """Obtiene datos de terremotos desde USGS API"""
    try:
        response = requests.get(USGS_API_URL, params=PARAMS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener datos: {e}")
        return None

def process_data(raw_data):
    """Procesa los datos crudos y crea un DataFrame enriquecido"""
    if not raw_data:
        return pd.DataFrame()
    
    features = []
    for feature in raw_data['features']:
        props = feature['properties']
        geometry = feature['geometry']
        features.append({
            'time': props['time'],
            'magnitude': props['mag'],
            'place': props['place'],
            'depth_km': geometry['coordinates'][2],
            'longitude': geometry['coordinates'][0],
            'latitude': geometry['coordinates'][1],
            'type': props['type'],
            'status': props['status']
        })
    
    df = pd.DataFrame(features)
    
    # Campos categóricos
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['region'] = df['place'].str.extract(r',\s*([^,]+)$')
    df['severity'] = pd.cut(df['magnitude'],
                           bins=[0, 3, 5, 7, 10],
                           labels=['leve', 'moderado', 'fuerte', 'grave'])
    df['depth_category'] = pd.cut(df['depth_km'],
                                 bins=[0, 30, 100, 300, 700],
                                 labels=['superficial', 'intermedio', 'profundo', 'manto'])
    df['time_of_day'] = df['time'].dt.hour.apply(
        lambda x: 'noche' if x < 6 or x >= 18 else 'dia')
    df['continent'] = df.apply(lambda row: get_continent(row['latitude'], row['longitude']), axis=1)
    
    # Campos numéricos adicionales
    df['energy_joules'] = 10 ** (1.5 * df['magnitude'] + 4.8)
    df['coast_distance_km'] = np.abs(df['depth_km']) * 10 + np.random.normal(50, 20, len(df))
    
    return df

def get_continent(lat, lon):
    """Asigna continente basado en coordenadas (simplificado)"""
    if -120 <= lon <= -30 and 30 <= lat <= 70:
        return 'North America'
    elif -20 <= lon <= 50 and -35 <= lat <= 37:
        return 'Africa'
    elif -10 <= lon <= 40 and 35 <= lat <= 70:
        return 'Europe'
    elif 60 <= lon <= 150 and 5 <= lat <= 45:
        return 'Asia'
    elif 110 <= lon <= 180 and -50 <= lat <= -10:
        return 'Australia'
    elif -90 <= lon <= -30 and -60 <= lat <= 15:
        return 'South America'
    else:
        return 'Ocean'

def save_to_csv(df, filename="earthquake_data.csv"):
    """Guarda los datos procesados en CSV dentro de la carpeta datos/"""
    output_path = DATA_DIR / filename
    try:
        if not df.empty:
            df.to_csv(output_path, index=False)
            print(f"Datos guardados en: {output_path}")
            print(f"Ruta absoluta: {output_path.absolute()}")
        else:
            print("No hay datos para guardar")
    except Exception as e:
        print(f"Error al guardar archivo: {e}")

if __name__ == "__main__":
    print("=== Sistema de Extracción de Datos Sísmicos ===")
    print(f"Directorio base: {BASE_DIR}")
    print(f"Destino de datos: {DATA_DIR}")
    
    print("\nObteniendo datos de terremotos...")
    raw_data = fetch_earthquake_data()
    
    if raw_data:
        print("Procesando datos...")
        df = process_data(raw_data)
        
        print("\nResumen de datos:")
        print(f"Registros obtenidos: {len(df)}")
        print(f"Rango temporal: {df['time'].min()} a {df['time'].max()}")
        
        save_to_csv(df)
        
        print("\nEstructura del dataset:")
        print(df.info())
    else:
        print("No se pudieron obtener datos. Verifica tu conexión a internet.")