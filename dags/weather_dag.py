from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

def fetch_and_store_weather():
    # Grille météo couvrant la zone de vol (Europe de l'Ouest)
    locations = [
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "Marseille", "lat": 43.2965, "lon": 5.3698},
        {"name": "Bordeaux", "lat": 44.8378, "lon": -0.5792},
        {"name": "Strasbourg", "lat": 48.5734, "lon": 7.7521},
        {"name": "Londres", "lat": 51.5074, "lon": -0.1278},
        {"name": "Bruxelles", "lat": 50.8503, "lon": 4.3517},
        {"name": "Genève", "lat": 46.2044, "lon": 6.1432},
        {"name": "Barcelone", "lat": 41.3851, "lon": 2.1734}
    ]
    
    hook = PostgresHook(postgres_conn_id='postgres_weather')
    
    # On vide la table de staging avant d'insérer les nouvelles météos (pour éviter les doublons)
    hook.run("TRUNCATE TABLE weather_staging;")

    for loc in locations:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": loc['lat'], "longitude": loc['lon'],
            "current": "temperature_2m,wind_speed_10m,wind_direction_10m",
            "timezone": "auto"
        }
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()['current']
            
            insert_sql = """
            INSERT INTO weather_staging 
            (latitude, longitude, temperature, wind_speed, wind_direction, observation_time, geom)
            VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));
            """
            hook.run(insert_sql, parameters=(
                loc['lat'], loc['lon'], data['temperature_2m'], data['wind_speed_10m'],
                data['wind_direction_10m'], data['time'], loc['lon'], loc['lat']
            ))
            print(f"Météo insérée pour {loc['name']} : {data['wind_speed_10m']} km/h de vent")

with DAG(
    dag_id='weather_ingestion_batch',
    start_date=datetime(2026, 6, 16),
    schedule_interval='@hourly',
    catchup=False,
    tags=['projet_final', 'batch']
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_and_store_grid',
        python_callable=fetch_and_store_weather
    )