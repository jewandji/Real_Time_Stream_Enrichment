import json
import psycopg2
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = '127.0.0.1:9092'
TOPIC_NAME = 'raw_flights'

def create_postgres_connection():
    # Connexion à ta base locale (ouverte sur le port 5432 via Docker)
    return psycopg2.connect(
        host="localhost",
        database="aviation_weather",
        user="admin",
        password="password",
        port="5432"
    )

def setup_database(cursor):
    # Création de la table finale qui alimentera ton Dashboard
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS enriched_flights (
            icao24 VARCHAR(20) PRIMARY KEY,
            callsign VARCHAR(20),
            longitude FLOAT,
            latitude FLOAT,
            altitude FLOAT,
            velocity FLOAT,
            true_track FLOAT,
            weather_temp FLOAT,
            weather_wind_speed FLOAT,
            weather_wind_dir FLOAT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

def consume_and_enrich():
    print(f"Écoute du topic Kafka '{TOPIC_NAME}' en cours...")
    
    # 1. Connexion à Kafka
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        api_version=(2, 5, 0),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest' # On ne lit que les nouveaux messages
    )
    
    # 2. Connexion à PostgreSQL
    conn = create_postgres_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    setup_database(cursor)
    print("Base de données prête. En attente des avions...")

    # 3. Boucle d'écoute et d'enrichissement
    try:
        for message in consumer:
            flight = message.value
            
            # --- ÉTAPE D'ENRICHISSEMENT ---
            # On cherche la météo la plus proche grâce à PostGIS (<-> calcule la distance)
            # ST_MakePoint prend toujours (Longitude, Latitude)
            enrichment_query = """
                SELECT temperature, wind_speed, wind_direction
                FROM weather_staging
                ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                LIMIT 1;
            """
            cursor.execute(enrichment_query, (flight['longitude'], flight['latitude']))
            weather_data = cursor.fetchone()
            
            # Si on trouve de la météo, on fusionne
            if weather_data:
                temp, wind_speed, wind_dir = weather_data
                
                # --- SAUVEGARDE (SINK) ---
                # On utilise UPSERT (ON CONFLICT) pour mettre à jour l'avion s'il existe déjà
                insert_query = """
                    INSERT INTO enriched_flights 
                    (icao24, callsign, longitude, latitude, altitude, velocity, true_track, 
                     weather_temp, weather_wind_speed, weather_wind_dir, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (icao24) DO UPDATE SET
                        longitude = EXCLUDED.longitude,
                        latitude = EXCLUDED.latitude,
                        altitude = EXCLUDED.altitude,
                        velocity = EXCLUDED.velocity,
                        true_track = EXCLUDED.true_track,
                        weather_temp = EXCLUDED.weather_temp,
                        weather_wind_speed = EXCLUDED.weather_wind_speed,
                        weather_wind_dir = EXCLUDED.weather_wind_dir,
                        last_updated = CURRENT_TIMESTAMP;
                """
                cursor.execute(insert_query, (
                    flight['icao24'], flight['callsign'], flight['longitude'], flight['latitude'],
                    flight['altitude'], flight['velocity'], flight['true_track'],
                    temp, wind_speed, wind_dir
                ))
                print(f"Avion {flight['callsign']} enrichi avec succès (Vent: {wind_speed} km/h)")
            else:
                print(f"Pas de météo trouvée pour l'avion {flight['callsign']}")
                
    except KeyboardInterrupt:
        print("\nArrêt du consommateur demandé.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    consume_and_enrich()