import json
import time
import random
from kafka import KafkaProducer

# Configuration de Kafka
KAFKA_BROKER = '127.0.0.1:9092'
TOPIC_NAME = 'raw_flights'

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0)
    )

def generate_fleet(num_planes=140):
    # Nos villes météo (servent de points de base pour la répartition)
    base_locations = [
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "Marseille", "lat": 43.2965, "lon": 5.3698},
        {"name": "Bordeaux", "lat": 44.8378, "lon": -0.5792},
        {"name": "Strasbourg", "lat": 48.5734, "lon": 7.7521},
        {"name": "Londres", "lat": 51.5074, "lon": -0.1278},
        {"name": "Bruxelles", "lat": 50.8503, "lon": 4.3517},
        {"name": "Genève", "lat": 46.2044, "lon": 6.1432},
        {"name": "Barcelone", "lat": 41.3851, "lon": 2.1734}
    ]
    
    compagnies = ["AFR", "EZY", "RYR", "BAW", "SWR", "DLH", "VLG", "IBE"]
    fleet = []
    
    for i in range(num_planes):
        loc = random.choice(base_locations)
        # On disperse les avions jusqu'à ~300km autour de la ville de base
        lat_offset = random.uniform(-3.0, 3.0) 
        lon_offset = random.uniform(-3.0, 3.0)
        
        flight = {
            "icao24": f"MOCK{i:03d}",
            "callsign": f"{random.choice(compagnies)}{random.randint(100, 999)}",
            "longitude": loc["lon"] + lon_offset,
            "latitude": loc["lat"] + lat_offset,
            "altitude": random.uniform(4000, 12000),
            "velocity": random.uniform(180.0, 280.0),
            "true_track": random.uniform(0.0, 360.0) # Direction aléatoire
        }
        fleet.append(flight)
        
    return fleet

def run_mock_producer(producer):
    print(f"🛠️ DÉMARRAGE DU SUPER MOCK PRODUCER vers '{TOPIC_NAME}'...")
    
    # Génération de notre flotte initiale de 140 avions
    mock_flights = generate_fleet(140)

    while True:
        try:
            print(f"[{time.strftime('%H:%M:%S')}] Envoi de {len(mock_flights)} avions sur le tapis Kafka...")
            
            for flight in mock_flights:
                # On simule un léger déplacement naturel basé sur leur cap (vitesse très réduite pour le visuel)
                # Astuce mathématique simple : on ajoute un peu de x et y pour les faire glisser sur la carte
                flight["longitude"] += random.uniform(-0.02, 0.02)
                flight["latitude"] += random.uniform(-0.02, 0.02)
                flight["altitude"] += random.uniform(-50, 50) # Turbulences
                
                producer.send(TOPIC_NAME, flight)
            
            producer.flush()
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\nArrêt du Super Mock Producer.")
            break

if __name__ == "__main__":
    my_producer = create_producer()
    run_mock_producer(my_producer)