import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# On charge le fichier .env caché
load_dotenv()

# Configuration de notre tapis roulant Kafka
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'raw_flights'

# On récupère les identifiants OPENSKY en toute sécurité
USERNAME = os.getenv("OPENSKY_USERNAME")
PASSWORD = os.getenv("OPENSKY_PASSWORD")

def create_producer():
    # On force l'utilisation de 127.0.0.1 et on fixe l'api_version pour éviter le bug de connexion
    KAFKA_BROKER = '127.0.0.1:9092'
    
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 5, 0) 
    )

def fetch_and_send_flights(producer):
    url = "https://opensky-network.org/api/states/all"
    params = {"lamin": 41.0, "lomin": -5.0, "lamax": 51.0, "lomax": 9.0}

    print(f"Début du streaming vers le topic Kafka '{TOPIC_NAME}'...")

    while True: # Boucle infinie pour le streaming
        try:
            response = requests.get(url, params=params, auth=(USERNAME, PASSWORD))
            
            if response.status_code == 200:
                data = response.json()
                states = data.get('states', [])
                
                if states:
                    print(f"[{time.strftime('%H:%M:%S')}] {len(states)} avions captés. Envoi sur le tapis Kafka...")
                    
                    for state in states:
                        # On nettoie et structure la donnée avant de l'envoyer
                        flight_data = {
                            "icao24": state[0],
                            "callsign": state[1].strip() if state[1] else "UNKNOWN",
                            "longitude": state[5],
                            "latitude": state[6],
                            "altitude": state[7],
                            "velocity": state[9],
                            "true_track": state[10] # Le cap de l'avion
                        }
                        
                        # On n'envoie que les avions qui ont des coordonnées GPS valides
                        if flight_data["longitude"] is not None and flight_data["latitude"] is not None:
                            producer.send(TOPIC_NAME, flight_data)
                    
                    # On force Kafka à envoyer tout ce qu'il a dans sa mémoire tampon
                    producer.flush() 
                
            elif response.status_code == 429:
                print("Limite d'API atteinte, pause de 30 secondes pour respirer...")
                time.sleep(30)
            else:
                print(f"Erreur API: {response.status_code}")

        except Exception as e:
            print(f"Erreur critique : {e}")

        # OpenSky limite les comptes gratuits à une requête toutes les 10 secondes.
        # On met le script en pause 10s avant de recommencer la boucle.
        time.sleep(60)

if __name__ == "__main__":
    my_producer = create_producer()
    fetch_and_send_flights(my_producer)