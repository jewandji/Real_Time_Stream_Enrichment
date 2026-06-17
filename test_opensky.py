import requests
import json
from dotenv import load_dotenv

def test_opensky_api():
    # On récupère les identifiants OPENSKY en toute sécurité
    USERNAME = os.getenv("OPENSKY_USERNAME")
    PASSWORD = os.getenv("OPENSKY_PASSWORD")
    
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": 41.0,
        "lomin": -5.0,
        "lamax": 51.0,
        "lomax": 9.0
    }
    
    print("Appel de l'API OpenSky avec authentification en cours...")
    
    # Ajout du paramètre auth=(USERNAME, PASSWORD)
    response = requests.get(url, params=params, auth=(USERNAME, PASSWORD))
    
    if response.status_code == 200:
        data = response.json()
        states = data.get('states', [])
        
        if states:
            print(f"Succès ! {len(states)} avions trouvés dans la zone.")
            premier_avion = states[0]
            print(json.dumps(premier_avion, indent=4))
        else:
            print("Aucun avion trouvé dans cette zone pour le moment.")
    elif response.status_code == 429:
         print("Erreur 429 : Toujours bloqué par la limite de requêtes. Attends quelques minutes.")
    elif response.status_code == 401:
         print("Erreur 401 : Identifiants incorrects.")
    else:
        print(f"Erreur API : {response.status_code}")

if __name__ == "__main__":
    test_opensky_api()