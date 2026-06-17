import requests
import json

def test_open_meteo_api():
    # Exemple de coordonnées GPS (ex: au-dessus de Paris)
    latitude = 48.8566
    longitude = 2.3522
    
    # On demande spécifiquement les données de vent (crucial pour ton use case)
    url = f"https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,wind_speed_10m,wind_direction_10m",
        "timezone": "auto"
    }
    
    print("Appel de l'API Open-Meteo en cours...")
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print("Succès ! Voici les données météo brutes :")
        print(json.dumps(data['current'], indent=4))
    else:
        print(f"Erreur API : {response.status_code}")

if __name__ == "__main__":
    test_open_meteo_api()