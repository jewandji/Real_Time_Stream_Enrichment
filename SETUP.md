# Guide de Déploiement et d'Exécution (Setup)

Ce document décrit les étapes exactes pour initialiser l'infrastructure, configurer les bases de données et lancer le pipeline de traitement de bout en bout.

## Étape 1 : Clonage et Démarrage de l'Infrastructure

Clonez le dépôt sur votre machine locale et démarrez les conteneurs Docker (Kafka, Zookeeper, PostgreSQL/PostGIS, Airflow).

```bash
git clone https://github.com/jewandji/Real_Time_Stream_Enrichment.git
cd Real_Time_Stream_Enrichment
docker-compose up -d
```

*Note : Patientez environ 30 à 60 secondes pour permettre l'initialisation complète des services, en particulier Zookeeper et Kafka.*

## Étape 2 : Configuration des variables d'environnement (Sécurité)

Pour des raisons de sécurité, les identifiants d'accès à l'API OpenSky Network ont été sortis du code. Vous devez créer un fichier nommé `.env` à la racine du projet et y renseigner vos propres identifiants après avoir créé un compte gratuit sur https://opensky-network.org/ :

```env
OPENSKY_USERNAME=votre_nom_d_utilisateur
OPENSKY_PASSWORD=votre_mot_de_passe
```

## Étape 3 : Initialisation d'Apache Airflow

Airflow nécessite la configuration de sa base de données interne et la création d'un profil administrateur pour accéder à l'interface web.

```bash
docker-compose run airflow-webserver airflow db init
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

1. Accédez à l'interface Airflow via `http://localhost:8080`.  
2. Dans l'onglet **Admin > Connections**, créez une nouvelle connexion :  
   * **Connection Id :** `postgres_weather`  
   * **Connection Type :** `Postgres`  
   * **Host :** `postgres_db`  
   * **Schema :** `aviation_weather`  
   * **Login :** `admin`  
   * **Password :** `password`  
   * **Port :** `5432`

3. Activez (Unpause) et déclenchez manuellement le DAG `weather_ingestion_batch` pour injecter la grille météorologique de référence dans PostgreSQL.

## Étape 4 : Installation des Dépendances Python

Il est fortement recommandé d'utiliser un environnement virtuel (venv ou conda) pour l'exécution des scripts locaux.

```bash
pip install -r requirements.txt
```

## Étape 5 : Activation de l'Extension Spatiale PostGIS

Exécutez la commande suivante pour activer les fonctions géographiques (`ST_MakePoint`, opérateurs de distance) dans la base de données cible :

```bash
docker exec -it postgres_db psql -U admin -d aviation_weather -c "CREATE EXTENSION IF NOT EXISTS postgis;"
```

## Étape 6 : Lancement du Pipeline Temps Réel

Pour observer le flux en temps réel, ouvrez trois terminaux distincts à la racine du projet et exécutez les scripts suivants en parallèle. L'ordre d'exécution a son importance.

### **Terminal 1 : Le Consommateur (L'Enrichisseur)**  
Ce script doit être lancé en premier pour écouter le topic Kafka et préparer la table finale.

```bash
python streaming/opensky_consumer.py
```

### **Terminal 2 : Le Producteur (Ingestion)**  
Pour garantir un flux constant de données lors de la présentation sans atteindre les limites de requêtes (Rate Limiting) de l'API publique d'OpenSky, lancez le Mock Producer :

```bash
python streaming/opensky_mock_producer.py
```

### **Terminal 3 : Le Tableau de Bord (Visualisation)**  
Lancez l'interface utilisateur pour visualiser la jointure des données en temps quasi-réel.

```bash
python -m streamlit run dashboard/app.py
```

Le tableau de bord sera automatiquement accessible depuis votre navigateur à l'adresse `http://localhost:8501`.