# Guide de Déploiement et d'Exécution (Setup)

Ce document décrit les étapes exactes pour initialiser l'infrastructure, configurer les bases de données et lancer le pipeline de traitement de bout en bout.

## Étape 1 : Clonage et Démarrage de l'Infrastructure

Clonez le dépôt sur votre machine locale et démarrez les conteneurs Docker (Kafka, Zookeeper, PostgreSQL/PostGIS, Airflow).

```bash
git clone https://github.com/jewandji/Real_Time_Stream_Enrichment.git
cd real_time_stream_enrichment
docker-compose up -d
```

## Étape 2 : Configuration des variables d'environnement (Sécurité)

Pour des raisons de sécurité, les identifiants d'accès à l'API OpenSky Network ont été sortis du code. Vous devez créer un fichier nommé `.env` à la racine du projet et y renseigner vos propres identifiants après avoir créé un compte gratuit sur https://opensky-network.org/ :

```env
OPENSKY_USERNAME=votre_nom_d_utilisateur
OPENSKY_PASSWORD=votre_mot_de_passe
```