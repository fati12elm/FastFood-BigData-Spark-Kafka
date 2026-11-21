
# FastFood.ma — Projet Big Data 2

## Jalon 1 — Traitement Batch avec Apache Spark

Projet académique EMSI — 5e année

## Objectif

Mettre en place un environnement Big Data distribué
et analyser les événements de commandes FastFood.ma.

## Technologies

- Apache Spark 3.5.7
- Apache Kafka 3.9.1
- PySpark
- SparkCore RDD
- Spark SQL / DataFrames
- MinIO
- Apache Parquet
- Docker Compose
- Python 3.11
- JupyterLab

## Architecture

Docker Compose :
- 1 Kafka Broker
- 1 Spark Master
- 2 Spark Workers
- 1 MinIO
- 1 JupyterLab

## Données

- 50 000 événements de commandes
- 200 restaurants
- 4 villes marocaines

Les données utilisées sont synthétiques,
générées avec producer.py en l'absence
des fichiers fournis par l'enseignant.

## Partie 1 — Cluster

Spark Master : http://localhost:8080
MinIO : http://localhost:9001
JupyterLab : http://localhost:8888

Topic Kafka : orders
Partitions Kafka : 3

## Partie 2 — SparkCore RDD

Analyses :
- Chiffre d'affaires par ville
- Panier moyen par ville
- Top 5 restaurants
- Lineage RDD et DAG

Résultats :
- 50 000 événements analysés
- 28 947 événements DELIVERED
- 5 jobs Spark terminés
- Temps total mesuré : 12,2266 secondes

## Partie 3 — Spark SQL

Analyses :
- DataFrames avec StructType
- Requêtes Spark SQL
- Jointure commandes/restaurants
- Classification des distances
- Comparaison UDF et expressions natives
- Analyse Catalyst avec explain(True)
- Écriture Parquet partitionnée par ville

Résultats :
- 50 000 événements en entrée
- 49 006 événements après nettoyage
- 994 événements supprimés
- 0 référence restaurant manquante

## Stockage MinIO

Bronze :
s3a://lake/bronze/

Silver :
s3a://lake/silver/orders_history/

4 partitions :
Casablanca, Marrakech, Rabat, Tanger.

## Exécution

Démarrer le cluster :

docker compose up -d --build

Créer le topic :

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders --partitions 3 --replication-factor 1

Générer les données :

docker compose exec jupyter python /workspace/scripts/producer.py --mode file --n 50000

Les fichiers générés doivent ensuite être
envoyés dans la couche Bronze de MinIO.

Exécuter la Partie 2 :

docker compose exec jupyter python /workspace/scripts/partie2_rdd.py

Exécuter la Partie 3 :

docker compose exec jupyter python /workspace/scripts/partie3_spark_sql.py

## Livrables

- Scripts Python
- docker-compose.yml
- Dockerfile
- README.md
- Résultats expérimentaux
- Captures Spark UI et MinIO
- Rapport du Jalon 1
