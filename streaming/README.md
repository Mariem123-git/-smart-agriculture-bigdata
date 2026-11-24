
# Lot 3 — Kafka et Spark Structured Streaming

## Objectif

Simuler l'arrivée de nouvelles observations météorologiques
et les transformer en données journalières utilisables
pour prédire l'ET0 du lendemain (J+1).

## Données de simulation

- Période : janvier 2025
- Villes : 50
- Observations horaires : 37 200
- Journées-villes produites : 1 550

## Scripts

- ingestion/prepare_streaming_sample.py :
  extraction des observations de janvier 2025.

- ingestion/producer.py :
  envoi progressif des observations dans Kafka.

- streaming/prepare_daily_stream.py :
  lecture Kafka et agrégation journalière avec Spark.

- streaming/verify_daily_output.py :
  vérification des résultats Parquet.

## Configuration

Topic Kafka : meteo.prediction

Kafka depuis Windows : localhost:9094

Kafka depuis Spark : kafka:9092

Spark Master : spark://spark-master:7077

## Résultats HDFS

Données journalières :
/data/meteo/streaming_lot3/dataset_journalier

Checkpoint :
/data/meteo/streaming_lot3/checkpoint

Copie de test pour le lot 4 :
/data/meteo/streaming_lot3/dataset_journalier_lot4

## Validation

- 1 550 journées-villes
- 50 villes
- 0 doublon ville/date
- 0 valeur NULL
- Toutes les colonnes nécessaires présentes
- Stockage HDFS HEALTHY

## Intégration avec le lot 4

Le lot 4 utilise les données météorologiques du jour J
pour prédire l'ET0 du lendemain J+1.

Les variables journalières sont compatibles avec
le schéma défini par ml/prepare_dataset.py
(sans les variables de retard optionnelles).

La colonne et0_j_plus_1 n'est pas fournie par le
Streaming : il s'agit de la cible à prédire.

Important :
dataset_journalier_lot4 est une copie statique
créée pour les tests. Elle n'est pas actualisée
automatiquement et se trouve dans le HDFS local.

La sortie Streaming actuelle est réécrite à chaque
micro-lot. Une publication atomique ou une autre
méthode de sortie sécurisée sera nécessaire pour
une lecture continue par le lot 4.
