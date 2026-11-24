
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

## Version sécurisée du Streaming

Le script streaming/prepare_daily_stream_secure.py
permet de publier les résultats journaliers sous forme
de snapshots HDFS.

Chaque micro-lot écrit d'abord dans un dossier temporaire,
puis publie le snapshot une fois l'écriture terminée.

### Résultats obtenus

- 37 200 observations Kafka
- 50 villes
- 1 550 journées-villes
- 16 snapshots produits
- 0 doublon ville/date
- 0 valeur NULL
- Stockage HDFS HEALTHY

### Chemins HDFS

Snapshots :
/data/meteo/streaming_lot3/snapshots_lot4

Checkpoint :
/data/meteo/streaming_lot3/checkpoint_snapshots

Dernier snapshot validé pour janvier 2025 :
/data/meteo/streaming_lot3/snapshots_lot4/batch_00015

### Utilisation par le lot 4

Le lot 4 doit lire un snapshot complet, et non
le dossier parent contenant toutes les versions.

Pour la démonstration de janvier 2025, il peut utiliser
batch_00015, qui contient 1 550 journées-villes.

Le dossier HDFS est local à notre infrastructure Docker.
Les autres membres doivent disposer du même environnement
ou transférer les données nécessaires.

### Vérification

Le script streaming/verify_secure_snapshot.py vérifie
le nombre de journées, les doublons, les valeurs NULL
et les colonnes attendues.

Le redémarrage avec checkpoint a été testé sans
nouveaux messages Kafka.