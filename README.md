# Smart Agriculture Big Data

## Présentation

Ce projet vise à mettre en place une architecture Big Data pour l'agriculture intelligente au Maroc.

Cette infrastructure permet de collecter, traiter, stocker et analyser des données météorologiques à l'aide de technologies Big Data distribuées.

## Architecture technique

L'infrastructure repose sur les composants suivants :

- **Docker Compose** : orchestration des conteneurs.
- **Apache Hadoop HDFS** : stockage distribué.
- **Apache Kafka** : ingestion des flux de données.
- **Apache Spark** : traitement batch et streaming.
- **Apache Parquet** : stockage optimisé des données analytiques.

### Architecture déployée

- Hadoop : 1 NameNode et 2 DataNodes.
- Kafka : 1 broker en mode KRaft.
- Spark : 1 Master et 2 Workers.

## Prérequis

- Git
- Docker Desktop
- Docker Compose v2
- Python 3.11
- Windows, Linux ou macOS compatible avec Docker

## Installation

### 1. Récupérer le projet

```bash
git clone https://github.com/Mariem123-git/-smart-agriculture-bigdata.git smart-agriculture-bigdata
cd smart-agriculture-bigdata
```

### 2. Démarrer l'infrastructure

```bash
cd infra
docker compose up -d
```

### 3. Vérifier les conteneurs

```bash
docker compose ps
```

## Interfaces Web

| Service | Adresse |
|---|---|
| Hadoop NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |

## Kafka

Le broker Kafka est accessible :

- Depuis les conteneurs : `kafka:9092`
- Depuis la machine hôte : `localhost:9094`

Le topic utilisé pour les données météorologiques est `meteo.hourly` (3 partitions, clé = `region_id`).

## Données

- **Source** : API Open-Meteo, données météo horaires historiques (licence CC BY 4.0).
- **Couverture** : 50 villes du Maroc, du 2015-01-01 au 2025-12-31.
- **Volume** : 4 821 600 événements (une ligne par ville et par heure), 15 champs dans Kafka.
- **Cible de prédiction** : l'ET₀ du lendemain (mm/jour), somme des 24 valeurs horaires du jour suivant.
- **Dictionnaire de données** : [`docs/dictionnaire_donnees.md`](docs/dictionnaire_donnees.md)
- **Valeurs de référence pour vérifier les résultats Spark** : [`docs/valeurs_reference.md`](docs/valeurs_reference.md)

Les fichiers de données (dossier `data/`) ne sont pas versionnés : ils sont trop volumineux et ignorés par Git.

## Archivage Kafka vers HDFS (voie froide)

Cette partie assure l'archivage des données météorologiques depuis Apache Kafka vers Hadoop HDFS à l'aide de Spark Structured Streaming.

### Architecture de l'archivage

Le traitement repose sur les composants suivants :

- **Apache Kafka** : source des événements météorologiques.
- **Apache Spark Structured Streaming** : lecture et traitement des messages Kafka.
- **Hadoop HDFS** : stockage distribué des données.
- **Parquet + Snappy** : format de stockage compressé et optimisé pour les analyses.

### Script d'archivage

Le script principal est :

`batch/archive_job/kafka_to_hdfs.py`

Il réalise les opérations suivantes :

1. Connexion au broker Kafka.
2. Lecture des messages du topic `meteo.daily`.
3. Désérialisation des événements JSON selon un schéma défini.
4. Filtrage des événements invalides.
5. Extraction des informations temporelles (année et mois).
6. Écriture des données dans HDFS au format Parquet.
7. Partitionnement des données par `region_id`.

### Lancement de l'archivage

Depuis la racine du dépôt, une fois l'infrastructure démarrée et le producteur terminé :

```bash
bash batch/archive_job/run_archive.sh
```

Le script :

1. Prépare les répertoires HDFS.
2. Copie les scripts nécessaires dans le conteneur Spark.
3. Lance l'archivage Kafka vers HDFS.
4. Exécute les traitements batch d'indicateurs.
5. Affiche les vérifications du stockage et de la réplication HDFS.

Variable optionnelle :

| Variable | Rôle |
|---|---|
| `MAX_OFFSETS` | Nombre maximal de messages par micro-batch (200000 par défaut) |

### Stockage HDFS

Les données sont organisées dans les répertoires suivants :

| Contenu | Chemin HDFS |
|---|---|
| Données Parquet | `/data/meteo/parquet` |
| Checkpoint Spark | `/data/meteo/checkpoint` |
| Indicateurs ET₀ mensuels | `/data/meteo/indicators/et0_mensuel` |
| Indicateurs de pluie annuelle | `/data/meteo/indicators/pluie_annuelle` |

Les fichiers sont stockés au format Parquet avec compression Snappy et partitionnés par région (`region_id`).

La réplication HDFS est configurée à **2**, permettant de répartir les blocs de données sur les deux DataNodes.

### Gestion du checkpoint

Le checkpoint Spark est enregistré dans :

`/data/meteo/checkpoint`

Il conserve les informations de progression du traitement, notamment les offsets Kafka déjà traités.

Il permet à Spark de reprendre le traitement avec le même checkpoint après une interruption, en évitant de retraiter les messages déjà validés.

### Vérification du stockage HDFS

Les commandes suivantes permettent de contrôler les données archivées :

**Vérifier les répertoires :**

```bash
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/parquet
```

**Vérifier le volume de stockage :**

```bash
docker exec hadoop-namenode hdfs dfs -du -h /data/meteo
```

**Compter les fichiers et répertoires :**

```bash
docker exec hadoop-namenode hdfs dfs -count -q -h /data/meteo/parquet
```

**Vérifier la réplication :**

```bash
docker exec hadoop-namenode hdfs fsck /data/meteo/parquet
```

### Résultats de l'archivage

L'archivage et les contrôles de stockage ont permis d'obtenir les résultats suivants :

| Indicateur | Résultat |
|---|---:|
| Messages Kafka | 4 821 600 |
| Lignes archivées dans HDFS | 4 821 600 |
| Identifiants `event_id` uniques | 4 821 600 |
| Doublons | 0 |
| Espace occupé dans HDFS | 137,5 MB |
| Format de stockage | Parquet |
| Compression | Snappy |
| Partitionnement | `region_id` |
| Réplication HDFS | 2 |

Les résultats confirment la correspondance entre le volume de données produit et le volume archivé dans HDFS, ainsi que l'unicité des identifiants des événements.

## Arrêt de l'infrastructure

```bash
docker compose down
```

Pour conserver les volumes et les données, ne pas utiliser :

```bash
docker compose down -v
```

## Équipe

Projet académique réalisé dans le cadre du module Big Data — ESI.

## Ingestion (producteur Kafka)

Le producteur lit `data/meteo_complet.jsonl`, valide chaque événement, ajoute `ingest_ts` et publie les données dans le topic `meteo.hourly` (clé = `region_id`).

### Prérequis

- Docker démarré (`cd infra && docker compose up -d`).
- Python 3.11 et `python -m pip install -r ingestion/requirements.txt`.
- Fichier de données dans `data/` et `.env` copié depuis `.env.example`.

### Lancement

```powershell
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic meteo.hourly --partitions 3 --replication-factor 1

python ingestion/producer.py

python ingestion/verify_ingestion.py
```

### Variables

| Variable | Rôle |
|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9094` depuis l'hôte, `kafka:9092` depuis Docker |
| `KAFKA_TOPIC` | Topic cible (`meteo.hourly`) |
| `INPUT_FILE` | Fichier source (.jsonl, .csv ou .parquet) |
| `DELAY` | Secondes par heure simulée (0 = chargement massif) |
| `LIMIT` | Nombre maximal de lignes (0 = tout) |

Les lignes invalides sont écrites dans `ingestion/rejects/rejected.jsonl`, et le bilan d'envoi dans `ingestion/reports/send_report.json`.