# Smart Agriculture Big Data

## Présentation

Ce projet vise à mettre en place une architecture Big Data pour l'agriculture intelligente au Maroc.

Cette infrastructure permet de collecter, traiter et stocker des données météorologiques à l'aide de technologies Big Data distribuées.

## Architecture technique

L'infrastructure repose sur les composants suivants :

- **Docker Compose** : orchestration des conteneurs.
- **Apache Hadoop HDFS** : stockage distribué.
- **Apache Kafka** : ingestion des flux de données.
- **Apache Spark** : traitement batch et streaming.

### Architecture déployée

- Hadoop : 1 NameNode et 2 DataNodes.
- Kafka : 1 broker en mode KRaft.
- Spark : 1 Master et 2 Workers.

## Prérequis

- Git
- Docker Desktop
- Docker Compose v2
- Windows, Linux ou macOS compatible avec Docker

## Installation

### 1. Récupérer le projet

```bash
git clone https://github.com/mariem123-git/smart-agriculture-bigdata.git
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
- depuis les conteneurs : `kafka:9092`
- depuis la machine hôte : `localhost:9094`

Le topic utilisé pour les données météorologiques est `meteo.hourly` (3 partitions, clé = `region_id`).

## Archivage HDFS (voie froide)

Le script `batch/archive_job/kafka_to_hdfs.py` consomme le topic `meteo.hourly` avec Spark Structured Streaming et l'écrit dans HDFS en Parquet (Snappy), partitionné par `region_id`.

Lancement complet (depuis la racine du dépôt, une fois l'infra démarrée et le producteur terminé) :

```bash
bash batch/archive_job/run_archive.sh
```

Le script crée les dossiers HDFS, copie les jobs dans `spark-master`, lance l'archivage puis les indicateurs batch, et affiche les vérifications (réplication, `fsck`).

Variable optionnelle : `MAX_OFFSETS` (messages par micro-batch, 200000 par défaut).

## Stockage HDFS

| Contenu | Chemin HDFS |
|---|---|
| Données Parquet | `/data/meteo/parquet` |
| Checkpoint | `/data/meteo/checkpoint` |
| Indicateurs : ET₀ mensuel | `/data/meteo/indicators/et0_mensuel` |
| Indicateurs : pluie annuelle | `/data/meteo/indicators/pluie_annuelle` |

Le stockage est partitionné par région (`region_id`), au format Parquet avec compression Snappy, avec une réplication de 2 (2 DataNodes).

Le checkpoint permet à Spark de reprendre le traitement après une interruption, sans perte ni doublon.

## Arrêt de l'infrastructure

```bash
docker compose down
```

Pour conserver les volumes et les données, ne pas utiliser `docker compose down -v`.

## Équipe

Projet académique réalisé dans le cadre du module Big Data — ESI.

## Ingestion (producteur Kafka)

Le producteur lit `data/meteo_complet.jsonl`, valide chaque événement, ajoute `ingest_ts`
et publie dans le topic `meteo.hourly` (clé = `region_id`).

### Prérequis
- Docker démarré (`cd infra && docker compose up -d`)
- Python 3.11 et `python -m pip install -r ingestion/requirements.txt`
- Fichier de données dans `data/` et `.env` copié depuis `.env.example`

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
| `KAFKA_TOPIC` | topic cible (`meteo.hourly`) |
| `INPUT_FILE` | fichier source (.jsonl, .csv ou .parquet) |
| `DELAY` | secondes par heure simulée (0 = chargement massif) |
| `LIMIT` | nombre max de lignes (0 = tout) |

Les lignes invalides sont écrites dans `ingestion/rejects/rejected.jsonl`,
le bilan d'envoi dans `ingestion/reports/send_report.json`.
## Archivage HDFS (voie froide)

Le job `batch/archive_job/kafka_to_hdfs.py` lit le topic `meteo.hourly`, valide les messages JSON et les écrit dans HDFS au format Parquet (Snappy), partitionné par `region_id`.

- Sortie : `/data/meteo/parquet`
- Checkpoint : `/data/meteo/checkpoint` (relancer le job ne relit que les nouveaux messages)
- Réplication HDFS : 2 (vérifiée avec `hdfs fsck`)
- Indicateurs batch (`indicators.py`) : ET₀ moyen par région et par mois, pluie annuelle, température moyenne par région, écrits dans `/data/meteo/indicators`

Lancement (infra démarrée et topic rempli par le producteur) :

    bash batch/archive_job/run_archive.sh
    ## Archivage HDFS (voie froide)

Le job `batch/archive_job/kafka_to_hdfs.py` lit le topic `meteo.hourly`, valide les messages JSON et les écrit dans HDFS au format Parquet (Snappy), partitionné par `region_id`.

- Sortie : `/data/meteo/parquet`
- Checkpoint : `/data/meteo/checkpoint` (relancer le job ne relit que les nouveaux messages)
- Réplication HDFS : 2 (vérifiée avec `hdfs fsck`)
- Indicateurs batch (`indicators.py`) : ET₀ moyen par région et par mois, pluie annuelle, température moyenne par région, écrits dans `/data/meteo/indicators`

Lancement (infra démarrée et topic rempli par le producteur) :

    bash batch/archive_job/run_archive.sh