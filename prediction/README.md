# Lot 4 — Prédictions en streaming et analyse

## Rôle dans le pipeline

```
Kafka (meteo.prediction)         lot 3 : producteur, janvier 2025
   │
   ▼
Spark Structured Streaming ──►   agrégation heures → jour par ville (même schéma que lot 1 / lot 3)
   │
   ▼
PipelineModel (lot 2, HDFS)  ──►  prédiction ET0(J+1), modèle chargé UNE fois au démarrage
   │
   ▼
HDFS /data/meteo/prediction_lot4/snapshots/batch_XXXXX   (Parquet, publication atomique)
   │
   ▼
analyse_predictions.py       ──►  comparaison avec les valeurs réelles historiques, indicateurs, délai
```

| Fichier | Rôle |
|---|---|
| `predict_stream.py` | Job Structured Streaming : Kafka → journalier → modèle → snapshots de prédictions |
| `run_predict_stream.sh` | Lance le job sur le cluster Spark (master + 2 workers) |
| `analyse_predictions.py` | Compare prédictions et réel, calcule MAE/RMSE/R², erreurs par ville et par jour, délai |
| `run_analyse.sh` | Lance l'analyse et exporte les preuves dans `docs/preuves_lot4/` |

## Choix de conception

- **Source Kafka** : conformément à la fiche de projet, le job de prédiction lit directement le topic Kafka. Il reprend la même agrégation que `streaming/prepare_daily_stream.py` (lot 3) : mêmes colonnes, mêmes noms, mêmes règles de journée complète (24 heures). Le lot 3 reste utilisable seul. Le mode `--source snapshot` permet de rejouer `batch_00015` du lot 3 sans Kafka.
- **Pas de fuite d'information** : le job n'utilise que les observations du jour J. `et0_j_plus_1` (réel) n'est lue que par `analyse_predictions.py`, après l'inférence.
- **Mode `update`** : une journée-ville est prédite une seule fois, quand sa 24ᵉ heure arrive. Le délai mesuré est donc celui de la première prédiction.
- **Reprise sur panne** : checkpoint dans `/data/meteo/prediction_lot4/checkpoint`. Chaque micro-lot est écrit dans un dossier temporaire puis renommé ; un micro-lot rejoué après redémarrage est ignoré, sans doublon.
- **Compatibilité modèle** : le job lit les colonnes attendues dans le `VectorAssembler` du modèle et s'arrête avec un message clair si le streaming ne peut pas les fournir (par exemple si le lot 2 a été réentraîné avec `--with-lags`).

## Prérequis : reproduire les sorties des lots 1 à 3

Le HDFS est local à chaque machine : après un `git pull`, les données des autres lots ne sont **pas** chez vous. Il faut les régénérer, dans cet ordre, depuis la racine du dépôt.

```bash
# 0. Infra + archive horaire (déjà faite chez vous)
cd infra && docker compose up -d && cd ..

# 1. Lot 1 : dataset journalier (train 2015-2023, val 2024, test 2025)
bash ml/run_prepare_dataset.sh

# 2. Lot 2 : entraînement, modèle sauvegardé dans /data/meteo/ml/model_et0
bash ml/run_train.sh
```

Vérification :

```bash
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/ml
# attendu : dataset_journalier, model_et0, resultats_evaluation, predictions_test
```

## Démonstration (lot 3 + lot 4)

**Terminal 1** — créer le topic et lancer le job de prédiction (il attend les données) :

```bash
docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic meteo.prediction --partitions 3 --replication-factor 1

RESET=1 bash prediction/run_predict_stream.sh
```

**Terminal 2** — publier janvier 2025 dans Kafka avec le producteur du lot 3. Adapter `INPUT_FILE` au fichier produit par `ingestion/prepare_streaming_sample.py` :

```bash
KAFKA_TOPIC=meteo.prediction INPUT_FILE=<fichier_janvier_2025.jsonl> DELAY=0.1 python ingestion/producer.py
```

`DELAY` est la pause en secondes par heure simulée : 744 heures × 0,1 s ≈ 75 s de flux visible. Dans le terminal 1, une ligne `Micro-lot N : … prédictions` apparaît à chaque micro-lot ayant terminé des journées. Le job s'arrête seul après `IDLE_TIMEOUT` secondes (120 par défaut) sans nouvelle donnée.

**Après l'arrêt** — analyse et preuves :

```bash
bash prediction/run_analyse.sh
```

### Test de reprise (à montrer dans la vidéo)

1. Pendant le flux (terminal 2 actif), arrêter le job : `docker restart spark-master` (le driver du job tourne dans ce conteneur).
2. Relancer **sans** `RESET=1` : `bash prediction/run_predict_stream.sh`.
3. Vérifier dans les logs que le job reprend après le dernier offset validé et que `run_analyse.sh` affiche `doublons ville/jour : 0`.

### Test sans Kafka

```bash
SOURCE=snapshot bash prediction/run_predict_stream.sh
```

## Résultats produits

HDFS : `/data/meteo/prediction_lot4/`

| Contenu | Chemin |
|---|---|
| Snapshots de prédictions (Parquet) | `snapshots/batch_XXXXX` |
| Checkpoint Spark | `checkpoint` |
| Résultats d'analyse (CSV) | `analyse/<nom>` |

Dépôt (`docs/preuves_lot4/`, générés par `run_analyse.sh`) :

| Fichier | Contenu |
|---|---|
| `predictions_et0.csv` | Prédictions ET₀(J+1) par ville et par date (sans valeur réelle) |
| `comparaison_reel.csv` | Prédiction, réel et erreur, calculés après coup |
| `metriques_globales.csv` | MAE, RMSE, R², biais, comparaison avec la persistance |
| `metriques_par_ville.csv` / `metriques_par_jour.csv` | Erreurs détaillées |
| `latence.csv` | Délai observé : dernière observation Kafka du jour → prédiction publiée |
| `analyse_lot4.txt` | Sortie console complète |

Colonnes de `predictions_et0.csv` : `region_id`, `date` (jour D observé), `date_cible` (D+1 prédit), `et0_jour`, `et0_predit_j_plus_1` (mm/jour), `modele`, `pred_ts`, `latence_s`.

## Limites connues

- Le flux ne couvre que janvier 2025 : l'évaluation porte sur 31 jours × 50 villes (1 550 prédictions) et reflète la saison hivernale, où l'ET₀ est faible.
- La journée du 31 janvier est prédite pour le 1ᵉʳ février ; son réel provient de l'historique (période test du lot 1).
- Le délai mesuré inclut l'intervalle de déclenchement (10 s par défaut), pas seulement le temps de calcul.
- Un événement Kafka dupliqué fausserait le comptage des 24 heures (la journée serait écartée). Le producteur Kafka est configuré en mode idempotent, ce qui limite ce risque sans l'exclure (par exemple si le producteur est relancé).
- Pas de watermark : l'état de l'agrégation grandit avec le nombre de journées. Sans conséquence pour un mois de données.
