#!/bin/bash
# A lancer depuis la racine du repo, une fois le lot 1 execute
# (dataset present dans HDFS : /data/meteo/ml/dataset_journalier).
set -e
export MSYS_NO_PATHCONV=1

DS=/data/meteo/ml/dataset_journalier
SNAP=/data/meteo/streaming_lot3/snapshots_lot4/batch_00015

echo "--- Verification HDFS ---"
docker exec hadoop-namenode hdfs dfs -ls $DS

echo "--- Copie du dataset HDFS -> ml/data ---"
rm -rf ml/data/dataset_journalier
mkdir -p ml/data
docker exec hadoop-namenode bash -c "rm -rf /tmp/ds && hdfs dfs -get $DS /tmp/ds"
docker cp hadoop-namenode:/tmp/ds ml/data/dataset_journalier

STREAM_ARG=""
if docker exec hadoop-namenode hdfs dfs -test -d $SNAP 2>/dev/null; then
  echo "--- Copie du snapshot streaming (lot 3) ---"
  rm -rf ml/data/snapshot
  docker exec hadoop-namenode bash -c "rm -rf /tmp/snap && hdfs dfs -get $SNAP /tmp/snap"
  docker cp hadoop-namenode:/tmp/snap ml/data/snapshot
  STREAM_ARG="--stream ml/data/snapshot"
fi

echo "--- Entrainement ---"
python ml/train_model.py --data ml/data/dataset_journalier --out ml/models $STREAM_ARG
