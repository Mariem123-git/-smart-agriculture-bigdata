#!/bin/bash
# Lot 2 : entrainement Spark MLlib. A lancer depuis la racine du repo, apres le lot 1.
set -e
export MSYS_NO_PATHCONV=1

echo "--- Dataset du lot 1 ---"
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/ml/dataset_journalier

# pyspark.ml a besoin de numpy (absent de l'image apache/spark) : installation sur les 3 noeuds
echo "--- numpy dans les conteneurs Spark ---"
for c in spark-master spark-worker1 spark-worker2; do
  docker exec -u root $c bash -c 'python3 -c "import numpy" 2>/dev/null || python3 -m pip install -q numpy || pip install -q numpy' || true
  docker exec $c python3 -c "import numpy; print('$c : numpy', numpy.__version__)"
done

docker cp ml/train_model.py spark-master:/tmp/train_model.py

echo "--- Entrainement ---"
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.dfs.replication=2 \
  --conf spark.sql.shuffle.partitions=16 \
  /tmp/train_model.py 2>&1 | grep -v -E " (INFO|WARN) "

echo "--- Sorties HDFS ---"
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/ml
docker exec hadoop-namenode hdfs dfs -cat '/data/meteo/ml/resultats_evaluation/part-*'

# copie locale des resultats pour le depot / la demo
mkdir -p ml/models
docker exec hadoop-namenode bash -c "hdfs dfs -cat '/data/meteo/ml/resultats_evaluation/part-*'" > ml/models/resultats_evaluation.json
echo "Resultats copies dans ml/models/resultats_evaluation.json"
