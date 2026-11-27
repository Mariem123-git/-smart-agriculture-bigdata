#!/bin/bash
# Lot 4 : job Spark Structured Streaming de prediction ET0 J+1.
# A lancer depuis la racine du repo, apres les lots 1 et 2 (modele dans HDFS).
#
# Variables optionnelles :
#   SOURCE=kafka|snapshot   kafka (defaut) = flux Kafka ; snapshot = rejoue le snapshot du lot 3
#   IDLE_TIMEOUT=120        arret propre apres N s sans nouvelle donnee (0 = tourne indefiniment)
#   RESET=1                 efface checkpoint + predictions avant de demarrer (nouvelle demo)
set -e
export MSYS_NO_PATHCONV=1

SOURCE=${SOURCE:-kafka}
IDLE_TIMEOUT=${IDLE_TIMEOUT:-120}

echo "--- Modele du lot 2 ---"
docker exec hadoop-namenode hdfs dfs -test -d /data/meteo/ml/model_et0 || {
  echo "Modele absent de HDFS : lancer d'abord ml/run_prepare_dataset.sh puis ml/run_train.sh"; exit 1; }
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/ml/model_et0

docker exec hadoop-namenode hdfs dfs -mkdir -p /data/meteo/prediction_lot4
if [ "${RESET:-0}" = "1" ]; then
  echo "--- RESET : suppression checkpoint et predictions ---"
  docker exec hadoop-namenode hdfs dfs -rm -r -f /data/meteo/prediction_lot4
  docker exec hadoop-namenode hdfs dfs -mkdir -p /data/meteo/prediction_lot4
fi
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data/meteo

# pyspark.ml a besoin de numpy (absent de l'image apache/spark) : installation sur les 3 noeuds
echo "--- numpy dans les conteneurs Spark ---"
for c in spark-master spark-worker1 spark-worker2; do
  docker exec -u root $c bash -c 'python3 -c "import numpy" 2>/dev/null || python3 -m pip install -q numpy || pip install -q numpy' || true
  docker exec $c python3 -c "import numpy; print('$c : numpy', numpy.__version__)"
done

docker cp prediction/predict_stream.py spark-master:/tmp/predict_stream.py

echo "--- Streaming de prediction (source=$SOURCE, arret apres ${IDLE_TIMEOUT}s d'inactivite) ---"
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.cores.max=2 \
  --conf spark.hadoop.dfs.replication=2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  /tmp/predict_stream.py --source "$SOURCE" --idle-timeout "$IDLE_TIMEOUT" 2>&1 \
  | grep -v -E " (INFO|WARN) " || true

echo "--- Snapshots de predictions publies ---"
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/prediction_lot4/snapshots
