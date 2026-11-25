#!/bin/bash
# Lot 1 : construit le dataset journalier a partir de l'archive horaire HDFS.
# A lancer depuis la racine du repo, une fois l'archivage termine.
set -e
export MSYS_NO_PATHCONV=1

docker exec hadoop-namenode hdfs dfsadmin -safemode wait
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/meteo/ml
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data/meteo

docker cp ml/prepare_dataset.py spark-master:/tmp/prepare_dataset.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.hadoop.dfs.replication=2 \
  /tmp/prepare_dataset.py

docker exec hadoop-namenode hdfs dfs -ls /data/meteo/ml/dataset_journalier
