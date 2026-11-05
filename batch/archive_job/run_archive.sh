#!/bin/bash
# A lancer depuis la racine du repo, une fois l'infra up et le producteur termine.
set -e
export MSYS_NO_PATHCONV=1

docker exec hadoop-namenode hdfs dfs -mkdir -p /data/meteo/parquet /data/meteo/checkpoint
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data/meteo

docker cp batch/archive_job/kafka_to_hdfs.py spark-master:/tmp/kafka_to_hdfs.py
docker cp batch/archive_job/indicators.py spark-master:/tmp/indicators.py

# 1) Archivage Kafka -> HDFS (Parquet Snappy, partitionne par region_id, replication 2)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.hadoop.dfs.replication=2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  /tmp/kafka_to_hdfs.py

# 2) Indicateurs batch (lecture du Parquet, ecriture des resultats, replication 2)
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.hadoop.dfs.replication=2 \
  /tmp/indicators.py

echo "--- Verification HDFS ---"
docker exec hadoop-namenode hdfs dfs -ls /data/meteo/parquet
docker exec hadoop-namenode hdfs dfs -du -h /data/meteo
docker exec hadoop-namenode hdfs fsck /data/meteo/parquet | grep -E "Average block replication|Under-replicated|Status" || true
docker exec hadoop-namenode hdfs fsck /data/meteo/parquet -files -blocks | grep -m 5 "repl=" || true