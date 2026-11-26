#!/bin/bash
# Lot 4 : analyse des predictions + export des preuves (CSV et sortie console) dans docs/preuves_lot4/.
# A lancer depuis la racine du repo, une fois predict_stream.py termine.
set -e
export MSYS_NO_PATHCONV=1

mkdir -p docs/preuves_lot4
docker cp prediction/analyse_predictions.py spark-master:/tmp/analyse_predictions.py

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.hadoop.dfs.replication=2 \
  /tmp/analyse_predictions.py 2>&1 | grep -v -E " (INFO|WARN) " | tee docs/preuves_lot4/analyse_lot4.txt

echo "--- Export des resultats vers docs/preuves_lot4/ ---"
for name in predictions_et0 comparaison_reel metriques_globales metriques_par_ville metriques_par_jour latence; do
  docker exec hadoop-namenode bash -c "hdfs dfs -cat '/data/meteo/prediction_lot4/analyse/$name/part-*'" \
    > docs/preuves_lot4/$name.csv
  echo "docs/preuves_lot4/$name.csv"
done
