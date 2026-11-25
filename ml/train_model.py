"""Lot 2 - Entrainement et evaluation ML avec Spark MLlib : ET0 du lendemain (J+1).

Entree  : dataset journalier du lot 1 (Parquet partitionne par `split`).
Modele  : Pipeline MLlib (VectorAssembler -> RandomForestRegressor), sauvegarde complete.
Compare : persistance (ET0(J+1) = ET0(J)), LinearRegression, RandomForest.
Metriques : MAE, RMSE (+ R2) sur validation et test.
Sorties HDFS (par defaut) :
  /data/meteo/ml/model_et0                 PipelineModel complet (a charger par le lot 4)
  /data/meteo/ml/resultats_evaluation      metriques (JSON)
  /data/meteo/ml/predictions_test          predictions sur 2025 (Parquet)
Lancement : voir ml/run_train.sh
"""
import argparse
import os

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")

# Contrat de donnees du lot 1 (sans variables de retard) : le lot 3 fournit les memes colonnes.
FEATURES = ["lat", "lon", "temp_moy", "temp_min", "temp_max", "humidite_moy",
            "vent_moy", "vent_max", "rayonnement_moy", "pluie_jour", "et0_jour",
            "mois", "jour_annee", "doy_sin", "doy_cos"]
LABEL = "et0_j_plus_1"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input", default=f"{HDFS}/data/meteo/ml/dataset_journalier")
    p.add_argument("--model-out", default=f"{HDFS}/data/meteo/ml/model_et0")
    p.add_argument("--results-out", default=f"{HDFS}/data/meteo/ml/resultats_evaluation")
    p.add_argument("--predictions-out", default=f"{HDFS}/data/meteo/ml/predictions_test")
    p.add_argument("--trees", type=int, default=80)
    p.add_argument("--depth", type=int, default=10)
    return p.parse_args()


def score(df, pred_col):
    out = {}
    for m in ("mae", "rmse", "r2"):
        out[m] = RegressionEvaluator(labelCol=LABEL, predictionCol=pred_col,
                                     metricName=m).evaluate(df)
    return out


def assembler():
    return VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="error")


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("lot2_train_model").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    data = spark.read.parquet(args.input)
    train = data.filter(F.col("split") == "train").cache()
    val = data.filter(F.col("split") == "val").cache()
    test = data.filter(F.col("split") == "test").cache()
    print(f"[1] Lignes : train={train.count()} val={val.count()} test={test.count()} "
          f"| {len(FEATURES)} variables")

    # ---- modeles candidats (chacun est un Pipeline complet, donc sauvegardable)
    candidates = {
        "linear_regression": Pipeline(stages=[assembler(), LinearRegression(
            featuresCol="features", labelCol=LABEL, predictionCol="prediction",
            regParam=0.01)]),
        "random_forest": Pipeline(stages=[assembler(), RandomForestRegressor(
            featuresCol="features", labelCol=LABEL, predictionCol="prediction",
            numTrees=args.trees, maxDepth=args.depth, seed=42)]),
    }

    results = {"persistance": {
        "val": score(val.withColumn("prediction", F.col("et0_jour")), "prediction"),
        "test": score(test.withColumn("prediction", F.col("et0_jour")), "prediction")}}
    for name, pipe in candidates.items():
        model = pipe.fit(train)
        results[name] = {"val": score(model.transform(val), "prediction"),
                         "test": score(model.transform(test), "prediction")}

    print("\n[2] Comparaison (MAE / RMSE en mm/jour)")
    print(f"{'modele':<20}{'MAE val':>9}{'RMSE val':>10}{'MAE test':>10}{'RMSE test':>11}{'R2 test':>9}")
    for name, r in results.items():
        print(f"{name:<20}{r['val']['mae']:>9.3f}{r['val']['rmse']:>10.3f}"
              f"{r['test']['mae']:>10.3f}{r['test']['rmse']:>11.3f}{r['test']['r2']:>9.3f}")

    best = min(candidates, key=lambda n: results[n]["val"]["rmse"])
    print(f"\n[3] Modele retenu (meilleur RMSE en validation) : {best}")

    # ---- modele final : reentraine sur train+val, evalue une seule fois sur test
    final = candidates[best].fit(train.unionByName(val))
    pred_test = final.transform(test)
    final_test = score(pred_test, "prediction")
    print(f"[4] Modele final (train+val) sur test 2025 : MAE={final_test['mae']:.3f} "
          f"RMSE={final_test['rmse']:.3f} R2={final_test['r2']:.3f}")

    # ---- sauvegarde du pipeline complet + verification de rechargement
    final.write().overwrite().save(args.model_out)
    reloaded = PipelineModel.load(args.model_out)
    check = score(reloaded.transform(test), "prediction")
    ok = abs(check["rmse"] - final_test["rmse"]) < 1e-9
    print(f"[5] Modele sauvegarde -> {args.model_out} | rechargement identique : {ok}")

    (pred_test.select("region_id", "date", "et0_jour", LABEL,
                      F.col("prediction").alias("et0_predit"))
     .coalesce(1).write.mode("overwrite").parquet(args.predictions_out))

    rows = []
    for name, r in results.items():
        for part, m in r.items():
            rows.append((name, part, float(m["mae"]), float(m["rmse"]), float(m["r2"])))
    rows.append((f"{best}_final_train_val", "test", float(final_test["mae"]),
                 float(final_test["rmse"]), float(final_test["r2"])))
    (spark.createDataFrame(rows, ["modele", "periode", "mae", "rmse", "r2"])
     .coalesce(1).write.mode("overwrite").json(args.results_out))
    print(f"[6] Resultats -> {args.results_out} | predictions -> {args.predictions_out}")
    spark.stop()


if __name__ == "__main__":
    main()
