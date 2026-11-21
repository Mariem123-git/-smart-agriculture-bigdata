"""Lot 1 - Préparation du dataset ML : ET0 du lendemain (J+1) par ville.

Entrée  : historique horaire archivé dans HDFS (Parquet, partitionné par region_id).
Sortie  : dataset JOURNALIER en Parquet, une ligne = (ville, jour D), avec
          - les variables connues à la fin du jour D (aucune donnée de D+1),
          - la cible `et0_j_plus_1` (mm/jour) = ET0 réelle du jour D+1,
          - la colonne `split` (train / val / test), découpage chronologique.

Découpage par défaut (livrable 3&4) : train 2015-2023, val 2024, test 2025.
Anti-fuite : une ligne n'est gardée que si le jour D ET le jour D+1 (la cible)
appartiennent à la même période. Le 31 décembre de chaque période est donc exclu.

Lancement : voir ml/run_prepare_dataset.sh
"""
import argparse
import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input", default=f"{HDFS}/data/meteo/parquet",
                   help="Parquet horaire archivé (voie froide)")
    p.add_argument("--output", default=f"{HDFS}/data/meteo/ml/dataset_journalier",
                   help="Dossier de sortie du dataset journalier")
    p.add_argument("--train-end", default="2023-12-31",
                   help="Dernier jour (inclus) de l'entraînement")
    p.add_argument("--val-end", default="2024-12-31",
                   help="Dernier jour (inclus) de la validation ; au-delà = test")
    p.add_argument("--with-lags", action="store_true",
                   help="Ajoute ET0 de la veille et moyenne ET0 des 7 derniers jours "
                        "(à n'activer que si le streaming peut les fournir)")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("lot1_prepare_dataset").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------ 1. lecture
    hourly = spark.read.parquet(args.input)
    n_hourly = hourly.count()
    print(f"[1] Lignes horaires lues : {n_hourly}")

    # ------------------------------------------------ 2. agrégation horaire -> jour
    daily = (hourly
             .withColumn("date", F.to_date("date"))
             .groupBy("region_id", "date")
             .agg(F.first("lat").alias("lat"),
                  F.first("lon").alias("lon"),
                  F.avg("temperature_2m").alias("temp_moy"),
                  F.min("temperature_2m").alias("temp_min"),
                  F.max("temperature_2m").alias("temp_max"),
                  F.avg("relative_humidity_2m").alias("humidite_moy"),
                  F.avg("wind_speed_10m").alias("vent_moy"),
                  F.max("wind_speed_10m").alias("vent_max"),
                  F.avg("shortwave_radiation").alias("rayonnement_moy"),
                  F.sum("precipitation").alias("pluie_jour"),
                  F.sum("et0_fao_evapotranspiration").alias("et0_jour"),
                  F.count("*").alias("n_heures")))

    # ------------------------------------------- 3. qualité : jours incomplets / NULL
    n_days = daily.count()
    complete = daily.filter(F.col("n_heures") == 24)
    n_complete = complete.count()
    print(f"[3] Jours-villes : {n_days} | complets (24 h) : {n_complete} "
          f"| écartés : {n_days - n_complete}")
    value_cols = ["temp_moy", "temp_min", "temp_max", "humidite_moy", "vent_moy",
                  "vent_max", "rayonnement_moy", "pluie_jour", "et0_jour"]
    complete = complete.dropna(subset=value_cols)
    print(f"    après suppression des NULL : {complete.count()}")

    # ----------------------------------------------------- 4. cible ET0 J+1
    w = Window.partitionBy("region_id").orderBy("date")
    df = (complete
          .withColumn("et0_j_plus_1", F.lead("et0_jour", 1).over(w))
          .withColumn("date_cible", F.lead("date", 1).over(w)))
    # le lendemain doit être exactement D+1 (pas de trou dans la série)
    df = df.filter(F.datediff("date_cible", "date") == 1)

    # --------------------------------------- 5. variables explicatives (sans futur)
    doy = F.dayofyear("date")
    df = (df.withColumn("mois", F.month("date"))
            .withColumn("jour_annee", doy)
            .withColumn("doy_sin", F.sin(2 * F.lit(3.141592653589793) * doy / 365.25))
            .withColumn("doy_cos", F.cos(2 * F.lit(3.141592653589793) * doy / 365.25)))

    if args.with_lags:
        # calculés sur toute la série AVANT le découpage : seul le passé est utilisé
        w7 = w.rowsBetween(-6, 0)
        df = (df.withColumn("et0_lag1", F.lag("et0_jour", 1).over(w))
                .withColumn("et0_moy_7j", F.avg("et0_jour").over(w7))
                .withColumn("n_7j", F.count("et0_jour").over(w7))
                .filter(F.col("n_7j") == 7)
                .drop("n_7j")
                .dropna(subset=["et0_lag1"]))

    # ----------------------------------------------- 6. découpage chronologique
    def period(c):
        return (F.when(F.col(c) <= F.lit(args.train_end).cast("date"), "train")
                 .when(F.col(c) <= F.lit(args.val_end).cast("date"), "val")
                 .otherwise("test"))

    df = (df.withColumn("split", period("date"))
            .withColumn("split_cible", period("date_cible"))
            .filter(F.col("split") == F.col("split_cible"))   # cible dans la même période
            .drop("split_cible", "date_cible"))

    # -------------------------------------------------------------- 7. écriture
    features = (["lat", "lon", "temp_moy", "temp_min", "temp_max", "humidite_moy",
                 "vent_moy", "vent_max", "rayonnement_moy", "pluie_jour", "et0_jour",
                 "mois", "jour_annee", "doy_sin", "doy_cos"]
                + (["et0_lag1", "et0_moy_7j"] if args.with_lags else []))
    out = df.select("region_id", "date", *features, "et0_j_plus_1", "split")
    (out.repartition("split")
        .write.mode("overwrite").partitionBy("split")
        .option("compression", "snappy").parquet(args.output))
    print(f"[7] Dataset écrit -> {args.output}")

    # ------------------------------------------------------------ 8. vérifications
    chk = spark.read.parquet(args.output)
    print("[8] Effectifs par période :")
    chk.groupBy("split").agg(F.count("*").alias("n"),
                             F.min("date").alias("du"),
                             F.max("date").alias("au")).orderBy("du").show()
    nulls = chk.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                        for c in features + ["et0_j_plus_1"]]).first()
    print("    NULL par colonne :", {k: v for k, v in nulls.asDict().items() if v})
    print("    Cible (mm/jour) :")
    chk.groupBy("split").agg(F.round(F.avg("et0_j_plus_1"), 3).alias("moy"),
                             F.round(F.stddev("et0_j_plus_1"), 3).alias("ecart_type")).show()

    # ligne d'exemple à partager avec les Lots 2, 3 et 4
    sample = (chk.filter((F.col("region_id") == "CASABLANCA") &
                         (F.col("date") == F.lit("2025-01-15").cast("date"))).first())
    if sample is not None:
        row = {k: (str(v) if k == "date" else v) for k, v in sample.asDict().items()}
        print("[8] Ligne d'exemple (contrat de données) :")
        print(json.dumps(row, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
