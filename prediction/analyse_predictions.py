"""Lot 4 - Analyse des predictions ET0 J+1 (apres le streaming).

Compare les predictions du job predict_stream.py aux valeurs REELLES de l'historique
(ml/dataset_journalier, periode test = 2025). Les valeurs reelles ne sont lues qu'ici,
apres l'inference : elles n'ont jamais servi a predire.

Calcule :
  * qualite des donnees (doublons, NULL, couverture) ;
  * MAE / RMSE / R2 / biais, compares a la methode de reference (persistance : ET0(J+1) = ET0(J)) ;
  * erreurs par ville et par jour ;
  * indicateurs metier : villes avec la plus forte demande d'evapotranspiration prevue ;
  * delai observe entre la derniere observation Kafka du jour et la prediction.

Sorties HDFS (CSV, un fichier par dossier) dans --out :
  predictions_et0, comparaison_reel, metriques_globales, metriques_par_ville,
  metriques_par_jour, latence

Lancement : voir prediction/run_analyse.sh
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--pred-dir", default=f"{HDFS}/data/meteo/prediction_lot4/snapshots")
    p.add_argument("--actual", default=f"{HDFS}/data/meteo/ml/dataset_journalier",
                   help="Dataset du lot 1 (contient et0_j_plus_1 et la colonne split)")
    p.add_argument("--out", default=f"{HDFS}/data/meteo/prediction_lot4/analyse")
    return p.parse_args()


def published_snapshots(spark, base):
    """Snapshots complets uniquement (presence de _SUCCESS) : les _tmp_* sont ignores."""
    Path = spark._jvm.org.apache.hadoop.fs.Path
    root = Path(base)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(root):
        return []
    found = []
    for st in fs.listStatus(root):
        name = st.getPath().getName()
        if st.isDirectory() and not name.startswith("_") \
                and fs.exists(Path(st.getPath().toString() + "/_SUCCESS")):
            found.append(st.getPath().toString())
    return sorted(found)


def save_csv(df, out, name):
    (df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out}/{name}"))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("lot4_analyse_predictions").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------ 1. predictions
    dirs = published_snapshots(spark, args.pred_dir)
    if not dirs:
        sys.exit(f"Aucun snapshot de predictions dans {args.pred_dir} : lancer d'abord predict_stream.py")
    raw = spark.read.parquet(*dirs)
    n_raw = raw.count()
    # une seule prediction par (ville, jour) : la plus recente si un micro-lot a ete rejoue
    latest = Window.partitionBy("region_id", "date").orderBy(F.col("pred_ts").desc())
    preds = (raw.withColumn("rang", F.row_number().over(latest))
                .filter("rang = 1").drop("rang").cache())
    n = preds.count()

    print("===== LOT 4 : ANALYSE DES PREDICTIONS =====")
    print(f"Snapshots lus : {len(dirs)} | lignes brutes : {n_raw} | predictions uniques : {n}")

    # ---------------------------------------------------------- 2. qualite
    n_cities = preds.select("region_id").distinct().count()
    nulls = preds.filter(F.col("et0_predit_j_plus_1").isNull()).count()
    negatives = preds.filter(F.col("et0_predit_j_plus_1") < 0).count()
    bounds = preds.agg(F.min("date").alias("premier_jour"), F.max("date").alias("dernier_jour"),
                       F.min("date_cible").alias("premiere_cible"),
                       F.max("date_cible").alias("derniere_cible")).first()
    print("\n[1] Qualite")
    print(f"    villes : {n_cities} | doublons ville/jour : {n_raw - n} | "
          f"predictions NULL : {nulls} | predictions negatives : {negatives}")
    print(f"    jours observes : {bounds['premier_jour']} -> {bounds['dernier_jour']} | "
          f"jours predits : {bounds['premiere_cible']} -> {bounds['derniere_cible']}")

    # -------------------------------- 3. jointure avec le reel (apres l'inference)
    actual = (spark.read.parquet(args.actual).filter(F.col("split") == "test")
              .select("region_id", "date", "et0_j_plus_1"))
    joined = preds.join(actual, ["region_id", "date"], "left")
    missing_actual = joined.filter(F.col("et0_j_plus_1").isNull()).count()
    ev = (joined.filter(F.col("et0_j_plus_1").isNotNull())
          .withColumn("erreur", F.col("et0_predit_j_plus_1") - F.col("et0_j_plus_1"))
          .withColumn("erreur_abs", F.abs("erreur"))
          .withColumn("erreur_ref", F.col("et0_jour") - F.col("et0_j_plus_1"))  # persistance
          .cache())
    n_ev = ev.count()
    print(f"\n[2] Predictions comparables au reel : {n_ev} / {n} (sans valeur reelle : {missing_actual})")

    # ----------------------------------------------------- 4. metriques globales
    g = ev.agg(
        F.count("*").alias("n"),
        F.avg("erreur_abs").alias("mae"),
        F.sqrt(F.avg(F.col("erreur") ** 2)).alias("rmse"),
        F.avg("erreur").alias("biais"),
        F.avg((F.col("erreur_abs") <= 0.5).cast("double")).alias("part_erreur_inf_05mm"),
        F.avg(F.abs("erreur_ref")).alias("mae_persistance"),
        F.sqrt(F.avg(F.col("erreur_ref") ** 2)).alias("rmse_persistance"),
        F.avg("et0_j_plus_1").alias("reel_moy"),
        F.avg("et0_predit_j_plus_1").alias("pred_moy"),
        F.sum(F.col("erreur") ** 2).alias("sse"),
    ).first()
    sst = ev.agg(F.sum((F.col("et0_j_plus_1") - F.lit(g["reel_moy"])) ** 2)).first()[0]
    r2 = 1 - g["sse"] / sst if sst else float("nan")
    gain_mae = 100 * (g["mae_persistance"] - g["mae"]) / g["mae_persistance"]
    gain_rmse = 100 * (g["rmse_persistance"] - g["rmse"]) / g["rmse_persistance"]

    print("\n[3] Metriques globales (mm/jour) - streaming janvier 2025")
    print(f"    {'':<14}{'MAE':>8}{'RMSE':>8}")
    print(f"    {'Modele':<14}{g['mae']:>8.3f}{g['rmse']:>8.3f}   R2={r2:.3f}  biais={g['biais']:+.3f}")
    print(f"    {'Persistance':<14}{g['mae_persistance']:>8.3f}{g['rmse_persistance']:>8.3f}")
    print(f"    Gain vs persistance : MAE {gain_mae:+.1f} % | RMSE {gain_rmse:+.1f} %")
    print(f"    Erreur <= 0,5 mm : {100 * g['part_erreur_inf_05mm']:.1f} % des journees-villes")
    print(f"    ET0 reel moyen {g['reel_moy']:.3f} | ET0 predit moyen {g['pred_moy']:.3f}")

    metrics = spark.createDataFrame([(
        int(g["n"]), float(g["mae"]), float(g["rmse"]), float(r2), float(g["biais"]),
        float(g["part_erreur_inf_05mm"]), float(g["mae_persistance"]),
        float(g["rmse_persistance"]), float(gain_mae), float(gain_rmse))],
        ["n", "mae", "rmse", "r2", "biais", "part_erreur_inf_05mm",
         "mae_persistance", "rmse_persistance", "gain_mae_pct", "gain_rmse_pct"])

    # ------------------------------------------------------------ 5. par ville
    by_city = (ev.groupBy("region_id")
               .agg(F.count("*").alias("n"),
                    F.round(F.avg("erreur_abs"), 4).alias("mae"),
                    F.round(F.sqrt(F.avg(F.col("erreur") ** 2)), 4).alias("rmse"),
                    F.round(F.avg("erreur"), 4).alias("biais"),
                    F.round(F.avg("et0_j_plus_1"), 4).alias("et0_reel_moy"),
                    F.round(F.avg("et0_predit_j_plus_1"), 4).alias("et0_predit_moy"),
                    F.round(F.avg(F.abs("erreur_ref")), 4).alias("mae_persistance"))
               .orderBy(F.col("mae").desc()))
    print("\n[4] 10 villes avec la plus grande erreur moyenne (MAE, mm/jour)")
    by_city.show(10, truncate=False)
    print("    5 villes avec la plus petite erreur")
    by_city.orderBy("mae").show(5, truncate=False)
    better = by_city.filter(F.col("mae") < F.col("mae_persistance")).count()
    print(f"    Villes ou le modele bat la persistance : {better} / {by_city.count()}")

    # ------------------------------------------------------------- 6. par jour
    by_day = (ev.groupBy("date", "date_cible")
              .agg(F.count("*").alias("n"),
                   F.round(F.avg("et0_j_plus_1"), 4).alias("et0_reel_moy"),
                   F.round(F.avg("et0_predit_j_plus_1"), 4).alias("et0_predit_moy"),
                   F.round(F.avg("erreur_abs"), 4).alias("mae"),
                   F.round(F.avg("erreur"), 4).alias("biais"))
              .orderBy("date"))
    print("[5] Erreur par jour (moyenne sur les villes)")
    by_day.show(40, truncate=False)

    # --------------------------------------- 7. indicateur metier : demande en eau
    last_day = bounds["dernier_jour"]
    print(f"[6] Villes avec la plus forte ET0 prevue pour le {bounds['derniere_cible']} "
          f"(besoin en irrigation le plus eleve)")
    (preds.filter(F.col("date") == F.lit(last_day))
          .select("region_id", F.round("et0_predit_j_plus_1", 3).alias("et0_predit_mm"))
          .orderBy(F.col("et0_predit_mm").desc()).show(10, truncate=False))
    print("    ET0 predite moyenne sur le mois, 10 villes les plus elevees")
    (preds.groupBy("region_id")
          .agg(F.round(F.avg("et0_predit_j_plus_1"), 3).alias("et0_predit_moy_mm"))
          .orderBy(F.col("et0_predit_moy_mm").desc()).show(10, truncate=False))

    # --------------------------------------------------------------- 8. latence
    lat = preds.filter(F.col("latence_s").isNotNull())
    lat_row = lat.agg(F.count("*").alias("n"),
                      F.round(F.avg("latence_s"), 2).alias("moyenne_s"),
                      F.round(F.expr("percentile_approx(latence_s, 0.95)"), 2).alias("p95_s"),
                      F.round(F.max("latence_s"), 2).alias("max_s")).first()
    print("[7] Delai observe : derniere observation Kafka du jour -> prediction publiee")
    if lat_row["n"]:
        print(f"    n={lat_row['n']} | moyenne {lat_row['moyenne_s']} s | "
              f"p95 {lat_row['p95_s']} s | max {lat_row['max_s']} s")
    else:
        print("    non disponible (predictions issues d'un snapshot, pas du flux Kafka)")
    latence = lat.agg(F.count("*").alias("n"),
                      F.round(F.avg("latence_s"), 2).alias("moyenne_s"),
                      F.round(F.expr("percentile_approx(latence_s, 0.95)"), 2).alias("p95_s"),
                      F.round(F.max("latence_s"), 2).alias("max_s"))

    # ------------------------------------------------------------- 9. sorties HDFS
    save_csv(preds.select("region_id", "date", "date_cible", "et0_jour",
                          F.round("et0_predit_j_plus_1", 4).alias("et0_predit_j_plus_1"),
                          "modele", "pred_ts", F.round("latence_s", 2).alias("latence_s"))
                  .orderBy("region_id", "date"), args.out, "predictions_et0")
    save_csv(ev.select("region_id", "date", "date_cible",
                       F.round("et0_jour", 4).alias("et0_jour"),
                       F.round("et0_predit_j_plus_1", 4).alias("et0_predit"),
                       F.round("et0_j_plus_1", 4).alias("et0_reel"),
                       F.round("erreur", 4).alias("erreur"))
               .orderBy("region_id", "date"), args.out, "comparaison_reel")
    save_csv(metrics, args.out, "metriques_globales")
    save_csv(by_city, args.out, "metriques_par_ville")
    save_csv(by_day, args.out, "metriques_par_jour")
    save_csv(latence, args.out, "latence")
    print(f"\nSorties ecrites dans {args.out}")
    spark.stop()


if __name__ == "__main__":
    main()
