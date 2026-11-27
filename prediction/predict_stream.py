"""Lot 4 - Predictions ET0 J+1 en Spark Structured Streaming.

Voie chaude du pipeline :
    Kafka (meteo.prediction, observations horaires de janvier 2025)
      -> agregation horaire -> journaliere par ville (meme logique que le lot 3)
      -> PipelineModel du lot 2 (charge UNE fois au demarrage)
      -> predictions ET0(J+1) publiees dans HDFS (un snapshot Parquet par micro-lot)

Regles respectees :
  * le modele est charge au demarrage et inclut ses propres transformations
    (VectorAssembler) : aucune preparation n'est refaite ici ;
  * la valeur reelle ET0(J+1) n'est JAMAIS lue pendant l'inference : elle n'est
    utilisee qu'apres coup, par analyse_predictions.py ;
  * mode "update" : une journee-ville n'est emise qu'une fois, au micro-lot ou sa
    24e heure arrive. Chaque prediction est donc calculee une seule fois ;
  * publication atomique (dossier temporaire puis rename) et idempotente : un
    micro-lot rejoue apres un redemarrage ne cree pas de doublon ;
  * le delai observe (latence_s) = heure de la prediction - heure Kafka de la
    derniere observation de la journee.

Modes :
  --source kafka     (defaut) lit le topic Kafka en continu
  --source snapshot  rejoue un snapshot du lot 3 (test sans Kafka)

Lancement : voir prediction/run_predict_stream.sh
"""
import argparse
import os
import time

from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")


def parse_args():
    p = argparse.ArgumentParser(description="Lot 4 - predictions ET0 J+1 en streaming")
    p.add_argument("--source", choices=["kafka", "snapshot"], default="kafka")
    p.add_argument("--kafka", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "meteo.prediction"))
    p.add_argument("--model", default=f"{HDFS}/data/meteo/ml/model_et0",
                   help="PipelineModel sauvegarde par le lot 2")
    p.add_argument("--output", default=f"{HDFS}/data/meteo/prediction_lot4/snapshots",
                   help="Dossier des snapshots de predictions")
    p.add_argument("--checkpoint", default=f"{HDFS}/data/meteo/prediction_lot4/checkpoint")
    p.add_argument("--snapshot-input",
                   default=f"{HDFS}/data/meteo/streaming_lot3/snapshots_lot4/batch_00015",
                   help="Utilise seulement avec --source snapshot")
    p.add_argument("--trigger-seconds", type=int, default=10)
    p.add_argument("--max-offsets", type=int, default=10000,
                   help="Messages Kafka max par micro-lot (10000 = environ 8 jours x 50 villes)")
    p.add_argument("--idle-timeout", type=int, default=0,
                   help="Arret propre apres N secondes sans nouvelle donnee (0 = jamais)")
    return p.parse_args()


# --------------------------------------------------------------------------- Kafka
HOURLY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("region_id", StringType()),
    StructField("zone", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("time", StringType()),
    StructField("date", StringType()),
    StructField("hour", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("relative_humidity_2m", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("shortwave_radiation", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("et0_fao_evapotranspiration", DoubleType()),
])


def read_hourly_stream(spark, args):
    """Observations horaires depuis Kafka (+ horodatage Kafka du message)."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.kafka)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")   # applique au 1er demarrage seulement
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .load())
    return (raw.select(F.from_json(F.col("value").cast("string"), HOURLY_SCHEMA).alias("o"),
                       F.col("timestamp").alias("kafka_ts"))
               .select("o.*", "kafka_ts")
               .filter(F.col("event_id").isNotNull() & F.col("region_id").isNotNull())
               .withColumn("date", F.to_date("date")))


def aggregate_daily(hourly):
    """Heures -> jour par ville. Memes colonnes et noms que le lot 3 / le lot 1."""
    return (hourly.groupBy("region_id", "date")
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
                 F.max("kafka_ts").alias("kafka_last_ts"),
                 F.count("*").alias("n_heures"),
                 F.size(F.collect_set("hour")).alias("n_heures_uniques"),
                 F.count("temperature_2m").alias("n_temp"),
                 F.count("relative_humidity_2m").alias("n_humidite"),
                 F.count("wind_speed_10m").alias("n_vent"),
                 F.count("shortwave_radiation").alias("n_rayonnement"),
                 F.count("precipitation").alias("n_pluie"),
                 F.count("et0_fao_evapotranspiration").alias("n_et0")))


def keep_complete_days(daily):
    """Seules les journees completes (24 h distinctes, aucune mesure absente) sont predites."""
    full = ((F.col("n_heures") == 24) & (F.col("n_heures_uniques") == 24) &
            (F.col("n_temp") == 24) & (F.col("n_humidite") == 24) &
            (F.col("n_vent") == 24) & (F.col("n_rayonnement") == 24) &
            (F.col("n_pluie") == 24) & (F.col("n_et0") == 24))
    two_pi = 2 * F.lit(3.141592653589793)
    return (daily.filter(full)
            .withColumn("mois", F.month("date"))
            .withColumn("jour_annee", F.dayofyear("date"))
            .withColumn("doy_sin", F.sin(two_pi * F.col("jour_annee") / 365.25))
            .withColumn("doy_cos", F.cos(two_pi * F.col("jour_annee") / 365.25)))


# ------------------------------------------------------------------------ inference
def model_inputs(model):
    """Colonnes que le modele attend (lues dans son VectorAssembler)."""
    for stage in model.stages:
        if isinstance(stage, VectorAssembler):
            return stage.getInputCols()
    raise RuntimeError("Aucun VectorAssembler dans le pipeline sauvegarde")


def predict(model, daily, model_path):
    """Applique le pipeline du lot 2. Aucune colonne cible n'est necessaire."""
    scored = model.transform(daily)
    if "kafka_last_ts" not in scored.columns:           # mode snapshot : pas d'horodatage Kafka
        scored = scored.withColumn("kafka_last_ts", F.lit(None).cast("timestamp"))
    scored = scored.withColumn("pred_ts", F.current_timestamp())
    return scored.select(
        "region_id",
        "date",                                          # jour D (observations)
        F.date_add("date", 1).alias("date_cible"),       # jour D+1 (ce qui est predit)
        "et0_jour",
        F.col("prediction").alias("et0_predit_j_plus_1"),
        F.lit(os.path.basename(model_path.rstrip("/"))).alias("modele"),
        "kafka_last_ts",
        "pred_ts",
        (F.col("pred_ts").cast("double") - F.col("kafka_last_ts").cast("double")).alias("latence_s"),
    )


# ------------------------------------------------------------------ publication HDFS
def hadoop_fs(spark, path_str):
    Path = spark._jvm.org.apache.hadoop.fs.Path
    return Path, Path(path_str).getFileSystem(spark._jsc.hadoopConfiguration())


def is_published(spark, base, name):
    Path, fs = hadoop_fs(spark, base)
    return fs.exists(Path(f"{base}/{name}/_SUCCESS"))


def publish(spark, df, base, name):
    """Ecrit dans un dossier temporaire puis le renomme : un lecteur ne voit jamais un snapshot partiel."""
    Path, fs = hadoop_fs(spark, base)
    final, tmp = Path(f"{base}/{name}"), Path(f"{base}/_tmp_{name}")
    for incomplete in (tmp, final):                      # reste d'un micro-lot interrompu
        if fs.exists(incomplete):
            fs.delete(incomplete, True)
    df.write.mode("errorifexists").option("compression", "snappy").parquet(f"{base}/_tmp_{name}")
    if not fs.exists(Path(f"{base}/_tmp_{name}/_SUCCESS")):
        raise RuntimeError("Ecriture Parquet incomplete")
    if not fs.rename(tmp, final):
        raise RuntimeError("Publication du snapshot echouee")


# ------------------------------------------------------------------------------ main
def main():
    args = parse_args()
    spark = SparkSession.builder.appName("lot4_predictions_streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Modele charge une seule fois, au demarrage
    model = PipelineModel.load(args.model)
    inputs = model_inputs(model)
    print("===== LOT 4 : PREDICTIONS ET0 J+1 =====", flush=True)
    print("Modele charge   :", args.model, flush=True)
    print("Variables       :", inputs, flush=True)
    print("Source          :", args.source, flush=True)
    print("Sortie HDFS     :", args.output, flush=True)

    activity = {"last": time.time()}

    # --------------------------------------------------------- mode test : snapshot lot 3
    if args.source == "snapshot":
        daily = spark.read.parquet(args.snapshot_input)
        missing = [c for c in inputs if c not in daily.columns]
        if missing:
            raise RuntimeError(f"Variables du modele absentes du snapshot : {missing}")
        preds = predict(model, daily, args.model).persist()
        n = preds.count()
        publish(spark, preds, args.output, "from_snapshot")
        print(f"Snapshot lot 3 -> {n} predictions publiees ({args.output}/from_snapshot)", flush=True)
        preds.unpersist()
        spark.stop()
        return

    # ------------------------------------------------------------------ mode streaming
    daily_stream = aggregate_daily(read_hourly_stream(spark, args))

    # Le modele ne doit pas exiger de colonne que le streaming ne sait pas fournir
    # (ex. et0_lag1 / et0_moy_7j si le lot 2 a ete reentraine avec --with-lags).
    available = set(daily_stream.columns) | {"mois", "jour_annee", "doy_sin", "doy_cos"}
    missing = [c for c in inputs if c not in available]
    if missing:
        raise RuntimeError(f"Le modele exige des variables que le streaming ne fournit pas : {missing}")

    def handle_batch(batch_df, batch_id):
        name = f"batch_{batch_id:05d}"
        batch_df.persist()          # l'agregation d'etat ne doit etre evaluee qu'une seule fois
        preds = None
        try:
            if batch_df.isEmpty():
                return
            activity["last"] = time.time()
            if is_published(spark, args.output, name):
                print(f"Micro-lot {batch_id} : deja publie (reprise), ignore", flush=True)
                return
            preds = predict(model, keep_complete_days(batch_df), args.model).persist()
            n = preds.count()
            if n == 0:
                print(f"Micro-lot {batch_id} : aucune journee complete", flush=True)
                return
            publish(spark, preds, args.output, name)
            stats = preds.agg(F.countDistinct("region_id").alias("villes"),
                              F.round(F.avg("et0_predit_j_plus_1"), 3).alias("et0_pred_moy"),
                              F.round(F.max("latence_s"), 1).alias("latence_max_s")).first()
            print(f"Micro-lot {batch_id} : {n} predictions | {stats['villes']} villes | "
                  f"ET0 predit moyen {stats['et0_pred_moy']} mm | "
                  f"latence max {stats['latence_max_s']} s -> {args.output}/{name}", flush=True)
        finally:
            if preds is not None:
                preds.unpersist()
            batch_df.unpersist()

    query = (daily_stream.writeStream
             .outputMode("update")
             .foreachBatch(handle_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger_seconds} seconds")
             .start())
    print("Streaming demarre. Checkpoint :", args.checkpoint, flush=True)

    # Arret propre optionnel quand plus aucune donnee n'arrive
    while query.isActive:
        if query.awaitTermination(5):
            break
        if args.idle_timeout and time.time() - activity["last"] > args.idle_timeout:
            print(f"Aucune nouvelle donnee depuis {args.idle_timeout} s : arret propre", flush=True)
            query.stop()
            break
    spark.stop()


if __name__ == "__main__":
    main()
