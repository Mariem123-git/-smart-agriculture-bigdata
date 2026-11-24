
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

HDFS = "hdfs://namenode:8020"
TOPIC = "meteo.prediction"
KAFKA = "kafka:9092"

BASE = f"{HDFS}/data/meteo/streaming_lot3"
OUTPUT = f"{BASE}/snapshots_lot4"
CHECKPOINT = f"{BASE}/checkpoint_snapshots"

spark = (
    SparkSession.builder
    .appName("lot3_streaming_journalier")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 1. Structure des messages Kafka
schema = StructType([
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
    StructField("et0_fao_evapotranspiration", DoubleType())
])

# 2. Lecture en continu depuis Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 2400)
    .load()
)

# 3. Transformer les messages JSON en colonnes
hourly = (
    kafka_stream
    .select(
        F.from_json(
            F.col("value").cast("string"), schema
        ).alias("observation")
    )
    .select("observation.*")
    .filter(
        F.col("event_id").isNotNull() &
        F.col("region_id").isNotNull()
    )
    .withColumn("date", F.to_date("date"))
)

# 4. Agrégation horaire vers journalière
daily = (
    hourly.groupBy("region_id", "date")
    .agg(
        F.first("lat").alias("lat"),
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
        F.count("*").alias("n_heures"),
        F.collect_set("hour").alias("heures_uniques"),
        F.count("temperature_2m").alias("n_temp"),
        F.count("relative_humidity_2m").alias("n_humidite"),
        F.count("wind_speed_10m").alias("n_vent"),
        F.count("shortwave_radiation").alias("n_rayonnement"),
        F.count("precipitation").alias("n_pluie"),
        F.count("et0_fao_evapotranspiration").alias("n_et0")
    )
)


# 5. Enregistrer les journées complètes
def enregistrer_journees(batch_df, batch_id):

    completes = (
        batch_df
        .filter(
            (F.col("n_heures") == 24) &
            (F.size("heures_uniques") == 24) &
            (F.col("n_temp") == 24) &
            (F.col("n_humidite") == 24) &
            (F.col("n_vent") == 24) &
            (F.col("n_rayonnement") == 24) &
            (F.col("n_pluie") == 24) &
            (F.col("n_et0") == 24)
        )
        .withColumn("mois", F.month("date"))
        .withColumn("jour_annee", F.dayofyear("date"))
        .withColumn(
            "doy_sin",
            F.sin(
                2 * F.lit(3.141592653589793) *
                F.col("jour_annee") / 365.25
            )
        )
        .withColumn(
            "doy_cos",
            F.cos(
                2 * F.lit(3.141592653589793) *
                F.col("jour_annee") / 365.25
            )
        )
        .select(
            "region_id", "date", "lat", "lon",
            "temp_moy", "temp_min", "temp_max",
            "humidite_moy", "vent_moy", "vent_max",
            "rayonnement_moy", "pluie_jour",
            "et0_jour", "mois", "jour_annee",
            "doy_sin", "doy_cos"
        )
    )

    completes.persist()

    try:
        nombre = completes.count()

        print(
            f"Micro-lot {batch_id} : "
            f"{nombre} journees-villes completes",
            flush=True
        )

        # Chemin du snapshot final et temporaire
        snapshot = f"{OUTPUT}/batch_{batch_id:05d}"
        temporaire = f"{OUTPUT}/_tmp_batch_{batch_id:05d}"

        Path = spark._jvm.org.apache.hadoop.fs.Path
        destination = Path(snapshot)
        staging = Path(temporaire)

        fs = destination.getFileSystem(
            spark._jsc.hadoopConfiguration()
        )

        succes = Path(snapshot + "/_SUCCESS")

        if fs.exists(succes):
            print(
                f"Snapshot {batch_id} deja termine",
                flush=True
            )
        else:
            # Nettoyer uniquement les dossiers incomplets
            if fs.exists(staging):
                fs.delete(staging, True)

            if fs.exists(destination):
                fs.delete(destination, True)

            # Ecrire d'abord dans un dossier temporaire
            (
                completes.write
                .mode("errorifexists")
                .option("compression", "snappy")
                .parquet(temporaire)
            )

            # Publier le snapshot en une opération HDFS
            if not fs.exists(Path(temporaire + "/_SUCCESS")):
                raise RuntimeError("Ecriture Parquet incomplete")

            if not fs.rename(staging, destination):
                raise RuntimeError("Publication du snapshot echouee")

            print(
                f"Snapshot termine : {snapshot}",
                flush=True
            )

    finally:
        completes.unpersist()



# 6. Démarrer Spark Structured Streaming
print("===== LOT 3 : STREAMING DEMARRE =====", flush=True)
print("Topic Kafka :", TOPIC, flush=True)
print("Sortie HDFS :", OUTPUT, flush=True)

query = (
    daily.writeStream
    .outputMode("complete")
    .foreachBatch(enregistrer_journees)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
