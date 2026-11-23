
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

HDFS = "hdfs://namenode:8020"

SOURCE = f"{HDFS}/data/meteo/source/meteo_complet.jsonl"
DESTINATION = f"{HDFS}/data/meteo/parquet"

spark = (
    SparkSession.builder
    .appName("conversion_jsonl_parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Structure des observations météorologiques
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

# 1. Lire le fichier historique JSONL
print("===== LECTURE DU JSONL =====")

df = (
    spark.read
    .schema(schema)
    .option("mode", "FAILFAST")
    .json(SOURCE)
)

# 2. Préparer les colonnes utilisées par indicators.py
df = (
    df.withColumn("ts", to_timestamp(col("time")))
      .withColumn("year", year(col("ts")))
      .withColumn("month", month(col("ts")))
      .withColumn("ingest_ts", lit(None).cast("string"))
)

# 3. Vérifier les données
print("===== VERIFICATION =====")

total = df.count()
print("Nombre de lignes JSONL :", total)

invalides = df.filter(
    col("event_id").isNull() |
    col("region_id").isNull() |
    col("ts").isNull() |
    col("et0_fao_evapotranspiration").isNull()
).count()

print("Lignes invalides :", invalides)

if invalides > 0:
    raise ValueError(
        "Des observations sont invalides. "
        "Conversion annulée."
    )

if total != 4821600:
    raise ValueError(
        f"Volume inattendu : {total} lignes. "
        "4821600 attendues."
    )

# 4. Enregistrer les fichiers Parquet
print("===== CONVERSION EN PARQUET =====")

(
    df.repartition(50, "region_id")
      .write
      .mode("errorifexists")
      .option("compression", "snappy")
      .partitionBy("region_id")
      .parquet(DESTINATION)
)

print("Conversion terminée !")
print("Destination :", DESTINATION)

spark.stop()
