"""Voie froide : Kafka (meteo.hourly) -> HDFS Parquet partitionné par region_id.
trigger(availableNow) : consomme tout ce qui est disponible puis s'arrête.
Relancer = ne lit que les NOUVEAUX messages grâce au checkpoint."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, year, month
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meteo.hourly")
OUT = f"{HDFS}/data/meteo/parquet"
CHK = f"{HDFS}/data/meteo/checkpoint"

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
    StructField("et0_fao_evapotranspiration", DoubleType()),
    StructField("ingest_ts", StringType()),
])

spark = SparkSession.builder.appName("archive_job").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 500000)
       .load())

df = (raw.select(from_json(col("value").cast("string"), schema).alias("e"))
      .select("e.*")
      .filter(col("event_id").isNotNull() & col("region_id").isNotNull())
      .withColumn("ts", to_timestamp("time"))
      .withColumn("year", year("ts"))
      .withColumn("month", month("ts")))

q = (df.writeStream.format("parquet")
     .option("path", OUT)
     .option("checkpointLocation", CHK)
     .option("compression", "snappy")
     .partitionBy("region_id")
     .outputMode("append")
     .trigger(availableNow=True)
     .start())
q.awaitTermination()
print("Archivage termine ->", OUT)