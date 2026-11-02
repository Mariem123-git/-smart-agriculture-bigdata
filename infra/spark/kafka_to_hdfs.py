
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType
)

# 1. Initialisation de Spark
spark = SparkSession.builder \
    .appName("SmartAgriculture-Kafka-HDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définition du schéma JSON reçu depuis Kafka
schema = StructType([
    StructField("region_id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("humidity", IntegerType(), True)
])

# 3. Lecture du topic Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "meteo.daily") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Conversion du message JSON en colonnes
json_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
)

# 5. Extraction et nettoyage des données
clean_df = json_df.select("data.*") \
    .filter(col("region_id").isNotNull()) \
    .withColumn("ingest_ts", current_timestamp())

# 6. Enregistrement dans HDFS au format Parquet
query = clean_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option(
        "path",
        "hdfs://namenode:8020/smart-agriculture/streaming/parquet"
    ) \
    .option(
        "checkpointLocation",
        "hdfs://namenode:8020/smart-agriculture/streaming/checkpoint"
    ) \
    .partitionBy("region_id") \
    .start()

query.awaitTermination()
