
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Verification_HDFS")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

path = "hdfs://namenode:8020/data/meteo/parquet"

df = spark.read.parquet(path)

total = df.count()
distinct = df.dropDuplicates().count()

print("========== VERIFICATION HDFS ==========")
print("Nombre total de lignes :", total)
print("Nombre de lignes distinctes :", distinct)
print("Nombre de doublons :", total - distinct)
print("=======================================")

spark.stop()
