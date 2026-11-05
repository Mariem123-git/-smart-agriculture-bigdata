"""Indicateurs batch sur les données archivées dans HDFS."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum as _sum, max as _max, round as _r, count

HDFS = os.getenv("HDFS_URI", "hdfs://namenode:8020")
spark = SparkSession.builder.appName("indicators").getOrCreate()
df = spark.read.parquet(f"{HDFS}/data/meteo/parquet")

print("Nombre de lignes lues :", df.count())
print("Doublons event_id :", df.count() - df.dropDuplicates(["event_id"]).count())

et0 = (df.groupBy("region_id", "year", "month")
         .agg(_r(avg("et0_fao_evapotranspiration"), 4).alias("et0_moyen"))
         .orderBy("region_id", "year", "month"))
et0.write.mode("overwrite").parquet(f"{HDFS}/data/meteo/indicators/et0_mensuel")
et0.show(24)

pluie = (df.groupBy("region_id", "year")
           .agg(_r(_sum("precipitation"), 1).alias("pluie_mm"))
           .orderBy("region_id", "year"))
pluie.write.mode("overwrite").parquet(f"{HDFS}/data/meteo/indicators/pluie_annuelle")
pluie.show(24)

temp = (df.groupBy("region_id")
          .agg(_r(avg("temperature_2m"), 2).alias("t_moy"),
               _max("temperature_2m").alias("t_max"),
               count("*").alias("n")))
temp.show()