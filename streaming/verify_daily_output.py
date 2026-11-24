
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("lot3_verification_streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://namenode:8020"
PATH = f"{HDFS}/data/meteo/streaming_lot3/dataset_journalier"

# Lecture des fichiers Parquet produits par Streaming
df = spark.read.parquet(PATH)

print("===== VERIFICATION LOT 3 =====")

# 1. Nombre de lignes
total = df.count()
print("Nombre de journees-villes :", total)

# 2. Nombre de villes
villes = df.select("region_id").distinct().count()
print("Nombre de villes :", villes)

# 3. Doublons ville + date
uniques = df.select("region_id", "date").distinct().count()
doublons = total - uniques
print("Doublons ville/date :", doublons)

# 4. Dates couvertes
df.agg(
    F.min("date").alias("premiere_date"),
    F.max("date").alias("derniere_date")
).show(truncate=False)

# 5. Colonnes attendues par le lot 1
colonnes_attendues = [
    "region_id", "date", "lat", "lon",
    "temp_moy", "temp_min", "temp_max",
    "humidite_moy", "vent_moy", "vent_max",
    "rayonnement_moy", "pluie_jour",
    "et0_jour", "mois", "jour_annee",
    "doy_sin", "doy_cos"
]

manquantes = [
    c for c in colonnes_attendues
    if c not in df.columns
]

print("Colonnes manquantes :", manquantes)

# 6. Valeurs nulles
nulls = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in colonnes_attendues
]).first().asDict()

nb_nulls = sum(nulls.values())
print("Valeurs NULL :", nb_nulls)

# 7. Exemple de donnees
print("===== EXEMPLE CASABLANCA =====")

df.filter(
    F.col("region_id") == "CASABLANCA"
).orderBy("date").show(5, truncate=False)

# 8. Validation finale
assert total == 1550, "Nombre de lignes incorrect"
assert villes == 50, "Nombre de villes incorrect"
assert doublons == 0, "Doublons detectes"
assert not manquantes, "Colonnes manquantes"
assert nb_nulls == 0, "Valeurs nulles detectees"

print("===== VALIDATION LOT 3 REUSSIE =====")

spark.stop()
