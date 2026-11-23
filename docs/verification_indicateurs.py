
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("verification_indicateurs")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://namenode:8020/data/meteo/indicators"

# Lecture des indicateurs déjà calculés
mensuel = spark.read.parquet(f"{HDFS}/et0_mensuel")
journalier = spark.read.parquet(f"{HDFS}/et0_journalier")

print("===== COMPARAISON ET0 MENSUELLE =====")

references_mensuelles = {
    "CASABLANCA": 0.0691,
    "MARRAKECH": 0.0785
}

for ville, attendu in references_mensuelles.items():
    lignes = mensuel.filter(
        (col("region_id") == ville) &
        (col("year") == 2015) &
        (col("month") == 1)
    ).collect()

    if len(lignes) != 1:
        raise ValueError(f"Résultat absent ou multiple : {ville}")

    obtenu = lignes[0]["et0_moyen"]
    correct = abs(obtenu - attendu) < 0.0001

    print(
        f"{ville} : obtenu={obtenu}, "
        f"attendu={attendu}, "
        f"resultat={'OK' if correct else 'ECHEC'}"
    )

    if not correct:
        raise ValueError(f"Valeur incorrecte : {ville}")

print("===== COMPARAISON ET0 JOURNALIERE =====")

references_journalieres = {
    "CASABLANCA": 6.53,
    "MARRAKECH": 6.84
}

for ville, attendu in references_journalieres.items():
    lignes = journalier.filter(
        (col("region_id") == ville) &
        (col("date") == "2015-07-15")
    ).collect()

    if len(lignes) != 1:
        raise ValueError(f"Résultat absent ou multiple : {ville}")

    obtenu = lignes[0]["et0_jour_mm"]
    correct = abs(obtenu - attendu) < 0.01

    print(
        f"{ville} : obtenu={obtenu}, "
        f"attendu={attendu}, "
        f"resultat={'OK' if correct else 'ECHEC'}"
    )

    if not correct:
        raise ValueError(f"Valeur incorrecte : {ville}")

print("===== VERIFICATION REUSSIE =====")

spark.stop()
