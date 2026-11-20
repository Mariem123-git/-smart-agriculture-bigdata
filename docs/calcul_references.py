"""Valeurs de référence calculées avec pandas à partir du CSV propre.
Sert à vérifier que les résultats Spark (indicateurs batch) sont justes.

Usage (depuis la racine du dépôt) :
    python docs/calcul_references.py data/meteo_clean.csv
Le CSV est lu par morceaux : peu de mémoire nécessaire.
"""
import sys
import pandas as pd

CSV = sys.argv[1] if len(sys.argv) > 1 else "data/meteo_clean.csv"
COLS = ["region_id", "date", "temperature_2m", "precipitation",
        "et0_fao_evapotranspiration"]
VILLES = ["CASABLANCA", "MARRAKECH", "IFRANE"]   # côte, plaine intérieure, montagne

jour, n_lignes, premieres = [], 0, []
for ch in pd.read_csv(CSV, usecols=COLS, chunksize=500_000):
    n_lignes += len(ch)
    jour.append(ch.groupby(["region_id", "date"]).agg(
        et0=("et0_fao_evapotranspiration", "sum"),
        pluie=("precipitation", "sum"),
        tsum=("temperature_2m", "sum"),
        tmax=("temperature_2m", "max"),
        n=("date", "size")).reset_index())
    if len(premieres) == 0:                    # 10 000 premières lignes = test Spark
        premieres.append(ch.head(10_000))

# un jour peut être coupé entre deux morceaux : on recombine
j = (pd.concat(jour).groupby(["region_id", "date"])
       .agg(et0=("et0", "sum"), pluie=("pluie", "sum"), tsum=("tsum", "sum"),
            tmax=("tmax", "max"), n=("n", "sum")).reset_index())
j["annee"] = j["date"].str[:4].astype(int)
j["mois"] = j["date"].str[5:7].astype(int)

print("## A. Contrôles globaux")
print(f"lignes lues            : {n_lignes}")
print(f"régions                : {j.region_id.nunique()}")
print(f"jours par région       : {j.groupby('region_id').size().unique()}")
print(f"jours incomplets (≠24h): {(j.n != 24).sum()}")
print(f"période                : {j.date.min()} -> {j.date.max()}")

print("\n## B. Test des 10 000 premières lignes (doit égaler la sortie Spark)")
print("(comparer de préférence la somme : les moyennes sont arrondies différemment par Spark et pandas)")
p = premieres[0]
b = p.groupby("region_id").agg(n=("date", "size"),
        et0_somme=("et0_fao_evapotranspiration", "sum"),
        et0_moyen=("et0_fao_evapotranspiration", "mean"),
        t_moy=("temperature_2m", "mean"),
        t_max=("temperature_2m", "max"),
        pluie_mm=("precipitation", "sum")).round(5)
print(b.loc[["AGADIR", "AZILAL", "CASABLANCA", "DAKHLA", "IFRANE",
             "KSAR_EL_KEBIR", "MIDELT", "SAFI"]].to_string())

print("\n## C. ET0 par mois, année 2015 (villes de référence)")
c = j[(j.annee == 2015) & j.region_id.isin(VILLES)]
c = (c.groupby(["region_id", "mois"])
       .agg(et0_horaire_moyen=("et0", lambda s: s.sum() / (24 * len(s))),
            et0_jour_moyen=("et0", "mean"),
            et0_total_mois=("et0", "sum")).round(4).reset_index())
print(c.to_string(index=False))

print("\n## D. Totaux annuels (mm/an)")
d = j[j.annee.isin([2015, 2020]) & j.region_id.isin(VILLES)]
d = d.groupby(["region_id", "annee"]).agg(
    et0_mm=("et0", "sum"), pluie_mm=("pluie", "sum")).round(1).reset_index()
print(d.to_string(index=False))

print("\n## E. ET0 d'un jour = somme des 24 valeurs horaires (mm/jour)")
e = j[j.date.isin(["2015-07-15", "2020-01-15"]) & j.region_id.isin(VILLES)]
print(e[["region_id", "date", "et0", "pluie", "tmax"]].round(2).to_string(index=False))
