
import argparse
import json
from pathlib import Path
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

source = Path(args.input)
destination = Path(args.output)

destination.parent.mkdir(parents=True, exist_ok=True)

nb_observations = 0
villes = set()
jours_villes = Counter()
ids = set()

# Lecture ligne par ligne pour éviter
# de charger le fichier de 1,6 Go en mémoire.
with source.open("r", encoding="utf-8") as entree:
    with destination.open("w", encoding="utf-8") as sortie:

        for ligne in entree:
            observation = json.loads(ligne)

            date = observation["date"]

            # Conserver uniquement janvier 2025
            if "2025-01-01" <= date < "2025-02-01":

                sortie.write(
                    json.dumps(observation) + "\n"
                )

                nb_observations += 1
                villes.add(observation["region_id"])

                cle = (observation["region_id"], date)
                jours_villes[cle] += 1
                ids.add(observation["event_id"])

print("===== PREPARATION LOT 3 =====")
print("Periode : janvier 2025")
print("Observations :", nb_observations)
print("Villes :", len(villes))
print("Jours-villes :", len(jours_villes))
print("Event IDs uniques :", len(ids))

jours_incomplets = sum(
    1 for n in jours_villes.values() if n != 24
)

print("Jours incomplets :", jours_incomplets)
print("Fichier cree :", destination)

if nb_observations != 37200:
    raise ValueError("Nombre d'observations inattendu")

if len(ids) != nb_observations or jours_incomplets != 0:
    raise ValueError("Doublons ou journées incomplètes")
