# Dictionnaire de données — Smart Agriculture Maroc

## 1. Provenance et licence

| Élément | Valeur |
|---|---|
| Source | API Open-Meteo, données météo historiques horaires |
| Couverture | 50 villes du Maroc, du 2015-01-01 au 2025-12-31 |
| Licence | Données sous licence **CC BY 4.0** ; API gratuite pour un usage non commercial |
| Attribution obligatoire | « Weather data by Open-Meteo.com » — https://open-meteo.com/en/license |
| Fuseau horaire des champs `time`, `date`, `hour` | Heure locale du Maroc (UTC+1), **déduite des données** : en été, le rayonnement solaire moyen culmine à 14:00 à Casablanca et Oujda et à 15:00 à Dakhla. À confirmer avec le paramètre `timezone` du script de collecte, s'il est dans le dépôt |

## 2. Volume

| Élément | Valeur |
|---|---|
| Lignes | **4 821 600** = 50 villes × 4 018 jours × 24 heures |
| Lignes par ville | 96 432 |
| `data/meteo_clean.csv` | environ 575 Mo |
| `data/meteo_complet.jsonl` | environ 1,6 Go (1 674 616 483 octets) |
| Latitude / longitude | 23,72 à 35,19 / −15,94 à −1,91 |

## 3. Sens d'un événement

Un événement est **une observation météo horaire d'une ville** : une ligne par ville et par heure, rejouée dans Kafka (topic `meteo.hourly`, clé = `region_id`, 3 partitions).

- **Identifiant** : `event_id = <region_id>_<AAAAMMJJHHMM>`, par exemple `AGADIR_201501010000`. Unique sur tout le jeu (0 doublon).
- **Horodatage** : `time`, au format ISO 8601 (`2015-01-01T00:00:00`).

## 4. Variables (15 champs dans Kafka, 14 dans le CSV)

| Champ | Type | Unité | Description | Plage observée | Rôle |
|---|---|---|---|---|---|
| `event_id` | texte | — | Identifiant unique de l'événement | — | identifiant |
| `region_id` | texte | — | Code de la ville (50 valeurs, ex. `CASABLANCA`) | — | clé Kafka, partition HDFS |
| `zone` | texte | — | Nom lisible de la ville | — | descriptif |
| `lat`, `lon` | réel | degrés | Coordonnées de la ville | 23,72–35,19 / −15,94–−1,91 | explicative (statique) |
| `time` | texte | — | Horodatage complet de l'heure | 2015-01-01 → 2025-12-31 | horodatage |
| `date` | texte | — | Jour (`AAAA-MM-JJ`) | — | regroupement journalier |
| `hour` | texte | — | Heure (`HH:00:00`) | 24 valeurs | explicative |
| `temperature_2m` | réel | °C | Température de l'air à 2 m | −14,0 à 50,9 | explicative |
| `relative_humidity_2m` | entier | % | Humidité relative à 2 m | 1 à 100 | explicative |
| `wind_speed_10m` | réel | km/h | Vitesse du vent à 10 m | 0 à 72,8 | explicative |
| `shortwave_radiation` | réel | W/m² | Rayonnement solaire | 0 à 1 095 | explicative |
| `precipitation` | réel | mm | Précipitations sur l'heure | 0 à 20,9 | explicative |
| `et0_fao_evapotranspiration` | réel | mm | Évapotranspiration de référence ET₀ de l'heure | 0 à 1,32 | **base de la cible** |
| `ingest_ts` | texte | — | Horodatage d'ingestion, **ajouté par le producteur Kafka** (absent du CSV) | — | technique |

Le vent est en km/h, unité par défaut de l'API Open-Meteo. Les données le confirment : moyenne d'environ 11 et 99ᵉ centile d'environ 34, ce qui serait irréaliste en m/s.

## 5. Qualité des données

Contrôles faits sur `meteo_clean.csv` :

| Contrôle | Résultat |
|---|---|
| Valeurs manquantes | 0 sur les 14 colonnes |
| Doublons sur `event_id` | 0 |
| Humidité entre 0 et 100 % | respecté |
| Rayonnement ≥ 0, précipitations ≥ 0 | respecté |
| 24 heures complètes pour chaque ville et chaque jour | oui (200 900 jours-villes, tous complets) |
| Température | de −14,0 à 50,9 °C, plausible pour le Maroc |

## 6. Cible de prédiction

> **Pour une ville et un jour D, on prédit l'ET₀ du jour D+1, en mm/jour : la somme des 24 valeurs horaires de `et0_fao_evapotranspiration` du jour D+1 pour cette ville.**

- **Tâche** : régression. **Unité** : mm/jour. **Horizon** : 1 jour.
- **Exemples** : une ligne = (ville, jour D). Il y en a 50 × 4 017 = **200 850** avec cible, car le dernier jour (2025-12-31) n'a pas de lendemain.
- **Variables autorisées** : uniquement ce qui est connu à la fin du jour D : agrégats des 24 heures de D (moyenne, min, max de la température, humidité, vent, rayonnement, pluie totale, ET₀ du jour D), valeurs des jours précédents, `lat`, `lon`, mois et jour de l'année.
- **Variables interdites** : toute mesure du jour D+1. Selon la documentation Open-Meteo, l'ET₀ est calculée par la formule FAO-56 Penman-Monteith à partir de la température, de l'humidité, du vent et du rayonnement. Utiliser ces variables de D+1 donnerait la réponse presque directement (fuite d'information).
- **Découpage temporel proposé, à valider par l'équipe** : entraînement 2015–2022, validation 2023, test 2024–2025. Pas de mélange aléatoire entre les jours.
- **Références simples** (voir `docs/valeurs_reference.md`) : persistance (prédire ET₀(D+1) = ET₀(D)) et climatologie ville-mois.
