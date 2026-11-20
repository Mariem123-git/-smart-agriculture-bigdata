# Valeurs de référence pour vérifier les résultats Spark

Calculées avec pandas sur `data/meteo_clean.csv` (4 821 600 lignes).
Pour les recalculer : `python docs/calcul_references.py data/meteo_clean.csv`.

## A. Contrôles globaux

| Contrôle | Valeur attendue |
|---|---|
| Lignes | 4 821 600 |
| Villes | 50 |
| Jours par ville | 4 018 (2015-01-01 → 2025-12-31) |
| Heures par jour et par ville | 24 (aucun jour incomplet) |
| Lignes par ville | 96 432 |
| Doublons sur `event_id` | 0 |

## B. Test des 10 000 premières lignes (200 heures par ville)

Ces valeurs correspondent à la sortie Spark du premier essai d'archivage. Comparer de préférence la **somme** : les moyennes sont arrondies différemment par Spark et pandas (par exemple AZILAL : 0,07405 devient 0,0741 dans Spark).

| Ville | n | ET₀ somme (mm) | ET₀ moyenne (mm/h) | T moy (°C) | T max (°C) | Pluie (mm) |
|---|---|---|---|---|---|---|
| AGADIR | 200 | 16,40 | 0,08200 | 12,83 | 19,3 | 0,0 |
| AZILAL | 200 | 14,81 | 0,07405 | 4,33 | 13,0 | 0,0 |
| CASABLANCA | 200 | 13,99 | 0,06995 | 11,46 | 17,5 | 0,0 |
| DAKHLA | 200 | 43,56 | 0,21780 | 16,04 | 22,5 | 0,0 |
| IFRANE | 200 | 15,50 | 0,07750 | 4,16 | 13,4 | 0,0 |
| KSAR_EL_KEBIR | 200 | 12,40 | 0,06200 | 9,94 | 18,9 | 0,0 |
| MIDELT | 200 | 16,16 | 0,08080 | 3,98 | 13,3 | 0,0 |
| SAFI | 200 | 15,15 | 0,07575 | 11,77 | 18,2 | 0,0 |

Les résultats Spark du premier essai (AGADIR 0,082, DAKHLA 0,2178, IFRANE 0,0775, KSAR_EL_KEBIR 0,062, IFRANE T moy 4,16 et T max 13,4) sont identiques.

## C. ET₀ moyenne par ville et par mois, année 2015

Colonne « horaire » = ce que calcule `indicators.py` (moyenne des valeurs horaires, en mm/h). Colonne « jour » = moyenne des ET₀ journalières (somme de 24 h, en mm/jour).

| Mois | CASABLANCA horaire | CASABLANCA jour | MARRAKECH horaire | MARRAKECH jour | IFRANE horaire | IFRANE jour |
|---|---|---|---|---|---|---|
| 1 | 0,0691 | 1,6594 | 0,0785 | 1,8848 | 0,0640 | 1,5358 |
| 2 | 0,0868 | 2,0836 | 0,1049 | 2,5182 | 0,0721 | 1,7307 |
| 3 | 0,1266 | 3,0394 | 0,1554 | 3,7306 | 0,1251 | 3,0035 |
| 4 | 0,1631 | 3,9133 | 0,2006 | 4,8140 | 0,1681 | 4,0343 |
| 5 | 0,2215 | 5,3161 | 0,2528 | 6,0671 | 0,2117 | 5,0819 |
| 6 | 0,2275 | 5,4597 | 0,2646 | 6,3500 | 0,2144 | 5,1467 |
| 7 | 0,2395 | 5,7471 | 0,2906 | 6,9732 | 0,2434 | 5,8406 |
| 8 | 0,2087 | 5,0077 | 0,2508 | 6,0187 | 0,2113 | 5,0706 |
| 9 | 0,1684 | 4,0413 | 0,2074 | 4,9787 | 0,1633 | 3,9187 |
| 10 | 0,1279 | 3,0700 | 0,1430 | 3,4319 | 0,1183 | 2,8400 |
| 11 | 0,0941 | 2,2577 | 0,1209 | 2,9007 | 0,0957 | 2,2963 |
| 12 | 0,0907 | 2,1761 | 0,1012 | 2,4281 | 0,0845 | 2,0277 |

## D. Totaux annuels (mm/an)

| Ville | Année | ET₀ | Pluie |
|---|---|---|---|
| CASABLANCA | 2015 | 1 335,0 | 209,7 |
| CASABLANCA | 2020 | 1 254,7 | 260,9 |
| IFRANE | 2015 | 1 297,8 | 688,0 |
| IFRANE | 2020 | 1 410,9 | 515,3 |
| MARRAKECH | 2015 | 1 588,4 | 243,5 |
| MARRAKECH | 2020 | 1 680,0 | 262,9 |

## E. ET₀ journalière (somme des 24 valeurs horaires, mm/jour)

À comparer avec l'indicateur « ET₀ par jour et par ville » de l'Étudiant 4.

| Ville | Date | ET₀ jour | Pluie jour (mm) | T max (°C) |
|---|---|---|---|---|
| CASABLANCA | 2015-07-15 | 6,53 | 0,0 | 33,0 |
| CASABLANCA | 2020-01-15 | 2,69 | 0,0 | 20,4 |
| IFRANE | 2015-07-15 | 6,14 | 0,7 | 31,3 |
| IFRANE | 2020-01-15 | 1,95 | 0,0 | 9,5 |
| MARRAKECH | 2015-07-15 | 6,84 | 1,3 | 40,4 |
| MARRAKECH | 2020-01-15 | 2,63 | 0,0 | 19,0 |

## F. Références simples pour la prédiction de l'ET₀ du lendemain

Découpage proposé : entraînement 2015–2022 (146 100 exemples), validation 2023 (18 250), test 2024–2025 (36 500). Moyenne de la cible sur le test : 4,30 mm/jour, écart-type 1,86.

| Référence | MAE test (mm/jour) | RMSE test (mm/jour) | MAE validation |
|---|---|---|---|
| Persistance : ET₀(D+1) = ET₀(D) | 0,565 | 0,807 | 0,546 |
| Climatologie ville-mois (apprise sur 2015–2022) | 0,710 | 0,983 | 0,780 |

Un modèle Spark ML ne devient intéressant que s'il fait **mieux que la persistance** (MAE inférieure à 0,565 sur le test).
