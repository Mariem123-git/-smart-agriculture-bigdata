"""Lot 2 - Entrainement du modele ET0 J+1.

Entree  : dataset journalier du lot 1 (Parquet, partitionne par `split`),
          copie depuis HDFS dans ml/data/dataset_journalier/.
Sortie  : ml/models/et0_model.joblib, ml/models/metrics.json,
          ml/models/predictions_test.csv, et (option) predictions sur un
          snapshot du streaming (lot 3).

Modeles compares sur validation puis test :
  - persistance : ET0(J+1) = ET0(J)           (reference a battre)
  - Ridge       : regression lineaire
  - HistGradientBoosting : modele retenu si meilleur RMSE en validation
"""
import argparse
import json
import os
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

# Variables connues a la fin du jour D (contrat du lot 1, sans retards).
FEATURES = ["lat", "lon", "temp_moy", "temp_min", "temp_max", "humidite_moy",
            "vent_moy", "vent_max", "rayonnement_moy", "pluie_jour", "et0_jour",
            "mois", "jour_annee", "doy_sin", "doy_cos"]
LAGS = ["et0_lag1", "et0_moy_7j"]
TARGET = "et0_j_plus_1"


def load_split(root, name):
    path = os.path.join(root, f"split={name}")
    if not os.path.isdir(path):
        sys.exit(f"Dossier introuvable : {path} (lance d'abord ml/run_train.sh)")
    df = pd.read_parquet(path)
    return df.sort_values(["region_id", "date"]).reset_index(drop=True)


def metrics(y, p):
    return {"rmse": float(np.sqrt(mean_squared_error(y, p))),
            "mae": float(mean_absolute_error(y, p)),
            "r2": float(r2_score(y, p))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="ml/data/dataset_journalier")
    ap.add_argument("--out", default="ml/models")
    ap.add_argument("--stream", default="",
                    help="Dossier d'un snapshot streaming (ex. ml/data/snapshot) "
                         "pour predire l'ET0 de J+1")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    tr, va, te = (load_split(args.data, s) for s in ("train", "val", "test"))
    feats = FEATURES + [c for c in LAGS if c in tr.columns]
    print(f"Lignes : train={len(tr)} val={len(va)} test={len(te)} | "
          f"{len(feats)} variables")
    print(f"Periodes : train {tr['date'].min()}..{tr['date'].max()} | "
          f"val {va['date'].min()}..{va['date'].max()} | "
          f"test {te['date'].min()}..{te['date'].max()}")

    candidates = {
        "ridge": make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
        "hist_gbm": HistGradientBoostingRegressor(
            max_iter=400, learning_rate=0.06, max_leaf_nodes=31,
            l2_regularization=1.0, early_stopping=False, random_state=42),
    }

    results = {"persistance": {
        "val": metrics(va[TARGET], va["et0_jour"]),
        "test": metrics(te[TARGET], te["et0_jour"])}}
    fitted = {}
    for name, model in candidates.items():
        model.fit(tr[feats], tr[TARGET])
        fitted[name] = model
        results[name] = {"val": metrics(va[TARGET], model.predict(va[feats])),
                         "test": metrics(te[TARGET], model.predict(te[feats]))}

    print(f"\n{'modele':<12}{'RMSE val':>10}{'RMSE test':>11}{'MAE test':>10}{'R2 test':>9}")
    for name, r in results.items():
        print(f"{name:<12}{r['val']['rmse']:>10.3f}{r['test']['rmse']:>11.3f}"
              f"{r['test']['mae']:>10.3f}{r['test']['r2']:>9.3f}")

    best = min(fitted, key=lambda n: results[n]["val"]["rmse"])
    print(f"\nModele retenu (meilleur RMSE validation) : {best}")

    # Reentrainement final sur train+val, evaluation honnete sur test.
    trva = pd.concat([tr, va], ignore_index=True)

    model = fitted[best].__class__(**fitted[best].get_params()) \
        if best == "hist_gbm" else make_pipeline(StandardScaler(), Ridge(alpha=1.0))
    model.fit(trva[feats], trva[TARGET])
    final_test = metrics(te[TARGET], model.predict(te[feats]))
    print(f"Modele final (train+val) sur test : RMSE={final_test['rmse']:.3f} "
          f"MAE={final_test['mae']:.3f} R2={final_test['r2']:.3f}")

    out = te[["region_id", "date", "et0_jour", TARGET]].copy()
    out["et0_predit"] = model.predict(te[feats])
    out.to_csv(os.path.join(args.out, "predictions_test.csv"), index=False)

    joblib.dump({"model": model, "features": feats}, os.path.join(args.out, "et0_model.joblib"))
    results["retenu"] = best
    results["final_test"] = final_test
    with open(os.path.join(args.out, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    if args.stream:
        sdf = pd.read_parquet(args.stream)
        missing = [c for c in feats if c not in sdf.columns]
        if missing:
            sys.exit(f"Colonnes absentes du snapshot streaming : {missing}")
        sdf["et0_j_plus_1_predit"] = model.predict(sdf[feats])
        cols = ["region_id", "date", "et0_jour", "et0_j_plus_1_predit"]
        sdf[cols].to_csv(os.path.join(args.out, "predictions_streaming.csv"), index=False)
        print(f"\nPredictions streaming : {len(sdf)} lignes -> predictions_streaming.csv")
        print(sdf[cols].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
