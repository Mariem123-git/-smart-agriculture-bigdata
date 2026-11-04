import os, json, time, sys
from datetime import datetime, timezone
import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC     = os.getenv("KAFKA_TOPIC", "meteo.hourly")
INPUT     = os.getenv("INPUT_FILE", "data/meteo_clean.csv")
DELAY     = float(os.getenv("DELAY", "0"))   # secondes par heure simulée
LIMIT     = int(os.getenv("LIMIT", "0"))     # 0 = tout envoyer
CHUNK     = 50_000

COLUMNS = ["event_id", "region_id", "zone", "lat", "lon", "time", "date", "hour",
           "temperature_2m", "relative_humidity_2m", "wind_speed_10m",
           "shortwave_radiation", "precipitation", "et0_fao_evapotranspiration"]
REQUIRED = ["event_id", "region_id", "time", "date", "hour"]
RANGES = {                                    # plages physiques (min, max)
    "lat": (20, 37), "lon": (-18, 0),         # Maroc
    "temperature_2m": (-30, 60),
    "relative_humidity_2m": (0, 100),
    "wind_speed_10m": (0, 200),
    "shortwave_radiation": (0, 1400),
    "precipitation": (0, 200),                # mm/heure
    "et0_fao_evapotranspiration": (0, 2),     # mm/heure
}

stats = {"sent": 0, "delivered": 0, "failed": 0, "rejected": 0}


def validate(rec):
    for c in REQUIRED:
        if rec.get(c) in (None, ""):
            return f"champ obligatoire manquant: {c}"
    for c, (lo, hi) in RANGES.items():
        v = rec.get(c)
        if v is None or isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"{c} nul ou non numérique: {v}"
        if not (lo <= v <= hi):
            return f"{c} hors plage: {v}"
    return None


def on_delivery(err, msg):
    if err is not None:
        stats["failed"] += 1
        print(f"Échec livraison: {err}", file=sys.stderr)
    else:
        stats["delivered"] += 1


def load():
    if INPUT.endswith(".parquet"):
        df = pd.read_parquet(INPUT)
    elif INPUT.endswith(".jsonl"):
        df = pd.read_json(INPUT, lines=True, convert_dates=False, dtype=False)
    else:
        df = pd.read_csv(INPUT)
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Colonnes absentes du fichier: {missing}\nColonnes trouvées: {list(df.columns)}")
    df = df[COLUMNS].copy()
    for c in ["event_id", "region_id", "zone", "time", "date", "hour"]:
        df[c] = df[c].astype(str)
    df = df.sort_values(["time", "region_id"], kind="stable")
    return df.head(LIMIT) if LIMIT else df

def main():
    os.makedirs("ingestion/rejects", exist_ok=True)
    os.makedirs("ingestion/reports", exist_ok=True)
    df = load()
    print(f"{len(df)} lignes à traiter -> topic {TOPIC} sur {BOOTSTRAP}")

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 200,
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    rejects = open("ingestion/rejects/rejected.jsonl", "w", encoding="utf-8")
    last_t = None
    t0 = time.time()

    for start in range(0, len(df), CHUNK):
        part = df.iloc[start:start + CHUNK].astype(object)
        part = part.where(part.notna(), None)
        for rec in part.to_dict(orient="records"):
            reason = validate(rec)
            if reason:
                stats["rejected"] += 1
                rejects.write(json.dumps({"reason": reason, "record": rec},
                                         default=str) + "\n")
                continue

            # rejeu : pause à chaque nouvelle heure simulée
            if DELAY > 0 and last_t is not None and rec["time"] != last_t:
                producer.flush()
                time.sleep(DELAY)
            last_t = rec["time"]

            rec["ingest_ts"] = datetime.now(timezone.utc).isoformat()
            payload = json.dumps(rec, default=str).encode("utf-8")
            while True:
                try:
                    producer.produce(TOPIC, key=rec["region_id"],
                                     value=payload, callback=on_delivery)
                    break
                except BufferError:
                    producer.poll(1)
            stats["sent"] += 1
            producer.poll(0)

        print(f"{min(start + CHUNK, len(df))}/{len(df)} lignes traitées")

    producer.flush()
    rejects.close()
    stats["duration_s"] = round(time.time() - t0, 1)
    with open("ingestion/reports/send_report.json", "w") as f:
        json.dump(stats, f, indent=2)
    print(stats)


if __name__ == "__main__":
    main()