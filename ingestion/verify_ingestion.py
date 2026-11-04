import json, uuid, os
from confluent_kafka import Consumer
from dotenv import load_dotenv
load_dotenv()

c = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"),
    "group.id": f"verify-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
c.subscribe([os.getenv("KAFKA_TOPIC", "meteo.hourly")])

last = {}                      # region -> dernier time vu
count = disorder = dup = 0
seen = set()
idle = 0
while idle < 15:               # arrêt après 15 s sans message
    m = c.poll(1.0)
    if m is None:
        idle += 1
        continue
    if m.error():
        continue
    idle = 0
    r = json.loads(m.value())
    count += 1
    if r["event_id"] in seen:
        dup += 1
    seen.add(r["event_id"])
    reg = m.key().decode()
    if reg in last and r["time"] < last[reg]:
        disorder += 1
    last[reg] = r["time"]
c.close()

report = json.load(open("ingestion/reports/send_report.json"))
print(f"Messages lus      : {count}")
print(f"Messages livrés   : {report['delivered']}")
print(f"Rejetés           : {report['rejected']}")
print(f"Doublons          : {dup}")
print(f"Désordres/région  : {disorder}")
print(f"Villes vues       : {len(last)}")
print("OK" if count == report["delivered"] and dup == 0 and disorder == 0 else "PROBLÈME")