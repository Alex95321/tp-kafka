import json
import sys
from kafka import KafkaConsumer

nom = sys.argv[1]
group_id = sys.argv[2]
topic = sys.argv[3] if len(sys.argv) > 3 else "commandes-partitionnees"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    group_id=group_id,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)
print(f"[{nom}] groupe={group_id} en écoute...", flush=True)

for m in consumer:
    print(f"[{nom}] partition={m.partition} offset={m.offset} -> commande {m.value['commande_id']} ({m.value['produit']})", flush=True)