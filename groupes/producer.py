import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

for i in range(1, 13):
    evt = {"commande_id": i, "client_id": (i % 3) + 1, "produit": "Produit" + str(i), "montant": 10 * i}
    # la clé (client_id) détermine la partition
    producer.send("commandes-partitionnees", key=evt["client_id"], value=evt)
    time.sleep(0.3)

producer.flush()
print("12 messages envoyés")