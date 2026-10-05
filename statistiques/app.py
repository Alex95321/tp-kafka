import json
import os
import threading
from collections import defaultdict

from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "commandes-creees"
GROUP_ID = os.getenv("GROUP_ID", "statistiques-group")

commandes_par_produit = defaultdict(int)
ca_par_mois = defaultdict(float)


def consommer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"[stats] En écoute sur '{TOPIC}' (groupe : {GROUP_ID})", flush=True)

    for message in consumer:
        evt = message.value
        produit = evt["produit"]
        mois = evt.get("date", "inconnu")[:7]  # "2026-10-05" -> "2026-10"

        commandes_par_produit[produit] += 1
        ca_par_mois[mois] += evt["montant"]

        print(f"[stats] Commande reçue : {produit} | {evt['montant']} | mois {mois}", flush=True)
        print(f"[stats] Commandes par produit : {dict(commandes_par_produit)}", flush=True)
        print(f"[stats] CA par mois : {dict(ca_par_mois)}", flush=True)


@app.route("/stats/produits", methods=["GET"])
def get_produits():
    return jsonify(dict(commandes_par_produit))


@app.route("/stats/produits/<produit>", methods=["GET"])
def get_produit(produit):
    return jsonify({"produit": produit, "nombre_commandes": commandes_par_produit.get(produit, 0)})


@app.route("/stats/ca", methods=["GET"])
def get_ca():
    return jsonify(dict(ca_par_mois))


@app.route("/stats/ca/<mois>", methods=["GET"])
def get_ca_mois(mois):
    return jsonify({"mois": mois, "chiffre_affaires": ca_par_mois.get(mois, 0)})


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8082)