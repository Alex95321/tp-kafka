import json
import os
import threading
from collections import defaultdict

from flask import Flask, jsonify
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "commandes-creees"
GROUP_ID = os.getenv("GROUP_ID", "clients-group")

clients = [
    {"id": 1, "nom": "Alice", "pays": "France"},
    {"id": 2, "nom": "Alex", "pays": "Belgique"},
    {"id": 3, "nom": "Madeleine", "pays": "France"},
]

# Compteur : client_id -> nombre de commandes
compteur_commandes = defaultdict(int)


def trouver_client(client_id):
    for c in clients:
        if c["id"] == client_id:
            return c
    return None


def consommer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"[clients] En écoute sur le topic '{TOPIC}' (groupe : {GROUP_ID})", flush=True)

    for message in consumer:
        evt = message.value
        client = trouver_client(evt["client_id"])
        nom = client["nom"] if client else f"inconnu (id {evt['client_id']})"

        compteur_commandes[evt["client_id"]] += 1

        print(
            f"[clients] Nouvelle commande -> client : {nom} | "
            f"produit : {evt['produit']} | montant : {evt['montant']}",
            flush=True,
        )
        print(f"[clients] Total commandes par client : {dict(compteur_commandes)}", flush=True)


@app.route("/clients", methods=["GET"])
def get_clients():
    return jsonify(clients)


@app.route("/clients/<int:id>", methods=["GET"])
def get_client(id):
    c = trouver_client(id)
    if c:
        return jsonify(c)
    return jsonify({"erreur": "Client introuvable"}), 404


@app.route("/clients/stats", methods=["GET"])
def get_stats():
    return jsonify(dict(compteur_commandes))


if __name__ == "__main__":
    threading.Thread(target=consommer, daemon=True).start()
    app.run(host="0.0.0.0", port=8080)