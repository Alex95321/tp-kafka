import json
import os
from flask import Flask, jsonify, request
from kafka import KafkaProducer

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "commandes-creees"

# Connexion du producteur : sérialise chaque message en JSON (UTF-8)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

commandes = [
    {"id": 1, "client_id": 1, "produit": "PC", "montant": 1200},
    {"id": 2, "client_id": 2, "produit": "Livre", "montant": 30},
    {"id": 3, "client_id": 1, "produit": "Casque", "montant": 80},
]


@app.route("/commandes", methods=["GET"])
def get_commandes():
    return jsonify(commandes)


@app.route("/commandes/<int:id>", methods=["GET"])
def get_commande(id):
    for c in commandes:
        if c["id"] == id:
            return jsonify(c)
    return jsonify({"erreur": "Commande introuvable"}), 404


@app.route("/commandes", methods=["POST"])
def creer_commande():
    data = request.get_json()
    nouvelle_id = max(c["id"] for c in commandes) + 1 if commandes else 1
    commande = {
        "id": nouvelle_id,
        "client_id": data["client_id"],
        "produit": data["produit"],
        "montant": data["montant"],
        "date": data.get("date", date.today().isoformat()),
    }
    commandes.append(commande)

    evenement = {
        "commande_id": commande["id"],
        "client_id": commande["client_id"],
        "produit": commande["produit"],
        "montant": commande["montant"],
        "date": commande["date"],
    }
    producer.send(TOPIC, evenement)
    producer.flush()

    return jsonify(commande), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)