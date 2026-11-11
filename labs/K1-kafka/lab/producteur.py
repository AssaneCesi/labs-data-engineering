"""Niveau 2 : envoyer les messages de data/messages.jsonl dans le topic.

    python -m lab.producteur
"""
import json

from confluent_kafka import Producer

from donnees.generer import TOPIC, charger

BOOTSTRAP = "localhost:9092"


def rapport_livraison(erreur, msg):
    """Appelée par la bibliothèque quand le broker a accusé réception (ou échoué)."""
    if erreur is not None:
        print(f"Échec d'envoi : {erreur}")


def produire():
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    messages = charger()
    for m in messages:
        # TODO 3 : choisir la CLÉ du message. Question : quel ordre veux-tu préserver ? Entre quels messages ?
        cle = None
        p.produce(TOPIC, key=cle, value=json.dumps(m).encode(), on_delivery=rapport_livraison)
        p.poll(0)
    p.flush()
    print(f"{len(messages)} messages envoyés dans {TOPIC}")


if __name__ == "__main__":
    produire()
