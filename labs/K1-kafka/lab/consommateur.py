"""Niveaux 1 à 3 : appliquer les messages dans DuckDB, de façon idempotente.

    python -m lab.consommateur                 # lit jusqu'à ce qu'il n'y ait plus rien de nouveau
    python -m lab.consommateur --groupe autre  # avec un autre groupe de consommation
"""
import argparse
import json
import time

import duckdb
from confluent_kafka import Consumer

from donnees.generer import ENTREPOT, TOPIC

BOOTSTRAP = "localhost:9092"
GROUPE_PAR_DEFAUT = "labo-k1"


def creer_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS etablissements (
            siret VARCHAR PRIMARY KEY,
            version INTEGER NOT NULL,
            etat_administratif VARCHAR,
            code_postal VARCHAR
        )""")


def appliquer_message(con, msg):
    """TODO 1 (niveau 1, sans Kafka) : appliquer UN message (un dict) à la table `etablissements`.

    Les trois garanties à obtenir, ensemble :
      - un siret inconnu est inséré ;
      - un message de version PLUS RÉCENTE remplace la ligne existante ;
      - un message déjà vu ou de version plus ancienne ne change RIEN.
    Pourquoi ces trois garanties sont-elles nécessaires avec Kafka ? (Réponse dans REPONSES.md.)
    """
    raise NotImplementedError("TODO 1")


def boucle(groupe, attente_max=8.0):
    """Lit le topic et applique chaque message. S'arrête après `attente_max` secondes sans rien de nouveau."""
    con = duckdb.connect(str(ENTREPOT))
    creer_table(con)
    # TODO 2 : configurer le consommateur.
    #   - son groupe est `groupe` ;
    #   - s'il n'a encore jamais commis d'offset, il part du DÉBUT du topic ;
    #   - il ne valide PAS les offsets tout seul (on décide nous-mêmes quand).
    config = {"bootstrap.servers": BOOTSTRAP}
    c = Consumer(config)
    c.subscribe([TOPIC])
    dernier = time.time()
    n = 0
    try:
        while time.time() - dernier < attente_max:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur : {msg.error()}")
                continue
            dernier = time.time()
            appliquer_message(con, json.loads(msg.value()))
            n += 1
            # TODO 4 : valider l'offset de ce message, au bon moment, de façon synchrone.
            #   Question : avant ou après l'écriture dans DuckDB ? Que se passe-t-il si le programme plante entre les deux ?
    finally:
        c.close()
        con.close()
    print(f"{n} messages traités (groupe {groupe})")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--groupe", default=GROUPE_PAR_DEFAUT)
    boucle(p.parse_args().groupe)
