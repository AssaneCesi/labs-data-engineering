"""Génère les messages du labo K1 : des mises à jour d'établissements (synthétiques).

    python donnees/generer.py              # écrit data/messages.jsonl
    python donnees/generer.py --corrompre  # simule l'incident du niveau 3 (table vidée)

Chaque message : {"siret", "version", "etat_administratif", "code_postal"}. La `version` augmente à chaque
mise à jour d'un même siret. Le fichier contient volontairement des doublons exacts et des messages
dans le désordre (une version ancienne peut arriver après une version récente).
"""
import argparse
import json
import random
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
DATA = RACINE / "data"
MESSAGES = DATA / "messages.jsonl"
ENTREPOT = DATA / "entrepot.duckdb"
BASELINE = DATA / "baseline_offsets.json"
TOPIC = "etablissements.maj"
CODES_POSTAUX = ["75001", "75011", "75015", "92100", "92200", "78000", "93100", "94300"]


def generer():
    rng = random.Random(2026)
    msgs = []
    for _ in range(300):
        siret = f"{rng.randint(10**13, 10**14 - 1)}"
        for v in range(1, rng.randint(3, 8) + 1):
            msgs.append({"siret": siret, "version": v,
                         "etat_administratif": rng.choices(["A", "F"], weights=[70, 30])[0],
                         "code_postal": rng.choice(CODES_POSTAUX)})
    rng.shuffle(msgs)                                    # désordre global
    msgs += rng.sample(msgs, k=len(msgs) // 10)          # 10 % de doublons exacts
    return msgs


def attendu(msgs=None):
    """État attendu : la version la plus élevée de chaque siret."""
    msgs = msgs if msgs is not None else charger()
    meilleur = {}
    for m in msgs:
        if m["siret"] not in meilleur or m["version"] > meilleur[m["siret"]]["version"]:
            meilleur[m["siret"]] = m
    return meilleur


def charger():
    return [json.loads(l) for l in MESSAGES.read_text(encoding="utf-8").splitlines() if l]


def main():
    DATA.mkdir(exist_ok=True)
    msgs = generer()
    MESSAGES.write_text("\n".join(json.dumps(m) for m in msgs) + "\n", encoding="utf-8")
    print(f"data/messages.jsonl : {len(msgs)} messages, {len(attendu(msgs))} établissements distincts")


def corrompre():
    """Incident : un bug a vidé la table de l'entrepôt. Les offsets du groupe de consommation sont mémorisés avant."""
    import duckdb
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "labo-k1"})
    parts = [TopicPartition(TOPIC, p) for p in c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions]
    baseline = {str(tp.partition): tp.offset for tp in c.committed(parts, timeout=10)}
    c.close()
    BASELINE.write_text(json.dumps(baseline))
    con = duckdb.connect(str(ENTREPOT))
    con.execute("DELETE FROM etablissements")
    con.close()
    print("Incident simulé : la table etablissements est vide. Offsets du groupe labo-k1 :", baseline)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--corrompre", action="store_true")
    corrompre() if p.parse_args().corrompre else main()
