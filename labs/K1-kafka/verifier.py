"""Vérification automatique du labo K1.

    python verifier.py 1    # appliquer un message de façon idempotente (sans broker)
    python verifier.py 2    # producteur, consommateur, offsets (broker lancé)
    python verifier.py 3    # ticket : reconstruire la table en rejouant le topic

Chaque contrôle affiche OK ou un message qui t'oriente, sans te donner la solution.
"""
import json
import random
import sys
import traceback
from collections import defaultdict
from pathlib import Path

RACINE = Path(__file__).resolve().parent
sys.path.insert(0, str(RACINE))
import duckdb  # noqa: E402

from donnees import generer as G  # noqa: E402

BOOTSTRAP = "localhost:9092"
GROUPE = "labo-k1"
resultats = []


def controle(nom, ok, conseil=""):
    resultats.append(bool(ok))
    print(f"  {'OK ' if ok else 'NON'}  {nom}")
    if not ok and conseil:
        print(f"       -> {conseil}")


def bilan(niveau):
    print()
    if all(resultats):
        print(f"Niveau {niveau} validé. Explique en 3 lignes ce que tu as fait et pourquoi, puis passe à la suite.")
    else:
        print(f"{resultats.count(False)} contrôle(s) à reprendre. Les indices sont dans indices/niveau{niveau}.md.")
    return 0 if all(resultats) else 1


def etat(con):
    return {r[0]: (r[1], r[2], r[3]) for r in con.execute(
        "SELECT siret, version, etat_administratif, code_postal FROM etablissements").fetchall()}


def etat_attendu():
    return {s: (m["version"], m["etat_administratif"], m["code_postal"]) for s, m in G.attendu().items()}


# ---------------------------------------------------------------------------------------
def niveau1():
    print("Niveau 1 : appliquer un message de façon idempotente\n")
    if not G.MESSAGES.exists():
        print("  Données absentes : lance d'abord  python donnees/generer.py")
        return 1
    from lab.consommateur import appliquer_message, creer_table

    def neuf():
        con = duckdb.connect(":memory:")
        creer_table(con)
        return con

    m = lambda v, e="A": {"siret": "11111111111111", "version": v, "etat_administratif": e, "code_postal": "75001"}  # noqa: E731
    try:
        con = neuf()
        appliquer_message(con, m(1))
        controle("Un siret inconnu est inséré", etat(con) == {"11111111111111": (1, "A", "75001")},
                 "Quelle instruction SQL insère, et sait réagir si la clé existe déjà ?")
        appliquer_message(con, m(2, "F"))
        controle("Une version plus récente remplace la ligne", etat(con) == {"11111111111111": (2, "F", "75001")},
                 "Il faut comparer la version du message à celle de la table.")
        appliquer_message(con, m(1, "A"))
        controle("Une version plus ancienne ne change rien", etat(con) == {"11111111111111": (2, "F", "75001")},
                 "Un message en retard ne doit jamais écraser une donnée plus récente.")
        appliquer_message(con, m(2, "F"))
        appliquer_message(con, m(2, "F"))
        controle("Le même message rejoué ne change rien et ne crée pas de doublon",
                 etat(con) == {"11111111111111": (2, "F", "75001")}, "Rejouer doit être sans effet.")

        msgs = G.charger()
        con = neuf()
        for x in msgs:
            appliquer_message(con, x)
        controle("Tout le fichier, dans l'ordre : l'état final est le bon", etat(con) == etat_attendu(),
                 "L'état final doit contenir, pour chaque siret, sa version la plus élevée.")
        con = neuf()
        melange = msgs + msgs
        random.Random(1).shuffle(melange)
        for x in melange:
            appliquer_message(con, x)
        controle("Tout le fichier, doublé et mélangé : même état final", etat(con) == etat_attendu(),
                 "Le résultat ne doit dépendre ni de l'ordre ni du nombre de rejeux.")
    except NotImplementedError as e:
        controle("appliquer_message", False, f"Il reste un TODO ({e}).")
    except Exception as e:  # noqa: BLE001
        controle("appliquer_message", False, f"Erreur : {type(e).__name__}: {str(e).splitlines()[0][:200]}")
        if "-v" in sys.argv:
            traceback.print_exc()
    return bilan(1)


# ---------------------------------------------------------------------------------------
def kafka_ou_sortir():
    import socket
    from confluent_kafka import Consumer
    try:
        socket.create_connection(("localhost", 9092), timeout=2).close()
    except OSError:
        print("  Le broker ne répond pas sur localhost:9092. Lance :  bash demarrer_kafka.sh")
        sys.exit(1)
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUPE, "log_level": 0})
    return c, c.list_topics(timeout=10)


def offsets(c, topic):
    from confluent_kafka import TopicPartition
    parts = sorted(c.list_topics(topic, timeout=10).topics[topic].partitions)
    fins = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    valides = {tp.partition: tp.offset for tp in c.committed([TopicPartition(topic, p) for p in parts], timeout=10)}
    return fins, valides


def partitions_par_cle(topic):
    """Lit tout le topic avec un consommateur jetable (aucun offset commis) : cle -> ensemble de partitions."""
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verifier-jetable", "enable.auto.commit": False, "log_level": 0})
    parts = sorted(c.list_topics(topic, timeout=10).topics[topic].partitions)
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    fins = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in parts}
    vus, par_cle, total = defaultdict(int), defaultdict(set), 0
    while sum(fins.values()) > sum(vus.values()):
        msg = c.poll(2.0)
        if msg is None:
            break
        if msg.error():
            continue
        vus[msg.partition()] += 1
        total += 1
        par_cle[msg.key().decode() if msg.key() else None].add(msg.partition())
    c.close()
    return par_cle, total


def niveau2():
    print("Niveau 2 : producteur, consommateur, offsets\n")
    c, meta = kafka_ou_sortir()
    t = meta.topics.get(G.TOPIC)
    controle(f"Le topic {G.TOPIC} existe avec 3 partitions", t is not None and len(t.partitions) == 3,
             "Crée-le avec kafka/bin/kafka-topics.sh --create (voir README, section niveau 2).")
    if t is None or len(t.partitions) != 3:
        return bilan(2)
    attendus = len(G.charger())
    par_cle, total = partitions_par_cle(G.TOPIC)
    controle(f"Le topic contient {attendus} messages", total == attendus,
             f"{total} messages. Tu as produit deux fois ? Supprime le topic (kafka-topics.sh --delete), recrée-le, reproduis.")
    controle("Tous les messages d'un même siret sont dans la même partition",
             None not in par_cle and all(len(s) == 1 for s in par_cle.values()),
             "Sans clé, Kafka répartit les messages au hasard et l'ordre par établissement est perdu. TODO 3 du producteur.")
    fins, valides = offsets(c, G.TOPIC)
    controle("Le groupe labo-k1 a validé ses offsets jusqu'au bout du topic (lag 0)",
             all(valides.get(p, -1) == fins[p] for p in fins),
             f"Offsets validés : {valides}, fin du topic : {fins}. Lance le consommateur ; vérifie le TODO 4.")
    if (G.ENTREPOT).exists():
        con = duckdb.connect(str(G.ENTREPOT), read_only=True)
        try:
            controle("La table contient la dernière version de chaque établissement", etat(con) == etat_attendu(),
                     "Compare avec l'état attendu : 300 sirets, version maximale de chacun.")
        except Exception as e:  # noqa: BLE001
            controle("La table existe", False, str(e).splitlines()[0])
        con.close()
    else:
        controle("L'entrepôt existe", False, "Lance le consommateur : python -m lab.consommateur")
    return bilan(2)


def niveau3():
    print("Niveau 3 : reconstruire la table en rejouant le topic (ticket)\n")
    c, meta = kafka_ou_sortir()
    if not G.BASELINE.exists():
        print("  Simule d'abord l'incident :  python donnees/generer.py --corrompre")
        return 1
    avant = {int(k): v for k, v in json.loads(G.BASELINE.read_text()).items()}
    fins, valides = offsets(c, G.TOPIC)
    controle("Aucun message republié (le topic a toujours le même nombre de messages)",
             sum(fins.values()) == len(G.charger()), "Le topic doit contenir exactement les messages d'origine : rejoue, ne republie pas.")
    controle("Les offsets du groupe labo-k1 n'ont pas bougé", valides == avant,
             f"Avant : {avant}, maintenant : {valides}. Le groupe de production ne doit pas être touché : utilise un autre groupe.")
    con = duckdb.connect(str(G.ENTREPOT), read_only=True)
    try:
        controle("La table est reconstruite à l'identique, sans doublon", etat(con) == etat_attendu(),
                 "Vide ou incomplète : as-tu lu tout le topic depuis le début ?")
    except Exception as e:  # noqa: BLE001
        controle("La table existe", False, str(e).splitlines()[0])
    con.close()
    return bilan(3)


if __name__ == "__main__":
    n = sys.argv[1] if len(sys.argv) > 1 else ""
    if n not in {"1", "2", "3"}:
        print(__doc__)
        sys.exit(2)
    sys.exit({"1": niveau1, "2": niveau2, "3": niveau3}[n]())
