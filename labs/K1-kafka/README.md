# Labo K1 : Kafka, le kit de survie

**Objectif :** comprendre le journal d'événements (topic, partition, clé, offset, groupe de consommation) et écrire un consommateur qui ne casse rien quand les messages arrivent en double ou dans le désordre.
**Durée :** environ 3 h pour les niveaux 1 à 3, plus le niveau 4 sur ton projet.
**Contexte :** un producteur publie des mises à jour d'établissements (`data/messages.jsonl`, synthétiques) dans le topic `etablissements.maj`. Ton consommateur les applique dans DuckDB (`data/entrepot.duckdb`). Le fichier contient volontairement des **doublons** et des messages **dans le désordre**, comme en vrai.

Un broker Kafka seul (mode KRaft, sans ZooKeeper) démarre tout seul à l'ouverture du codespace (`bash demarrer_kafka.sh` pour le relancer).

## Avant de coder

Réponds dans `REPONSES.md`, en deux lignes chacune. Pas de bonne réponse attendue à ce stade.

1. Deux messages concernent le même établissement : version 3 puis version 2 (retard réseau). Que doit contenir la table à la fin ?
2. Un consommateur plante après avoir écrit un message dans la base mais avant d'avoir « validé » son offset. Que se passe-t-il au redémarrage ? Qu'est-ce que ça impose à l'écriture ?
3. Dans quel cas l'ordre des messages compte-t-il, et entre quels messages ?

## Niveau 1, guidé : appliquer un message (sans broker)

Complète le TODO 1 de `lab/consommateur.py` : la fonction `appliquer_message`. Pas besoin de Kafka pour ce niveau.

```bash
python verifier.py 1
```

Le vérificateur rejoue tout le fichier dans l'ordre, puis doublé et mélangé : l'état final doit être le même.

## Niveau 2, semi-guidé : un vrai topic

1. Crée le topic avec **3 partitions** :
   ```bash
   kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic etablissements.maj --partitions 3 --replication-factor 1
   kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic etablissements.maj
   ```
2. Complète le TODO 3 de `lab/producteur.py` (la clé), puis `python -m lab.producteur`.
3. Complète les TODO 2 et 4 de `lab/consommateur.py`, puis `python -m lab.consommateur`.
4. Observe les messages avec la ligne de commande :
   ```bash
   kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic etablissements.maj --from-beginning --max-messages 5 \
     --property print.key=true --property print.partition=true
   kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group labo-k1
   ```
   Repère la colonne `LAG`.
5. `python verifier.py 2`

## Niveau 3, autonome : le ticket

> **Ticket #K1-3.** Un bug a vidé la table `etablissements` de l'entrepôt. Le topic, lui, est intact. Reconstruis la table **en rejouant le topic depuis le début**, sans republier aucun message, sans doublon, et **sans toucher aux offsets du groupe `labo-k1`** (celui de la production).

Simule d'abord l'incident :

```bash
python donnees/generer.py --corrompre
```

Puis à toi de jouer, sans code de départ. `python verifier.py 3`. Dans `REPONSES.md`, explique ce que tu as fait et pourquoi ton choix ne perturbe pas la production.

## Niveau 4 : sur ton projet

Voir `niveau4_projet.md`.

## Bloqué ?

Les indices sont dans `indices/`, trois par niveau. Lis-les un par un. Tu peux aussi coller la sortie de `verifier.py` à Claude.

## Utile

| Besoin | Commande |
|---|---|
| Lister les topics | `kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| Supprimer le topic (repartir de zéro) | `kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic etablissements.maj` |
| Redémarrer Kafka | `pkill -f kafka.Kafka; bash demarrer_kafka.sh` |
| Repartir de zéro | `pkill -f kafka.Kafka; rm -rf data; python donnees/generer.py; bash demarrer_kafka.sh` |
| Journal du broker | `tail data/kafka.log` |

## Quand tu as fini

`git add -A && git commit -m "K1 terminé" && git push`, puis supprime le codespace sur https://github.com/codespaces.
