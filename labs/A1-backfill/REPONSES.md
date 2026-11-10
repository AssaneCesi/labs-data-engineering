# Mes réponses : labo A1

## Avant de coder

1. Rejouer le 10 le run du 3 novembre : quel fichier, et comment le code le sait-il ?

2. Que doit contenir la table pour le 3 novembre si on la charge deux fois ?

3. Un seul écrivain à la fois dans DuckDB : qu'est-ce que ça impose à l'orchestrateur ?

## Après le niveau 1

4. Pourquoi `max_active_runs=1` ?

5. Pourquoi la date des données vient du run et pas de l'horloge ?

## Après le niveau 2

6. Quelle commande as-tu lancée ? Qu'est-ce qui t'a surpris ?

7. Quelle différence entre une relance automatique (retry), un rejeu (clear) et un backfill ?

8. Pourquoi `catchup=False` et un backfill volontaire, plutôt que `catchup=True` ?

## Après le niveau 3

9. Comment as-tu rechargé uniquement le 3 novembre ?

10. Comment as-tu prouvé que les autres jours n'ont pas bougé ?

## Question d'entretien

> « Ton pipeline quotidien a été en panne trois jours. Comment rattrapes-tu, sans créer de doublons, et comment le prouves-tu ? »

Ta réponse (5 lignes maximum) :
