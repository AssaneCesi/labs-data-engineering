# Mes réponses : labo K1

## Avant de coder

1. Version 3 puis version 2 : que contient la table à la fin ?

2. Plantage entre l'écriture en base et la validation de l'offset : que se passe-t-il au redémarrage ?

3. Quand l'ordre compte-t-il, et entre quels messages ?

## Après le niveau 1

4. Pourquoi les trois garanties (insertion, remplacement par plus récent, ignorer ancien ou doublon) sont-elles nécessaires avec Kafka ?

## Après le niveau 2

5. Pourquoi la clé du message est-elle le siret ? Que se passerait-il sans clé ?

6. Pourquoi valider l'offset après l'écriture en base et non avant ? Quelle garantie de livraison ça donne (au plus une fois, au moins une fois, exactement une fois) ?

7. Le LAG vaut combien ? Qu'est-ce qu'il mesure ?

8. Pourquoi 3 partitions ? Que se passe-t-il si on lance un deuxième consommateur dans le même groupe ? Et un troisième, quatrième ?

## Après le niveau 3

9. Comment as-tu rejoué ? Pourquoi un autre groupe, et pas les offsets de `labo-k1` ?

10. Pourquoi cette opération est-elle sans danger ici ? Qu'est-ce qui serait dangereux si l'écriture n'était pas idempotente ?

## Question d'entretien

> « Comment garantis-tu qu'un consommateur Kafka ne crée pas de doublons ? »

Ta réponse (5 lignes maximum) :
