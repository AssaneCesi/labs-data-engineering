# Mes réponses : labo E3

## Avant de coder

1. `df.filter(...).select(...)` seul : que calcule Spark ? Et après `.count()` ?

2. `filter` et `groupBy` : quelle différence pour le déplacement des données ?

3. 40 000 lignes : Spark est-il le bon outil ? Qu'est-ce qui te ferait changer d'avis ?

## Après le niveau 1

4. Pourquoi un schéma explicite plutôt que `inferSchema` ?

5. Pourquoi Parquet plutôt que CSV ? À quoi sert `partitionBy` ?

6. Colle la ligne `Exchange` de ton plan. Que représente-t-elle ?

## Après le niveau 2

7. Qu'est-ce qui change dans le plan entre `SortMergeJoin` et `BroadcastHashJoin` ? Quand la diffusion est-elle une mauvaise idée ?

8. Pourquoi 200 partitions de shuffle est une mauvaise valeur ici ? Et sur 3,7 millions de lignes ?

## Après le niveau 3

9. Pourquoi pas de `collect()` ? Pourquoi un seul fichier de sortie ?

10. Une fenêtre (`row_number`) plutôt qu'un `groupBy` + `max` : qu'est-ce que ça change ?

## Question d'entretien

> « Un job Spark qui tournait en 10 minutes en met maintenant 2 heures. Comment diagnostiques-tu ? »

Ta réponse (5 lignes maximum) :
