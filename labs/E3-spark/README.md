# Labo E3 : Spark, le kit de survie

**Objectif :** comprendre comment Spark exécute un job (plan, shuffle, partitions), et savoir écrire et expliquer un job simple en PySpark.
**Durée :** environ 3 h pour les niveaux 1 à 3, plus le niveau 4 sur ton projet.
**Contexte :** 40 000 établissements au format SIRENE simplifié et un petit référentiel NAF (`data/`, générés, synthétiques). Spark tourne en local sur 2 cœurs : pas de cluster, mais le même moteur et les mêmes plans.

## Avant de coder

Réponds dans `REPONSES.md`, en deux lignes chacune. Pas de bonne réponse attendue à ce stade : on en reparlera.

1. Si j'écris `df.filter(...).select(...)` sans rien d'autre, que calcule Spark ? Et après `.count()` ?
2. Quelle différence entre un `filter` et un `groupBy` du point de vue du déplacement des données entre machines ?
3. 40 000 lignes : Spark est-il le bon outil ? Qu'est-ce qui te ferait changer d'avis ?

## Niveau 1, guidé : lire, filtrer, agréger, écrire

Complète les `TODO` 1 à 4 de `lab/niveau1.py`, puis :

```bash
python verifier.py 1 2>/dev/null
```

(`2>/dev/null` masque les messages techniques de la machine virtuelle Java. Retire-le si tu cherches une erreur.)

Quand c'est fini, ouvre un `python` et regarde le plan de ton agrégation :

```python
from lab.session import creer_session; from lab.niveau1 import *
spark = creer_session(); df = actifs_avec_departement(lire_etablissements(spark))
compter_par_departement(df).explain()
```

Repère la ligne `Exchange` : c'est le shuffle. Note-la dans `REPONSES.md`.

## Niveau 2, semi-guidé : une jointure sans shuffle du gros côté

Dans `lab/niveau2.py`, joins les établissements au référentiel NAF pour ajouter le libellé. Deux contraintes : réduire les partitions de shuffle et obtenir un plan sans `SortMergeJoin`. Dans ce labo Spark ne diffuse rien tout seul : c'est à toi de le lui demander.

```bash
python verifier.py 2 2>/dev/null
```

Point de départ : compare `df.explain()` avant et après ta correction. Ouvre aussi l'interface Spark (port 4040, onglet SQL) tant que ta session tourne.

## Niveau 3, autonome : le ticket

> **Ticket #E3-3.** Le métier veut, pour chaque département (2 premiers caractères du code postal), **le code NAF le plus fréquent parmi les établissements actifs**, avec son libellé et son effectif. En cas d'égalité, le plus petit code NAF. Le résultat tient en une centaine de lignes : il doit être livré en **un seul fichier Parquet** dans `data/sortie/top_naf`.

Crée `lab/niveau3.py` avec une fonction `produire(spark)` qui construit, écrit et **retourne** le DataFrame (colonnes `departement`, `code_naf`, `libelle`, `nb`). Pas de code de départ. Tu peux réutiliser tes fonctions des niveaux 1 et 2.

```bash
python verifier.py 3 2>/dev/null
```

Dans `REPONSES.md`, explique pourquoi tu n'as pas fait de `collect()` et pourquoi un seul fichier.

## Niveau 4 : sur ton projet

Voir `niveau4_projet.md`.

## Bloqué ?

Les indices sont dans `indices/`, trois par niveau, du plus léger au plus explicite. Lis-les un par un. Tu peux aussi coller la sortie de `verifier.py` à Claude.

## Utile

| Besoin | Commande |
|---|---|
| Voir le plan d'un DataFrame | `df.explain()` (ajoute `True` pour tous les plans) |
| Voir les partitions | `df.rdd.getNumPartitions()` |
| Regarder une sortie Parquet | `ls -R data/sortie` |
| Repartir de zéro | `rm -rf data; python donnees/generer.py` |

## Quand tu as fini

Fais un commit de ton travail (`git add -A && git commit -m "E3 terminé" && git push`), puis supprime le codespace sur https://github.com/codespaces. Il se met de toute façon en veille après 30 minutes d'inactivité.
