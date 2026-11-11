"""Niveau 2 : une jointure sans shuffle sur le gros côté.

Dans ce labo, Spark ne diffuse rien automatiquement (voir lab/session.py) et garde ses 200 partitions
de shuffle par défaut. Ton job doit faire mieux, sur ces deux points.
"""
from lab.niveau1 import CSV_ETAB, lire_etablissements  # noqa: F401  (ta fonction du niveau 1)
from pathlib import Path

CSV_NAF = str(Path(CSV_ETAB).with_name("referentiel_naf.csv"))


def configurer(spark):
    """Réduis le nombre de partitions de shuffle : 200 est absurde pour 40 000 lignes.
    La vérification attend une valeur inférieure ou égale à 8.
    """
    raise NotImplementedError("à toi")


def etablissements_avec_libelle(spark):
    """Retourne les établissements (tous, 40 000) avec une colonne `libelle` issue de referentiel_naf.csv
    (clé : code_naf). Contrainte : le plan physique ne doit contenir aucune jointure par tri-fusion
    (SortMergeJoin), et aucun shuffle du gros côté.
    """
    raise NotImplementedError("à toi")
