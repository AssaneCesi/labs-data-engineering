"""Niveau 1 : lire, filtrer, agréger, écrire. Complète les TODO.

Rappel : en Spark, les transformations (select, filter, withColumn, groupBy...) ne calculent rien.
Seule une action (count, show, write...) déclenche le travail.
"""
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

RACINE = Path(__file__).resolve().parent.parent
CSV_ETAB = str(RACINE / "data" / "etablissements.csv")
SORTIE = str(RACINE / "data" / "sortie" / "actifs_par_departement")


def lire_etablissements(spark):
    """TODO 1 : lire le CSV avec un schéma EXPLICITE (pas inferSchema).

    Colonnes : siret, code_naf, code_postal, etat_administratif (texte) et date_creation (date).
    Pourquoi pas inferSchema ? Réfléchis à ce qu'il oblige Spark à faire avant même ta première transformation.
    """
    raise NotImplementedError("TODO 1")


def actifs_avec_departement(df):
    """TODO 2 : ne garder que les établissements actifs (etat_administratif == 'A')
    et ajouter une colonne `departement` = les 2 premiers caractères de code_postal.
    """
    raise NotImplementedError("TODO 2")


def compter_par_departement(df):
    """TODO 3 : nombre d'établissements par `departement`, colonne `nb`, triés du plus grand au plus petit.
    Ce groupBy provoque un shuffle : repère la ligne `Exchange` dans df.explain().
    """
    raise NotImplementedError("TODO 3")


def ecrire_parquet(df):
    """TODO 4 : écrire df (le résultat de TODO 2, pas le comptage) en Parquet dans SORTIE,
    partitionné par `departement`, en écrasant l'existant. Pourquoi Parquet plutôt que CSV ?
    """
    raise NotImplementedError("TODO 4")
