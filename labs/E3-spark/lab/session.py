"""Création de la session Spark du labo : mode local, 2 cœurs, interface sur le port 4040."""
from pyspark.sql import SparkSession


def creer_session(nom="labo-e3"):
    return (
        SparkSession.builder.master("local[2]").appName(nom)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "false")   # AQE coupée : on veut voir le plan « brut »
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")   # aucun broadcast automatique : c'est à toi de le demander
        .config("spark.sql.shuffle.partitions", "200")   # la valeur par défaut de Spark
        .getOrCreate()
    )
