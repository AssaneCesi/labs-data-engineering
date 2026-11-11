# Indices du niveau 1

Lis un indice, essaie, puis passe au suivant seulement si tu bloques encore.

## TODO 1 : lire avec un schéma explicite
1. Un `StructType` est une liste de `StructField(nom, type)`. Les types sont déjà importés en haut du fichier.
2. `spark.read` a un format (ici CSV), des options (l'en-tête) et une méthode de schéma.
3. `spark.read.csv(CSV_ETAB, header=True, schema=schema)`, avec `schema = StructType([StructField("siret", StringType()), ...])`.

## TODO 2 : filtrer et ajouter une colonne
1. Deux transformations enchaînées : une qui retire des lignes, une qui ajoute une colonne.
2. `F.col("x") == "A"` est une expression de colonne. `F.substring(colonne, début, longueur)` : en SQL, les positions commencent à 1.
3. `df.filter(F.col("etat_administratif") == "A").withColumn("departement", F.substring("code_postal", 1, 2))`.

## TODO 3 : compter par département
1. Regroupe, puis agrège : deux étapes.
2. `groupBy(...).agg(F.count("*").alias("nb"))`, puis un tri.
3. `.orderBy(F.col("nb").desc())`. Pourquoi le tri est-il placé après l'agrégation ?

## TODO 4 : écrire en Parquet partitionné
1. L'écriture passe par `df.write`. Trois réglages : mode, partitionnement, format.
2. `mode("overwrite")` rend l'écriture rejouable : même idée que l'idempotence de E2 et A1.
3. `df.write.mode("overwrite").partitionBy("departement").parquet(SORTIE)`.
