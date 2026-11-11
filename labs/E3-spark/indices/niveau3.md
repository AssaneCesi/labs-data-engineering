# Indices du niveau 3

1. Deux étapes : d'abord compter par (département, code NAF), ensuite choisir le meilleur de chaque département.
2. « Garder la première ligne de chaque groupe » se fait avec une fenêtre : `Window.partitionBy(...).orderBy(...)` et `F.row_number().over(...)`. Le tri de la fenêtre règle aussi l'égalité.
3. Squelette :
   ```
   w = Window.partitionBy("departement").orderBy(F.col("nb").desc(), F.col("code_naf"))
   top = comptes.withColumn("r", F.row_number().over(w)).filter("r = 1").drop("r")
   ```
   Puis une jointure par diffusion avec le référentiel, et `coalesce(1)` avant d'écrire en Parquet. Pourquoi `coalesce` plutôt que `repartition(1)` ici ?
