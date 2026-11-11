# Indices du niveau 2

## Les partitions de shuffle
1. Un option de configuration Spark SQL contient « shuffle partitions » dans son nom.
2. `spark.conf.set("spark.sql.shuffle.partitions", ...)`.
3. Une valeur de 4 convient : 2 cœurs, 40 000 lignes.

## La jointure
1. Regarde `df.explain()` de ta jointure simple : quel type de jointure Spark a choisi ? Combien d'`Exchange` ?
2. Le petit côté peut être copié intégralement sur chaque executor, ce qui évite de déplacer le gros. Cherche `broadcast` dans `pyspark.sql.functions`.
3. `etab.join(F.broadcast(naf), "code_naf", "left")`. Pourquoi `left` ? Que se passerait-il avec un code NAF absent du référentiel dans une jointure interne ?
