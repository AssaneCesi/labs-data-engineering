# Niveau 4 : appliquer au projet plateforme-data

Tu sais lire un plan Spark, éviter un shuffle inutile et écrire un job court. Applique-le à SIRENE, **sur l'échantillon** (`data/entrepot_echantillon.duckdb`) puis, pour la mesure, sur le volume réel.

## Tâches

1. **Reproduire.** Écris un petit script `spark/top_naf_par_departement.py` dans `plateforme-data` qui produit le même résultat que le ticket du niveau 3, à partir des établissements actifs de ton entrepôt (export Parquet ou CSV depuis DuckDB).
2. **Comparer.** Fais la même requête en SQL dans DuckDB. Compare les résultats ligne à ligne, puis les temps : sur l'échantillon, puis sur les 3,7 millions de lignes.
3. **Décider.** Rédige `docs/dimensionnement_duckdb_spark.md` (une page) : à partir de quel volume, de quelle contrainte (concurrence, mémoire, flux continu) passerais-tu à Spark pour ce pipeline ? Pourquoi DuckDB suffit aujourd'hui ? Quels chiffres l'appuient ?
4. **Journal.** Ajoute à `docs/journal.md` une entrée : ce que Spark t'a appris sur ton pipeline, avec un extrait du plan `explain()`.

## Critères d'acceptation (visibles dans le dépôt)

- Le script et la note existent, avec les temps mesurés (pas des estimations).
- Les deux résultats (Spark et DuckDB) sont identiques, et tu l'as vérifié.
- Tu réponds en deux minutes à : « Pourquoi n'utilises-tu pas Spark pour 3,7 millions de lignes ? », et à la question inverse : « Que ferais-tu à 3,7 milliards ? ».

Coche ensuite E3 et E4 dans Cap Data Quality.
