# Indices du niveau 1

## TODO 1 : appliquer un message
1. Trois situations : siret inconnu, version plus récente, version égale ou plus ancienne. Peut-on tout faire en une seule instruction SQL ?
2. DuckDB comprend `INSERT ... ON CONFLICT (colonne) DO UPDATE SET ...`, et on peut ajouter une condition `WHERE` à la partie `DO UPDATE`. Dans la mise à jour, `excluded` désigne la ligne qu'on essayait d'insérer.
3. Squelette :
   ```
   INSERT INTO etablissements VALUES (?, ?, ?, ?)
   ON CONFLICT (siret) DO UPDATE SET version = excluded.version, ... WHERE excluded.version > etablissements.version
   ```
   Pourquoi la condition est-elle un `>` strict, et non `>=` ?
