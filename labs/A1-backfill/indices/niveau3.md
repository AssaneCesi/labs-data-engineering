# Indices du niveau 3

1. Quel est le plus petit périmètre de rejeu possible ? Une seule date, une seule tâche.
2. Tu connais déjà deux outils du niveau 2 qui savent viser une seule date. Attention au fuseau horaire des bornes, et à ce qu'un backfill fait par défaut des runs déjà réussis.
3. Soit « Clear » sur la seule case du 3 novembre dans la grille, soit un backfill dont le début et la fin valent `2026-11-03T00:00:00+01:00`, avec `--reprocess-behavior completed`.

**Pour la preuve :** la table `journal_chargements` trace chaque chargement avec son heure. Une requête sur cette table, comparée à l'heure de la republication, prouve ce qui a été rechargé.
