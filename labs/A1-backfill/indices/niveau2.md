# Indices du niveau 2

## Lancer le backfill
1. `airflow backfill --help` liste les sous-commandes ; l'une d'elles crée un backfill. Lis son `--help` en entier.
2. Il te faut l'identifiant du DAG, une date de début, une date de fin (incluse) et une limite de runs simultanés.
3. Forme générale : `airflow backfill create --dag-id <dag> --from-date <début> --to-date <fin> --max-active-runs 1`.

## Les runs sont créés mais rien ne bouge
1. Regarde l'interrupteur à côté du nom du DAG dans l'interface.
2. Un nouveau DAG est en pause par défaut, et un DAG en pause n'exécute rien, même un backfill.
3. Active-le dans l'interface, ou `airflow dags unpause a1_chargement_quotidien`.

## Le 1er novembre manque, ou un jour en trop apparaît
1. Regarde la colonne `logical_date` de `airflow dags list-runs a1_chargement_quotidien`.
2. Les dates de la commande sont lues en UTC. Minuit à Paris le 1er novembre, c'est quelle heure en UTC ?
3. Écris les bornes avec leur décalage horaire, par exemple `2026-11-01T00:00:00+01:00`.

## Rejouer un jour
1. Dans l'interface, une case de la grille correspond à une tâche d'un run précis : regarde ses boutons.
2. Trois moyens : « Clear » sur la case dans l'interface, `airflow tasks clear` sur une seule date, ou un nouveau backfill.
3. Un backfill ne refait pas ce qui a déjà réussi, sauf si tu changes son option `--reprocess-behavior`.
