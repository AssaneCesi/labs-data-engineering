# Niveau 4 : appliquer au projet plateforme-data

Tu sais maintenant rattraper et rejouer sans doublon dans un environnement simple. Applique-le à ton DAG `pipeline_plateforme_donnees_complete`, **sur l'entrepôt d'échantillon** : un rejeu sur les données réelles recharge 3,7 millions de lignes par date.

## Tâches

1. **Relire.** Dans `dags/pipeline_complete_dag.py`, retrouve l'équivalent des TODO 3 et 5 du labo : d'où vient la date des données ? Où est rendu idempotent le chargement de `raw` ? Et celui de `dq.resultats` ?
2. **Prouver l'idempotence.** Rejoue deux fois le même jour, puis compare, pour ce jour, le nombre de lignes de `raw.etablissements` et de `dq.resultats` avant et après.
3. **Backfill.** Rattrape deux jours passés avec une seule commande et un seul run à la fois. Pense au fuseau des bornes.
4. **Documenter.** Ajoute au README du projet une section « Rejouer ou rattraper un jour » : la commande exacte, ce qui est rechargé, et comment vérifier qu'il n'y a pas de doublon.

## Critères d'acceptation (visibles dans le dépôt)

- La section « Rejouer ou rattraper un jour » existe dans le README, avec une commande qui fonctionne.
- `docs/journal.md` contient le résultat de la preuve d'idempotence : les comptages avant et après.
- Tu sais répondre en moins de deux minutes : « ton pipeline a été en panne trois jours, que fais-tu ? ».

Coche ensuite l'exercice A1 dans Cap Data Quality.
