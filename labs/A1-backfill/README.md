# Labo A1 : DAG quotidien, backfill et rejeu idempotent

**Objectif :** savoir rattraper des jours passés et rejouer un jour précis sans jamais créer de doublon, puis le prouver.
**Durée :** environ 2 h 30 pour les niveaux 1 à 3, plus le niveau 4 sur ton projet.
**Contexte :** un producteur dépose chaque jour un fichier d'établissements (`data/entrant/`). Ton DAG le charge dans DuckDB (`data/entrepot.duckdb`). Les données sont synthétiques.

L'interface Airflow s'ouvre toute seule dans un onglet (sinon : onglet **PORTS** en bas, port 8080). Pas de mot de passe dans le labo.

## Avant de coder

Réponds dans `REPONSES.md`, en deux lignes chacune. Pas de bonne réponse attendue à ce stade : on en reparlera.

1. Si tu rejoues le 10 le run du 3 novembre, quel fichier doit être chargé ? Comment le code peut-il le savoir ?
2. Que doit contenir la table pour le 3 novembre si on la charge deux fois ?
3. DuckDB n'accepte qu'un seul écrivain à la fois. Qu'est-ce que ça impose à l'orchestrateur ?

## Niveau 1, guidé : un DAG rejouable

Complète les `TODO` de `dags/a1_dag.py` (1, 2, 4, 5) et de `lab/chargement.py` (3), puis :

```bash
python verifier.py 1
```

Le vérificateur charge ton DAG, teste le chargement deux fois sur la même date et exécute un run daté du 7 novembre. Il ne lance aucun run dans l'interface.

## Niveau 2, semi-guidé : backfill puis rejeu

1. Charge les jours du **1er au 5 novembre** (heure de Paris) avec **une seule commande** dans le terminal. Contrainte : un seul run à la fois.
2. Rejoue **un** de ces jours déjà chargés, par le moyen de ton choix.
3. Vérifie :

```bash
python verifier.py 2
```

Point de départ : `airflow backfill --help`. Regarde aussi la grille du DAG dans l'interface pendant que ça tourne.

## Niveau 3, autonome : le ticket

> **Ticket #A1-3.** Le fichier du 3 novembre livré par le producteur était tronqué : il manquait 40 % des lignes. Le producteur vient de republier le fichier complet. Recharge uniquement le 3 novembre, sans toucher aux autres jours, et prouve-le.

Simule d'abord la republication par le producteur :

```bash
python donnees/generer.py --corriger 2026-11-03
```

Puis à toi de jouer, sans code de départ. Vérifie avec `python verifier.py 3`, et explique dans `REPONSES.md` comment tu as prouvé que les autres jours n'ont pas bougé.

## Niveau 4 : sur ton projet

Voir `niveau4_projet.md`.

## Bloqué ?

Les indices sont dans `indices/`, trois par niveau, du plus léger au plus explicite. Lis-les un par un. Tu peux aussi coller la sortie de `verifier.py` à Claude.

## Utile

| Besoin | Commande |
|---|---|
| Voir les runs du DAG | `airflow dags list-runs a1_chargement_quotidien` |
| Regarder l'entrepôt | `python -c "import duckdb; print(duckdb.connect('data/entrepot.duckdb', read_only=True).sql('SELECT date_chargement, count(*) FROM raw_etablissements GROUP BY 1 ORDER BY 1'))"` |
| Redémarrer Airflow | `pkill -f airflow; bash demarrer_airflow.sh` |
| Repartir de zéro | `pkill -f airflow; rm -rf data airflow_home; bash setup.sh; bash demarrer_airflow.sh` |

## Quand tu as fini

Fais un commit de ton travail (`git add -A && git commit -m "A1 terminé" && git push`), puis supprime le codespace sur https://github.com/codespaces pour ne pas consommer ton stockage gratuit. Il se met de toute façon en veille après 30 minutes d'inactivité.
