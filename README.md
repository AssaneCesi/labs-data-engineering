# Labos data engineering

Des environnements d'entraînement prêts à l'emploi, un par concept : un clic, et l'environnement s'ouvre dans le navigateur avec les outils installés, les données générées et les exercices. Chaque labo suit le même déroulé :

1. **Niveau 1, guidé :** du code à compléter (`TODO`).
2. **Niveau 2, semi-guidé :** le même concept dans une variante, avec peu d'aide.
3. **Niveau 3, autonome :** un ticket réaliste, sans code de départ.
4. **Niveau 4 :** la transposition au projet [plateforme-data](https://github.com/AssaneCesi/plateforme-data).

`python verifier.py <niveau>` contrôle ton travail et t'oriente sans donner la solution. Les indices sont gradués dans `indices/`.

## Labos

| Code | Sujet | Outils | Ouvrir |
|---|---|---|---|
| A1 | DAG quotidien, backfill, rejeu idempotent | Airflow 3, DuckDB | [![Ouvrir dans Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/AssaneCesi/labs-data-engineering?quickstart=1&devcontainer_path=.devcontainer%2FA1-backfill%2Fdevcontainer.json) |

Le lien rouvre ton codespace existant s'il y en a déjà un pour ce labo, au lieu d'en créer un nouveau.

## Bon à savoir

- **Première ouverture :** environ 3 minutes d'installation, ensuite l'interface Airflow s'ouvre seule (onglet **PORTS**, port 8080).
- **Quota gratuit :** 120 heures-cœur par mois sur un compte GitHub personnel, soit environ 60 heures sur ces machines à 2 cœurs, et 15 Go de stockage. Un codespace se met en veille après 30 minutes d'inactivité.
- **À la fin d'un labo :** commit et push de ton travail, puis supprime le codespace sur https://github.com/codespaces.
- **Données :** uniquement synthétiques ou open data.
