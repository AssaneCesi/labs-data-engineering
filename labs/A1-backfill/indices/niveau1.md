# Indices du niveau 1

Lis un indice, essaie, puis passe au suivant seulement si tu bloques encore.

## TODO 1 : un run par jour
1. Airflow comprend des raccourcis de fréquence qui commencent par `@`.
2. Le raccourci quotidien équivaut à l'expression cron `0 0 * * *`.
3. `schedule="@daily"`.

## TODO 2 : la date de départ
1. Une date de départ sans fuseau horaire est interprétée en UTC.
2. `pendulum` est déjà importé, et il sait construire une date avec un fuseau.
3. `pendulum.datetime(année, mois, jour, tz="Europe/Paris")`.

## TODO 3 : charger deux fois sans doublon
1. Après le chargement du 3 novembre, que doit contenir la table pour ce jour ? Et juste avant l'insertion ?
2. Supprime ce qui existe déjà pour cette date, dans la même transaction que l'insertion. Pourquoi « dans la même transaction » est important ?
3. `DELETE FROM raw_etablissements WHERE date_chargement = ?`, avec la date en paramètre, juste avant l'`INSERT`.

## TODO 4 : les relances
1. Ce sont des paramètres de tâche, regroupés ici dans `default_args`.
2. `retries` compte les relances, pas les tentatives : 2 relances, c'est 3 tentatives au total.
3. `"retries": 2` et `"retry_delay": timedelta(minutes=2)`.

## TODO 5 : la date vient du run
1. Que vaut « maintenant » quand on rejoue le 10 le run du 3 novembre ?
2. `run.logical_date` porte la date du run. Elle peut être vide pour un run déclenché à la main : `run.run_after` sert alors de repli.
3. Ces dates sont des `datetime` Python en UTC. Minuit à Paris, c'est 23 h ou 22 h UTC la veille : convertis avec `.astimezone(ZoneInfo("Europe/Paris"))` (import : `from zoneinfo import ZoneInfo`) avant `.date()`.
