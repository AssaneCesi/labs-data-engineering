"""Labo A1 : un DAG quotidien, rejouable, prêt pour le backfill.

Niveau 1 : complète les TODO 1, 2, 4 et 5 ci-dessous, et le TODO 3 dans lab/chargement.py.
Vérifie ensuite avec : python verifier.py 1
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="a1_chargement_quotidien",
    schedule=None,           # TODO 1 : un run par jour
    start_date=None,         # TODO 2 : le premier fichier date du 1er novembre 2026 (fuseau Europe/Paris)
    end_date=pendulum.datetime(2026, 11, 7, tz="Europe/Paris"),   # les données du labo s'arrêtent le 7
    catchup=False,           # laissé à False : tu rattraperas le passé avec un backfill, volontairement
    max_active_runs=1,       # pourquoi 1 ? Réponds en une phrase dans REPONSES.md
    default_args={
        "retries": 0,                          # TODO 4 : 2 relances...
        "retry_delay": timedelta(minutes=0),   # ...espacées de 2 minutes
    },
    tags=["labo", "A1"],
)
def a1_chargement_quotidien():

    @task
    def charger():
        from lab.chargement import charger_jour

        contexte = get_current_context()
        run = contexte["dag_run"]

        # TODO 5 : la date des données doit venir du RUN, pas de l'horloge.
        # Si tu rejoues le run du 3 novembre le 10, c'est le fichier du 3 qu'il faut recharger.
        # Indice : regarde les attributs de `run` (logical_date, run_after) et leur fuseau horaire.
        jour = pendulum.now("Europe/Paris").date()

        return charger_jour(jour, run.run_id)

    charger()


a1_chargement_quotidien()
