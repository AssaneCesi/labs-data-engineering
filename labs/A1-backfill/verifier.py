"""Vérification automatique du labo A1.

    python verifier.py 1    # DAG et chargement rejouable (sans lancer Airflow)
    python verifier.py 2    # backfill du 1er au 5 novembre, puis rejeu d'une date
    python verifier.py 3    # ticket : rejouer uniquement le 3 novembre après correction

Chaque contrôle affiche OK ou un message qui t'oriente, sans te donner la solution.
"""
import contextlib
import csv
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

RACINE = Path(__file__).resolve().parent
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")
sys.path.insert(0, str(RACINE))
ENTRANT = RACINE / "data" / "entrant"
ENTREPOT = RACINE / "data" / "entrepot.duckdb"
DAG_ID = "a1_chargement_quotidien"
JOURS_BACKFILL = [date(2026, 11, d) for d in range(1, 6)]
JOUR_TICKET = date(2026, 11, 3)

resultats = []


def controle(nom, ok, conseil=""):
    resultats.append(ok)
    print(f"  {'OK ' if ok else 'NON'}  {nom}")
    if not ok and conseil:
        print(f"       -> {conseil}")


def lignes_fichier(jour):
    with open(ENTRANT / f"etablissements_{jour:%Y-%m-%d}.csv", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def charger_dag():
    from airflow.dag_processing.dagbag import DagBag
    sac = DagBag(dag_folder=str(RACINE / "dags"))
    return sac.dags.get(DAG_ID), sac.import_errors


def bilan(niveau):
    print()
    if all(resultats):
        print(f"Niveau {niveau} validé. Explique en 3 lignes ce que tu as fait et pourquoi, puis passe à la suite.")
    else:
        print(f"{resultats.count(False)} contrôle(s) à reprendre. Les indices sont dans indices/niveau{niveau}.md.")
    return 0 if all(resultats) else 1


# ---------------------------------------------------------------------------------------
def niveau1():
    print("Niveau 1 : le DAG et le chargement\n")
    if not ENTRANT.exists():
        print("  Données absentes : lance d'abord  python donnees/generer.py")
        return 1

    dag, erreurs = charger_dag()
    controle("Le DAG se charge sans erreur", dag is not None and not erreurs,
             f"Erreur d'import : {list(erreurs.values())[0] if erreurs else 'DAG introuvable'}")
    if dag is None:
        return bilan(1)

    frequence = f"{getattr(dag, 'schedule', '')} {getattr(dag.timetable, 'expression', '')}"
    controle("TODO 1 : un run par jour", "@daily" in frequence or "0 0 * * *" in frequence,
             "Le paramètre schedule accepte un raccourci de fréquence ou une expression cron.")

    sd = dag.start_date
    controle("TODO 2 : premier jour au 1er novembre 2026, heure de Paris",
             sd is not None and sd.in_timezone("Europe/Paris").date() == date(2026, 11, 1)
             and sd.in_timezone("Europe/Paris").hour == 0,
             "Une date avec fuseau horaire : pendulum.datetime(..., tz=...).")

    t = dag.get_task("charger")
    controle("TODO 4 : 2 relances espacées de 2 minutes",
             t.retries == 2 and t.retry_delay == timedelta(minutes=2),
             f"Valeurs actuelles : retries={t.retries}, retry_delay={t.retry_delay}.")

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp) / "test.duckdb"
        from lab.chargement import charger_jour
        import duckdb
        jour = date(2026, 11, 2)
        charger_jour(jour, "essai_1", entrepot=base)
        charger_jour(jour, "essai_2", entrepot=base)
        con = duckdb.connect(str(base), read_only=True)
        n = con.execute("SELECT count(*) FROM raw_etablissements WHERE date_chargement = ?", [jour]).fetchone()[0]
        con.close()
        attendu = lignes_fichier(jour)
        controle("TODO 3 : charger deux fois le même jour ne crée pas de doublons", n == attendu,
                 f"Après deux chargements du {jour} : {n} lignes, alors que le fichier en contient {attendu}.")

        os.environ["LAB_ENTREPOT"] = str(Path(tmp) / "test_dag.duckdb")
        import pendulum
        print("       (exécution d'un run de test daté du 7 novembre, quelques secondes...)")
        dag.get_task("charger").retries = 0      # pas d'attente de relance pendant le test
        try:
            with open(os.devnull, "w") as nul, contextlib.redirect_stdout(nul), contextlib.redirect_stderr(nul):
                dag.test(logical_date=pendulum.datetime(2026, 11, 7, tz="Europe/Paris"))
        except Exception:  # noqa: BLE001
            pass
        finally:
            os.environ.pop("LAB_ENTREPOT", None)
        if (Path(tmp) / "test_dag.duckdb").exists():
            con = duckdb.connect(str(Path(tmp) / "test_dag.duckdb"), read_only=True)
            dates = [r[0] for r in con.execute(
                "SELECT DISTINCT date_chargement FROM raw_etablissements").fetchall()]
            con.close()
        else:
            dates = []
        controle("TODO 5 : le run du 7 novembre charge le fichier du 7 novembre",
                 dates == [date(2026, 11, 7)],
                 (f"Le run daté du 7 novembre a chargé : {', '.join(map(str, dates))}. "
                  if dates else "Le run daté du 7 novembre n'a rien chargé (il a échoué). ")
                 + "La date des données doit venir du run, pas de l'horloge.")
    return bilan(1)


# ---------------------------------------------------------------------------------------
def lire_entrepot():
    import duckdb
    if not ENTREPOT.exists():
        return None, None
    con = duckdb.connect(str(ENTREPOT), read_only=True)
    comptes = dict(con.execute("""SELECT date_chargement, count(*) FROM raw_etablissements
                                  GROUP BY 1""").fetchall())
    journal = con.execute("""SELECT date_chargement, charge_le FROM journal_chargements
                             ORDER BY charge_le""").fetchall()
    con.close()
    return comptes, journal


def runs_backfill():
    """Nombre de runs créés par un backfill, d'après la base d'Airflow (None si illisible)."""
    try:
        from airflow.models.dagrun import DagRun
        from airflow.utils.session import create_session
        with create_session() as s:
            runs = s.query(DagRun).filter(DagRun.dag_id == DAG_ID).all()
            return sum(1 for r in runs if str(r.run_type).lower().endswith("backfill"))
    except Exception:  # noqa: BLE001
        return None


def niveau2():
    print("Niveau 2 : backfill du 1er au 5 novembre, puis rejeu d'une date\n")
    comptes, journal = lire_entrepot()
    if comptes is None:
        print("  Entrepôt vide : aucun run n'a encore chargé de données.")
        return 1
    nb = runs_backfill()
    controle("Les runs viennent d'un backfill (pas de déclenchements manuels un par un)",
             nb is None or nb >= len(JOURS_BACKFILL),
             f"{nb} run(s) de type backfill trouvés. Le backfill se lance en une seule commande.")
    manquants = [j for j in JOURS_BACKFILL if j not in comptes]
    controle("Les 5 jours du 1er au 5 novembre sont chargés", not manquants,
             f"Jours absents : {', '.join(map(str, manquants))}. Vérifie les bornes de ta commande.")
    faux = [f"{j} ({comptes[j]} lignes, fichier {lignes_fichier(j)})" for j in JOURS_BACKFILL
            if j in comptes and comptes[j] != lignes_fichier(j)
            and not (j == JOUR_TICKET and (ENTRANT / f".correction_{j}").exists())]
    controle("Chaque jour contient exactement les lignes de son fichier", not faux,
             f"Écarts : {'; '.join(faux)}. Un jour en double ? Repense au TODO 3.")
    rejoues = {j for j, _ in journal if sum(1 for k, _ in journal if k == j) >= 2}
    controle("Au moins un jour a été rejoué, sans créer de doublon", bool(rejoues) and not faux,
             "Rejoue un jour déjà chargé (depuis l'interface ou la ligne de commande), puis relance ce contrôle.")
    return bilan(2)


# ---------------------------------------------------------------------------------------
def niveau3():
    print("Niveau 3 : ticket « le fichier du 3 novembre était tronqué »\n")
    marqueur = ENTRANT / f".correction_{JOUR_TICKET}"
    if not marqueur.exists():
        print("  Le producteur n'a pas encore republié le fichier. Simule-le :")
        print(f"  python donnees/generer.py --corriger {JOUR_TICKET}")
        return 1
    corrige_le = datetime.fromisoformat(marqueur.read_text().strip())
    comptes, journal = lire_entrepot()
    attendu = lignes_fichier(JOUR_TICKET)
    controle(f"Le {JOUR_TICKET} contient le fichier corrigé ({attendu} lignes)",
             comptes.get(JOUR_TICKET) == attendu,
             f"L'entrepôt contient {comptes.get(JOUR_TICKET)} lignes pour ce jour.")
    apres = [j for j, quand in journal if quand > corrige_le]
    controle(f"Le {JOUR_TICKET} a été rechargé après la correction", JOUR_TICKET in apres,
             "Aucun chargement de ce jour depuis la republication du fichier.")
    autres = sorted({j for j in apres if j != JOUR_TICKET})
    controle("Aucun autre jour n'a été rechargé", not autres,
             f"Rechargés aussi : {', '.join(map(str, autres))}. Le ticket demande de ne toucher que le {JOUR_TICKET}.")
    autres_ok = all(comptes.get(j) == lignes_fichier(j) for j in JOURS_BACKFILL if j != JOUR_TICKET)
    controle("Les autres jours sont intacts", autres_ok)
    return bilan(3)


if __name__ == "__main__":
    niveau = sys.argv[1] if len(sys.argv) > 1 else ""
    fonctions = {"1": niveau1, "2": niveau2, "3": niveau3}
    if niveau not in fonctions:
        print(__doc__)
        sys.exit(2)
    sys.exit(fonctions[niveau]())
