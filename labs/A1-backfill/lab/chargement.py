"""Chargement d'un fichier quotidien dans l'entrepôt DuckDB du labo.

Ce module ne sait rien de l'orchestrateur : il reçoit une date et un identifiant de run.
C'est le DAG qui décide QUELLE date charger. Même séparation que dans ton projet.
"""
import os
from pathlib import Path

import duckdb

RACINE = Path(__file__).resolve().parent.parent
ENTREPOT = RACINE / "data" / "entrepot.duckdb"
ENTRANT = RACINE / "data" / "entrant"


def fichier_du_jour(jour):
    return ENTRANT / f"etablissements_{jour:%Y-%m-%d}.csv"


def charger_jour(jour, run_id, entrepot=None):
    """Charge le fichier du jour `jour` dans raw_etablissements et trace le chargement.

    Contrat à respecter : appeler cette fonction deux fois pour le même jour doit laisser
    exactement les mêmes données qu'un seul appel (rejeu idempotent).
    """
    entrepot = entrepot or os.environ.get("LAB_ENTREPOT") or ENTREPOT
    fichier = fichier_du_jour(jour)
    if not fichier.exists():
        raise FileNotFoundError(f"Pas de fichier pour le {jour} : {fichier}")

    con = duckdb.connect(str(entrepot))
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS raw_etablissements (
            siret VARCHAR, code_postal VARCHAR, etat_administratif VARCHAR, date_chargement DATE)""")
        con.execute("""CREATE TABLE IF NOT EXISTS journal_chargements (
            run_id VARCHAR, date_chargement DATE, nb_lignes BIGINT, charge_le TIMESTAMP)""")

        con.execute("BEGIN TRANSACTION")

        # TODO 3 : rends ce chargement rejouable.
        # Aujourd'hui, charger deux fois le même jour double ses lignes.
        # Une seule instruction SQL suffit, AVANT l'insertion. Laquelle, et sur quel critère ?

        con.execute("""
            INSERT INTO raw_etablissements
            SELECT siret, code_postal, etat_administratif, CAST(? AS DATE)
            FROM read_csv(?, header = true, all_varchar = true)""", [jour, str(fichier)])
        n = con.execute("SELECT count(*) FROM raw_etablissements WHERE date_chargement = ?",
                        [jour]).fetchone()[0]
        con.execute("INSERT INTO journal_chargements VALUES (?, ?, ?, now())", [run_id, jour, n])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return n
