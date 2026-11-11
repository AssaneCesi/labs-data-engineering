"""Vérification automatique du labo E3.

    python verifier.py 1    # lecture, filtre, agrégation, écriture Parquet
    python verifier.py 2    # jointure par diffusion, partitions de shuffle
    python verifier.py 3    # ticket : top NAF par département avec une fenêtre

Chaque contrôle affiche OK ou un message qui t'oriente, sans te donner la solution.
"""
import csv
import sys
import traceback
from collections import Counter
from pathlib import Path

RACINE = Path(__file__).resolve().parent
sys.path.insert(0, str(RACINE))
DATA = RACINE / "data"
resultats = []


def controle(nom, ok, conseil=""):
    resultats.append(bool(ok))
    print(f"  {'OK ' if ok else 'NON'}  {nom}")
    if not ok and conseil:
        print(f"       -> {conseil}")


def bilan(niveau):
    print()
    if all(resultats):
        print(f"Niveau {niveau} validé. Explique en 3 lignes ce que tu as fait et pourquoi, puis passe à la suite.")
    else:
        print(f"{resultats.count(False)} contrôle(s) à reprendre. Les indices sont dans indices/niveau{niveau}.md.")
    return 0 if all(resultats) else 1


def session():
    from lab.session import creer_session
    s = creer_session("verifier")
    s.sparkContext.setLogLevel("ERROR")
    return s


def lignes():
    with open(DATA / "etablissements.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def plan(df):
    return df._jdf.queryExecution().executedPlan().toString()


def essayer(nom, f, conseil):
    try:
        return f()
    except NotImplementedError as e:
        controle(nom, False, f"Il reste un TODO ({e}).")
    except Exception as e:  # noqa: BLE001
        controle(nom, False, f"{conseil} Erreur : {type(e).__name__}: {str(e).splitlines()[0][:200]}")
        if "-v" in sys.argv:
            traceback.print_exc()
    return None


# ---------------------------------------------------------------------------------------
def niveau1():
    print("Niveau 1 : lire, filtrer, agréger, écrire\n")
    if not (DATA / "etablissements.csv").exists():
        print("  Données absentes : lance d'abord  python donnees/generer.py")
        return 1
    from pyspark.sql.types import DateType
    from lab import niveau1 as n1
    spark = session()
    rows = lignes()
    attendus = [r for r in rows if r["etat_administratif"] == "A"]

    df = essayer("TODO 1 : lecture", lambda: n1.lire_etablissements(spark), "Vérifie le schéma passé à spark.read.")
    if df is not None:
        types = {f.name: f.dataType for f in df.schema.fields}
        controle("TODO 1 : 5 colonnes, date_creation de type date",
                 list(types) == ["siret", "code_naf", "code_postal", "etat_administratif", "date_creation"]
                 and isinstance(types.get("date_creation"), DateType),
                 "Un schéma explicite : StructType avec un StructField par colonne. Spark sait lire une date au format ISO.")
        controle("TODO 1 : 40 000 lignes lues", df.count() == len(rows), "Le fichier a un en-tête : option header.")

    actifs = None
    if df is not None:
        actifs = essayer("TODO 2 : actifs + département", lambda: n1.actifs_avec_departement(df), "Regarde filter et withColumn.")
    if actifs is not None:
        controle("TODO 2 : seuls les actifs sont gardés", actifs.count() == len(attendus),
                 f"{len(attendus)} actifs attendus, {actifs.count()} obtenus.")
        ok = "departement" in actifs.columns and actifs.filter("departement != substring(code_postal, 1, 2)").count() == 0
        controle("TODO 2 : departement = 2 premiers caractères du code postal", ok,
                 "Une colonne `departement` avec F.substring(...) ou .substr(1, 2).")

    if actifs is not None and "departement" in actifs.columns:
        cpt = essayer("TODO 3 : comptage", lambda: n1.compter_par_departement(actifs), "groupBy puis count.")
        if cpt is not None:
            attendu = Counter(r["code_postal"][:2] for r in attendus)
            res = [(r["departement"], r["nb"]) for r in cpt.collect()] if {"departement", "nb"} <= set(cpt.columns) else []
            controle("TODO 3 : colonnes departement et nb, valeurs justes", dict(res) == dict(attendu),
                     "Une ligne par département, la colonne de comptage s'appelle `nb` (alias).")
            controle("TODO 3 : tri décroissant sur nb", [n for _, n in res] == sorted([n for _, n in res], reverse=True),
                     "orderBy avec une colonne décroissante.")
            controle("TODO 3 : le plan contient un shuffle (Exchange)", "Exchange" in plan(cpt),
                     "Si c'est faux, tu as sans doute collecté les données avant d'agréger.")

        r = essayer("TODO 4 : écriture Parquet", lambda: (n1.ecrire_parquet(actifs), True)[1], "Regarde df.write.")
        if r:
            sortie = Path(n1.SORTIE)
            parts = sorted(p.name for p in sortie.glob("departement=*")) if sortie.exists() else []
            controle("TODO 4 : un dossier par département (partitionBy)", len(parts) == len({r['code_postal'][:2] for r in rows}),
                     "partitionBy(\"departement\") avant parquet(...).")
            relu = spark.read.parquet(str(sortie)).count() if parts else -1
            controle("TODO 4 : on relit autant de lignes que d'actifs", relu == len(attendus), f"Relu : {relu}, attendu : {len(attendus)}.")
    return bilan(1)


def niveau2():
    print("Niveau 2 : jointure par diffusion\n")
    from lab import niveau2 as n2
    spark = session()
    essayer("configurer", lambda: n2.configurer(spark), "Une option de configuration Spark SQL.")
    val = int(spark.conf.get("spark.sql.shuffle.partitions"))
    controle("Partitions de shuffle <= 8", val <= 8, f"Valeur actuelle : {val}. spark.conf.set(...)")

    df = essayer("jointure", lambda: n2.etablissements_avec_libelle(spark), "Une jointure sur code_naf.")
    if df is not None:
        n = df.count()
        controle("Les 40 000 établissements sont conservés", n == 40000, f"{n} lignes. Une jointure interne perd ce qui n'a pas de correspondance : lequel choisir ?")
        controle("Colonne libelle renseignée", "libelle" in df.columns and df.filter("libelle is null").count() == 0,
                 "Colonne `libelle` absente, ou vide sur certaines lignes.")
        p = plan(df)
        controle("Plan : BroadcastHashJoin", "BroadcastHashJoin" in p,
                 "Dis à Spark quel côté est petit : fonction broadcast() de pyspark.sql.functions.")
        controle("Plan : aucun SortMergeJoin", "SortMergeJoin" not in p, "Une jointure par tri-fusion déplace les deux côtés.")
        controle("Plan : aucun Exchange hashpartitioning (shuffle du gros côté)", "hashpartitioning" not in p,
                 "Une diffusion remplace le shuffle : exécute df.explain() et lis-le de bas en haut.")
    return bilan(2)


def niveau3():
    print("Niveau 3 : top NAF par département (ticket)\n")
    try:
        from lab import niveau3 as n3
    except ImportError:
        print("  Crée lab/niveau3.py avec une fonction  produire(spark) -> DataFrame  (voir README).")
        return 1
    spark = session()
    rows = lignes()
    cpt = Counter((r["code_postal"][:2], r["code_naf"]) for r in rows if r["etat_administratif"] == "A")
    meilleur = {}
    for (dep, naf), nb in cpt.items():
        if dep not in meilleur or (nb, [-ord(c) for c in naf]) > (meilleur[dep][1], [-ord(c) for c in meilleur[dep][0]]):
            meilleur[dep] = (naf, nb)
    attendu = {(d, n, nb) for d, (n, nb) in meilleur.items()}

    df = essayer("produire(spark)", lambda: n3.produire(spark), "La fonction doit retourner un DataFrame.")
    if df is not None:
        controle("Colonnes : departement, code_naf, libelle, nb", df.columns == ["departement", "code_naf", "libelle", "nb"],
                 f"Colonnes obtenues : {df.columns}")
        obtenu = {(r["departement"], r["code_naf"], r["nb"]) for r in df.collect()} if {"departement", "code_naf", "nb"} <= set(df.columns) else set()
        controle("Une ligne par département, avec le bon NAF et le bon comptage", obtenu == attendu,
                 f"{len(obtenu)} lignes obtenues, {len(attendu)} attendues. Égalité de comptage : prends le plus petit code_naf. Seuls les établissements actifs comptent.")
        controle("Le libellé vient du référentiel", df.filter("libelle is null").count() == 0 and df.filter("libelle not like 'Activité %'").count() == 0,
                 "Jointure avec referentiel_naf.csv.")
        controle("Une fonction de fenêtre est utilisée (Window dans le plan)", "Window" in plan(df),
                 "Pour garder la première ligne de chaque groupe : row_number() sur une fenêtre partitionnée.")
        sortie = DATA / "sortie" / "top_naf"
        fichiers = list(sortie.glob("*.parquet")) if sortie.exists() else []
        controle("Sortie : exactement un fichier Parquet dans data/sortie/top_naf", len(fichiers) == 1,
                 f"{len(fichiers)} fichier(s). Le résultat est minuscule : pourquoi vaut-il mieux un seul fichier ? coalesce ou repartition ?")
    return bilan(3)


if __name__ == "__main__":
    n = sys.argv[1] if len(sys.argv) > 1 else ""
    if n not in {"1", "2", "3"}:
        print(__doc__)
        sys.exit(2)
    sys.exit({"1": niveau1, "2": niveau2, "3": niveau3}[n]())
