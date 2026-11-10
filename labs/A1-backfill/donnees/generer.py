"""Génère les fichiers quotidiens du labo : des établissements au format SIRENE simplifié.

    python donnees/generer.py                       # 7 fichiers, du 1er au 7 novembre 2026
    python donnees/generer.py --corriger 2026-11-03 # le producteur republie le fichier corrigé

Le fichier du 3 novembre est volontairement tronqué au départ (60 % des lignes) :
c'est l'incident du niveau 3. Les données sont synthétiques, sans aucune personne réelle.
"""
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
ENTRANT = RACINE / "data" / "entrant"
PREMIER_JOUR = date(2026, 11, 1)
NB_JOURS = 7
JOUR_TRONQUE = date(2026, 11, 3)
CODES_POSTAUX = ["75001", "75011", "75015", "92100", "92200", "92400", "93100", "94300"]


def _siret(rng):
    """14 chiffres ; la clé de contrôle n'est pas le sujet de ce labo."""
    return "".join(str(rng.randint(0, 9)) for _ in range(14))


def lignes_du_jour(jour):
    """Contenu complet d'un jour : même graine pour un même jour, donc reproductible."""
    rng = random.Random(jour.toordinal())
    n = 1800 + rng.randint(0, 400)
    return [
        {"siret": _siret(rng),
         "code_postal": rng.choice(CODES_POSTAUX),
         "etat_administratif": rng.choices(["A", "F"], weights=[85, 15])[0]}
        for _ in range(n)
    ]


def ecrire(jour, lignes):
    ENTRANT.mkdir(parents=True, exist_ok=True)
    chemin = ENTRANT / f"etablissements_{jour:%Y-%m-%d}.csv"
    with open(chemin, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["siret", "code_postal", "etat_administratif"])
        w.writeheader()
        w.writerows(lignes)
    return chemin, len(lignes)


def generer_tout():
    for i in range(NB_JOURS):
        jour = PREMIER_JOUR + timedelta(days=i)
        lignes = lignes_du_jour(jour)
        if jour == JOUR_TRONQUE:
            lignes = lignes[: int(len(lignes) * 0.6)]      # fichier tronqué par le producteur
        chemin, n = ecrire(jour, lignes)
        print(f"{chemin.name} : {n} lignes" + ("  <- tronqué" if jour == JOUR_TRONQUE else ""))
    marqueur = ENTRANT / f".correction_{JOUR_TRONQUE:%Y-%m-%d}"
    marqueur.unlink(missing_ok=True)


def corriger(jour):
    chemin, n = ecrire(jour, lignes_du_jour(jour))
    (ENTRANT / f".correction_{jour:%Y-%m-%d}").write_text(datetime.now().isoformat())
    print(f"{chemin.name} republié par le producteur : {n} lignes (fichier complet)")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--corriger", type=date.fromisoformat, help="republier le fichier complet de ce jour")
    a = p.parse_args()
    corriger(a.corriger) if a.corriger else generer_tout()
