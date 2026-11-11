"""Génère les données du labo E3 : des établissements au format SIRENE simplifié (synthétiques).

    python donnees/generer.py

Produit data/etablissements.csv (40 000 lignes) et data/referentiel_naf.csv (30 codes).
Une partie des établissements est volontairement concentrée sur quelques codes NAF
et codes postaux : c'est ce qui rend les agrégations et les jointures intéressantes.
"""
import csv
import random
from datetime import date, timedelta
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
DATA = RACINE / "data"
NB = 40_000
CODES_NAF = [f"{n:02d}.{s:02d}Z" for n, s in [
    (47, 11), (47, 91), (56, 10), (56, 30), (62, 1), (62, 2), (68, 20), (68, 31), (69, 10), (70, 22),
    (41, 20), (43, 21), (43, 22), (45, 20), (46, 90), (49, 41), (52, 29), (55, 10), (64, 19), (66, 22),
    (71, 12), (73, 11), (74, 10), (77, 11), (78, 10), (82, 11), (85, 59), (86, 21), (90, 1), (96, 2)]]
CODES_POSTAUX = ["75001", "75011", "75015", "92100", "92200", "92400", "93100", "94300",
                 "78000", "78100", "91000", "95000", "13001", "69001", "31000", "33000"]


def main():
    DATA.mkdir(exist_ok=True)
    rng = random.Random(2026)
    with open(DATA / "referentiel_naf.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["code_naf", "libelle"])
        for c in CODES_NAF:
            w.writerow([c, f"Activité {c}"])
    # 40 % des établissements sur 2 codes NAF seulement : une distribution déséquilibrée (skew)
    poids_naf = [30, 10] + [60 / 28] * 28
    poids_cp = [20, 15] + [65 / 14] * 14
    debut = date(1995, 1, 1)
    with open(DATA / "etablissements.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["siret", "code_naf", "code_postal", "etat_administratif", "date_creation"])
        for i in range(NB):
            siret = f"{rng.randint(10**13, 10**14 - 1)}"
            w.writerow([
                siret,
                rng.choices(CODES_NAF, weights=poids_naf)[0],
                rng.choices(CODES_POSTAUX, weights=poids_cp)[0],
                rng.choices(["A", "F"], weights=[80, 20])[0],
                (debut + timedelta(days=rng.randint(0, 11_000))).isoformat(),
            ])
    print(f"data/etablissements.csv : {NB} lignes ; data/referentiel_naf.csv : {len(CODES_NAF)} codes")


if __name__ == "__main__":
    main()
