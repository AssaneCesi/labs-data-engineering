#!/usr/bin/env bash
# Installation du labo, lancée une seule fois à la création du codespace.
set -euo pipefail
cd "$(dirname "$0")"
pip install --no-cache-dir --quiet "pyspark==3.5.3" duckdb
python donnees/generer.py
echo "Labo prêt. Ouvre README.md."
