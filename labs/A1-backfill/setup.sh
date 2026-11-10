#!/usr/bin/env bash
# Installation du labo, lancée une seule fois à la création du codespace.
set -euo pipefail
cd "$(dirname "$0")"
AIRFLOW_VERSION=3.3.2
PY=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Installation d'Airflow ${AIRFLOW_VERSION} (Python ${PY}), environ 2 minutes..."
pip install --no-cache-dir --quiet "apache-airflow==${AIRFLOW_VERSION}" duckdb \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"
python donnees/generer.py
airflow db migrate > /dev/null
echo "Labo prêt. Ouvre README.md."
