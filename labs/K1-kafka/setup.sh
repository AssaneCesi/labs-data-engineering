#!/usr/bin/env bash
# Installation du labo, lancée une seule fois à la création du codespace.
set -euo pipefail
cd "$(dirname "$0")"
KAFKA_VERSION=3.9.0
echo "Téléchargement de Kafka ${KAFKA_VERSION} (environ 1 minute)..."
mkdir -p kafka
curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" | tar xz -C kafka --strip-components=1
pip install --no-cache-dir --quiet confluent-kafka duckdb
python donnees/generer.py
echo "Labo prêt. Ouvre README.md."
