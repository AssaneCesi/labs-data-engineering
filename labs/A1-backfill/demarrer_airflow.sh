#!/usr/bin/env bash
# Démarre Airflow en arrière-plan (lancé automatiquement à chaque ouverture du codespace).
cd "$(dirname "$0")"
if pgrep -f "airflow standalone" > /dev/null; then
  echo "Airflow tourne déjà."
  exit 0
fi
mkdir -p airflow_home
setsid nohup airflow standalone > airflow_home/standalone.log 2>&1 < /dev/null &
echo "Airflow démarre (environ 1 minute). Interface : onglet PORTS, port 8080. Journal : airflow_home/standalone.log"
