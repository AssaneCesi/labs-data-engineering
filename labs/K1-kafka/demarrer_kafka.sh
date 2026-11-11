#!/usr/bin/env bash
# Démarre un broker Kafka seul (mode KRaft, sans ZooKeeper) en arrière-plan. Relançable sans risque.
cd "$(dirname "$0")"
if (echo > /dev/tcp/localhost/9092) 2>/dev/null; then echo "Kafka tourne déjà."; exit 0; fi
export KAFKA_HEAP_OPTS="-Xmx512m -Xms512m"
CONF=kafka/config/kraft/server.properties
LOGS="$PWD/data/kafka-logs"
mkdir -p data
sed -i "s#^log.dirs=.*#log.dirs=${LOGS}#" "$CONF"
if [ ! -f "${LOGS}/meta.properties" ]; then
  kafka/bin/kafka-storage.sh format -t "$(kafka/bin/kafka-storage.sh random-uuid)" -c "$CONF" > data/kafka-format.log 2>&1
fi
setsid nohup kafka/bin/kafka-server-start.sh "$CONF" > data/kafka.log 2>&1 < /dev/null &
for i in $(seq 1 30); do
  (echo > /dev/tcp/localhost/9092) 2>/dev/null && { echo "Kafka prêt sur localhost:9092"; exit 0; }
  sleep 1
done
echo "Kafka ne répond pas : regarde data/kafka.log"; exit 1
