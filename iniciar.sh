#!/usr/bin/env bash
set -e

# Usar el Compose de esta carpeta incluso al invocar el script desde otra ruta.
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

echo "==> 1. Verificando Docker..."
docker --version
docker compose version >/dev/null 2>&1 || { echo "   Falta 'docker compose'."; exit 1; }

echo "==> 2. Levantando Kafka..."
docker compose up -d

echo "==> 3. Esperando a que Kafka esté listo..."
intento=0
until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092 >/dev/null 2>&1; do
  intento=$((intento + 1))
  if [ "$intento" -ge 30 ]; then
    echo "Kafka no respondió después de 30 intentos. Revisa: docker compose logs kafka"
    exit 1
  fi
  sleep 2
done

echo "==> 4. Estado:"
docker compose ps

echo "==> 5. Creando el topic 'flota' (si no existe)..."
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --topic flota \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

echo "==> 6. Topics existentes:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --list --bootstrap-server localhost:9092

echo ""
echo "============================================================"
echo " Kafka listo en localhost:9092"
echo " Topic 'flota' disponible."
echo ""
echo " Prueba (en tu máquina, con Python):"
echo "   uv sync"
echo "   uv run productor_flota.py"
echo ""
echo " Para apagar:  docker compose down"
echo "============================================================"
