#!/usr/bin/env bash
# Week 4 end-to-end happy-path scenario.
#
# Brings up the infra stack (docker compose) + all six services (gradle bootRun in
# background), POSTs samples/sample-order.json to order-service, then polls the order_svc
# sagas table until state=COMPLETED. Exits non-zero on timeout or wrong terminal state.
#
# Usage:
#   ./chaos/scenarios/happy-path.sh         # default 30s deadline
#   DEADLINE_SECONDS=60 ./chaos/scenarios/happy-path.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEADLINE_SECONDS="${DEADLINE_SECONDS:-30}"
JAVA_HOME_OVERRIDE="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export JAVA_HOME="$JAVA_HOME_OVERRIDE"
# Services bootRun from the host machine connect via the EXTERNAL listener on
# 29092; the broker's PLAINTEXT listener (kafka:9092) only resolves inside the
# docker network.
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

# Saga happy path doesn't need projection-service -- that's the week-6 CDC consumer.
# notifier-sink is included because it tails OrderCompleted.
SERVICES=(order-service:8081 payment-service:8082 inventory-service:8083 shipping-service:8084 notifier-sink:8086)
LOG_DIR=".local/happy-path"
mkdir -p "$LOG_DIR"

cleanup() {
  echo "--- cleaning up bootRun processes"
  for entry in "${SERVICES[@]}"; do
    svc="${entry%:*}"
    pkill -f "io.outboxarena.${svc%-*}" 2>/dev/null || true
    pkill -f "gradle.*:modules:${svc}:bootRun" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "--- 1. infra up (postgres + kafka)"
docker compose -f infra/docker-compose.yml up -d postgres kafka >/dev/null

echo "--- 2. wait for infra"
for i in $(seq 1 60); do
  if docker exec outbox-postgres pg_isready -U postgres >/dev/null 2>&1 \
     && docker exec outbox-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1; then
    break
  fi
  [ "$i" -eq 60 ] && echo "infra not ready" && exit 1
  sleep 1
done

echo "--- 3. start six services (background)"
for entry in "${SERVICES[@]}"; do
  svc="${entry%:*}"
  port="${entry#*:}"
  echo "  starting $svc on :$port"
  ./gradlew ":modules:${svc}:bootRun" --no-daemon > "$LOG_DIR/${svc}.log" 2>&1 &
done

echo "--- 4. wait for all six /actuator/health 200"
for entry in "${SERVICES[@]}"; do
  port="${entry#*:}"
  svc="${entry%:*}"
  for i in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${port}/actuator/health" || true)"
    [ "$code" = "200" ] && echo "  $svc up" && break
    [ "$i" -eq 120 ] && echo "$svc did not come up in 120s -- tail of log:" && tail -50 "$LOG_DIR/${svc}.log" && exit 1
    sleep 1
  done
done

echo "--- 5. POST sample order"
order_uuid="$(curl -fsS -X POST http://localhost:8081/orders \
    -H 'Content-Type: application/json' \
    -d @samples/sample-order.json | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderUuid"])')"
echo "  order $order_uuid created"

echo "--- 6. poll orders.status until COMPLETED (deadline ${DEADLINE_SECONDS}s)"
# The orchestrator updates orders.status as it advances the saga. The sagas row
# is currently a placeholder; week 5 will use it for compensation tracking.
final_state=""
for i in $(seq 1 "$DEADLINE_SECONDS"); do
  final_state="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
      "SELECT status FROM orders WHERE order_uuid='${order_uuid}'")"
  echo "  t+${i}s status=${final_state}"
  if [ "$final_state" = "COMPLETED" ]; then
    echo "happy path verified -- order ${order_uuid} reached COMPLETED in ${i}s"
    exit 0
  fi
  sleep 1
done

echo "saga did not complete in ${DEADLINE_SECONDS}s; last state=${final_state}"
echo "--- last 30 lines of each service log:"
for entry in "${SERVICES[@]}"; do
  svc="${entry%:*}"
  echo "###### $svc"
  tail -30 "$LOG_DIR/${svc}.log" || true
done
exit 1
