#!/usr/bin/env bash
# Week 6 scenario: Debezium tails order_svc.orders -> projection-service materialises
# the order_views read model. Validates the command-plane / data-plane boundary
# (ADR-0002) by showing the same business fact reaching two different consumers via
# two different mechanisms.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEADLINE_SECONDS="${DEADLINE_SECONDS:-45}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

# All six services (including projection-service) -- this is the first scenario where
# projection-service actually participates.
SERVICES=(order-service:8081 payment-service:8082 inventory-service:8083 shipping-service:8084 notifier-sink:8086 projection-service:8095)
LOG_DIR=".local/cdc-projection"
mkdir -p "$LOG_DIR"

cleanup() {
  for entry in "${SERVICES[@]}"; do
    svc="${entry%:*}"
    pkill -f "io.outboxarena.${svc%-*}" 2>/dev/null || true
    pkill -f "gradle.*:modules:${svc}:bootRun" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "--- 1. infra up (postgres + kafka + kafka-connect)"
docker compose -f infra/docker-compose.yml up -d postgres kafka kafka-connect >/dev/null
for i in $(seq 1 60); do
  if docker exec outbox-postgres pg_isready -U postgres >/dev/null 2>&1 \
     && docker exec outbox-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1 \
     && curl -fsS http://localhost:18083/ >/dev/null 2>&1; then
    break
  fi
  [ "$i" -eq 60 ] && echo "infra not ready" && exit 1
  sleep 1
done
echo "  infra ready"

echo "--- 2. start six services FIRST so Flyway creates the orders table before Debezium tries to subscribe"
for entry in "${SERVICES[@]}"; do
  svc="${entry%:*}"
  ./gradlew ":modules:${svc}:bootRun" --no-daemon > "$LOG_DIR/${svc}.log" 2>&1 &
done

for entry in "${SERVICES[@]}"; do
  port="${entry#*:}"
  svc="${entry%:*}"
  for i in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${port}/actuator/health" || true)"
    [ "$code" = "200" ] && echo "  $svc up" && break
    [ "$i" -eq 120 ] && echo "$svc did not come up" && tail -30 "$LOG_DIR/${svc}.log" && exit 1
    sleep 1
  done
done

echo "--- 3a. pre-create the publication as the order_svc role (table owner)"
docker exec -e PGPASSWORD=order_svc outbox-postgres psql -U order_svc -d order_svc -c \
    "CREATE PUBLICATION outbox_arena_pub FOR TABLE public.orders, public.order_items;" 2>&1 | head -5

echo "--- 3b. register Debezium connector (publication already exists, autocreate disabled)"
register_response="$(curl -fsS -X POST -H 'Content-Type: application/json' \
    --data @infra/kafka-connect/debezium-connector.json \
    http://localhost:18083/connectors 2>&1 || true)"
echo "  connector registration: $(echo "$register_response" | head -c 200)"

# Wait for the connector AND its task to reach RUNNING state. A connector can report
# RUNNING while its underlying task has failed -- we need both.
for i in $(seq 1 30); do
  status_json="$(curl -fsS http://localhost:18083/connectors/outbox-arena-cdc/status 2>/dev/null || echo '{}')"
  connector_state="$(echo "$status_json" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("connector",{}).get("state","unknown"))' 2>/dev/null || echo "unknown")"
  task_state="$(echo "$status_json" | python3 -c 'import sys,json; d=json.load(sys.stdin); tasks=d.get("tasks",[]); print(tasks[0]["state"] if tasks else "no-task")' 2>/dev/null || echo "unknown")"
  echo "  connector=${connector_state} task=${task_state}"
  if [ "$connector_state" = "RUNNING" ] && [ "$task_state" = "RUNNING" ]; then
    break
  fi
  if [ "$task_state" = "FAILED" ]; then
    echo "FAILED: connector task crashed -- detail:"
    echo "$status_json" | python3 -m json.tool 2>/dev/null | head -40
    exit 1
  fi
  [ "$i" -eq 30 ] && echo "connector never became RUNNING+RUNNING" && exit 1
  sleep 1
done

echo "--- 4. POST sample order"
order_uuid="$(curl -fsS -X POST http://localhost:8081/orders \
    -H 'Content-Type: application/json' \
    -d @samples/sample-order.json | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderUuid"])')"
echo "  order $order_uuid created"

echo "--- 5. poll projection-service /views/orders/{uuid} until present (deadline ${DEADLINE_SECONDS}s)"
projection_status=""
for i in $(seq 1 "$DEADLINE_SECONDS"); do
  resp="$(curl -fsS "http://localhost:8095/views/orders/${order_uuid}" 2>/dev/null || echo "")"
  if [ -n "$resp" ]; then
    projection_status="$(echo "$resp" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || echo "")"
    echo "  t+${i}s projection.status=${projection_status}"
    if [ "$projection_status" = "COMPLETED" ]; then
      echo "CDC projection verified -- order ${order_uuid} reached COMPLETED in the read model"
      exit 0
    fi
  else
    echo "  t+${i}s projection: 404 (not yet)"
  fi
  sleep 1
done

echo "FAILED: projection did not reach COMPLETED in ${DEADLINE_SECONDS}s; last status=${projection_status}"
echo "--- last 20 lines of projection-service log:"
tail -20 "$LOG_DIR/projection-service.log" || true
exit 1
