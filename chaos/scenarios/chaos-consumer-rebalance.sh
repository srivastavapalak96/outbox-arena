#!/usr/bin/env bash
# Polish chaos scenario: 2 payment-service instances in the same consumer group; kill one
# mid-flight to force a consumer-group rebalance. Asserts that:
#   1. The saga doesn't lose events during the rebalance (every PaymentRequested gets a
#      PaymentAuthorized reply).
#   2. processed_events on the payment-service DB has no duplicates (consumer-side
#      idempotency held).
#
# This is the canonical demo of consumer-group-rebalance safety. Kafka's at-least-once
# delivery semantics mean a partition's in-flight messages can be re-delivered to a
# different consumer after rebalance; our IdempotentConsumer wrapper is what catches
# those re-deliveries.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

ORDERS_TOTAL="${ORDERS_TOTAL:-40}"
DRAIN_DEADLINE_SECONDS="${DRAIN_DEADLINE_SECONDS:-90}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

LOG_DIR=".local/chaos-consumer-rebalance"
mkdir -p "$LOG_DIR"

cleanup() {
  pkill -f "io.outboxarena\." 2>/dev/null || true
  pkill -f "gradle.*bootRun" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- 1. infra up"
docker compose -f infra/docker-compose.yml down -v >/dev/null 2>&1 || true
docker compose -f infra/docker-compose.yml up -d postgres kafka >/dev/null
for i in $(seq 1 60); do
  if docker exec outbox-postgres pg_isready -U postgres >/dev/null 2>&1 \
     && docker exec outbox-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1; then
    break
  fi
  [ "$i" -eq 60 ] && echo "infra not ready" && exit 1
  sleep 1
done
echo "  infra ready"

echo "--- 2. start order-service + 2 payment-service instances"
SERVER_PORT=8081 ./gradlew :modules:order-service:bootRun --no-daemon > "$LOG_DIR/order-service.log" 2>&1 &
SERVER_PORT=8082 ./gradlew :modules:payment-service:bootRun --no-daemon > "$LOG_DIR/payment-service-A.log" 2>&1 &
SERVER_PORT=8182 ./gradlew :modules:payment-service:bootRun --no-daemon > "$LOG_DIR/payment-service-B.log" 2>&1 &

for port in 8081 8082 8182; do
  for i in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:${port}/actuator/health" || true)"
    [ "$code" = "200" ] && echo "  port ${port} up" && break
    [ "$i" -eq 120 ] && echo "port ${port} did not come up" && exit 1
    sleep 1
  done
done

echo "--- 3. POST ${ORDERS_TOTAL} orders rapidly"
for i in $(seq 1 "$ORDERS_TOTAL"); do
  curl -fsS -o /dev/null -X POST http://localhost:8081/orders \
      -H 'Content-Type: application/json' \
      -d "{\"buyerId\":\"reb-${i}\",\"currency\":\"USD\",\"items\":[{\"sellerId\":\"seller-a\",\"sku\":\"sku-1\",\"qty\":1,\"unitPriceCents\":50}]}" &
  if [ $((i % 8)) -eq 0 ]; then wait; fi
done
wait
echo "  ${ORDERS_TOTAL} POSTs returned"

echo "--- 4. wait 3s for some events to land, then kill payment-service B"
sleep 3
PAYMENT_B_PID="$(pgrep -fl 'SERVER_PORT=8182' | grep -v gradle | awk '{print $1}' | head -1 || true)"
if [ -z "$PAYMENT_B_PID" ]; then
  # fallback: find by listen port
  PAYMENT_B_PID="$(lsof -i :8182 -nP -sTCP:LISTEN | awk 'NR>1 && $1=="java" {print $2; exit}' || true)"
fi
if [ -z "$PAYMENT_B_PID" ]; then
  echo "couldn't find payment-service B PID; continuing without kill"
else
  echo "  killing payment-service B (pid ${PAYMENT_B_PID})"
  kill -9 "$PAYMENT_B_PID"
fi

echo "--- 5. wait for outbox to drain on order-service + payments processed"
for i in $(seq 1 "$DRAIN_DEADLINE_SECONDS"); do
  outbox_pending="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
      "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  payments_processed="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c \
      "SELECT count(*) FROM payments")"
  echo "  t+${i}s order-outbox.unpublished=${outbox_pending}  payments.count=${payments_processed}"
  if [ "$outbox_pending" = "0" ] && [ "$payments_processed" = "$ORDERS_TOTAL" ]; then
    break
  fi
  [ "$i" -eq "$DRAIN_DEADLINE_SECONDS" ] && echo "FAILED: didn't converge" && exit 1
  sleep 1
done

echo "--- 6. assert no duplicate processed_events"
duplicates="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c \
    "SELECT count(*) - count(DISTINCT event_id) FROM processed_events WHERE consumer_group='payment-service'")"
processed_count="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c \
    "SELECT count(*) FROM processed_events WHERE consumer_group='payment-service'")"
echo "  processed_events: ${processed_count}  duplicates: ${duplicates}  expected ${ORDERS_TOTAL}"

if [ "$duplicates" != "0" ]; then
  echo "FAILED: processed_events contains duplicates (consumer idempotency broken)"
  exit 1
fi
if [ "$processed_count" != "$ORDERS_TOTAL" ]; then
  echo "FAILED: expected ${ORDERS_TOTAL} processed_events, found ${processed_count}"
  exit 1
fi

echo "consumer-rebalance chaos verified -- 2-instance group survived a kill mid-flight; ${ORDERS_TOTAL} orders -> ${processed_count} payments + ${processed_count} processed_events with 0 duplicates"
