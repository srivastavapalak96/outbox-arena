#!/usr/bin/env bash
# Week 5 compensation scenario A: payment fails -> CANCELLED (no compensation needed).
#
# Posts an order whose total exceeds the payment-service stub gateway's authorise
# ceiling (100,000 cents). Expected: orders.status reaches CANCELLED, payment row is
# FAILED, no inventory reserved, no shipment attempted.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEADLINE_SECONDS="${DEADLINE_SECONDS:-30}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

SERVICES=(order-service:8081 payment-service:8082 inventory-service:8083 shipping-service:8084 notifier-sink:8086)
LOG_DIR=".local/compensation-payment-fail"
mkdir -p "$LOG_DIR"

cleanup() {
  for entry in "${SERVICES[@]}"; do
    svc="${entry%:*}"
    pkill -f "io.outboxarena.${svc%-*}" 2>/dev/null || true
    pkill -f "gradle.*:modules:${svc}:bootRun" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "--- 1. infra up"
docker compose -f infra/docker-compose.yml up -d postgres kafka >/dev/null
for i in $(seq 1 60); do
  if docker exec outbox-postgres pg_isready -U postgres >/dev/null 2>&1 \
     && docker exec outbox-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1; then
    break
  fi
  [ "$i" -eq 60 ] && echo "infra not ready" && exit 1
  sleep 1
done

echo "--- 2. start services"
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

echo "--- 3. POST order with total > 100,000 cents (triggers PaymentFailed)"
# Two items at 80,000c each = 160,000c total -- exceeds the 100,000c ceiling.
order_uuid="$(curl -fsS -X POST http://localhost:8081/orders \
    -H 'Content-Type: application/json' \
    -d '{"buyerId":"buyer-payfail","currency":"USD","items":[{"sellerId":"seller-a","sku":"sku-1","qty":1,"unitPriceCents":80000},{"sellerId":"seller-a","sku":"sku-1","qty":1,"unitPriceCents":80000}]}' \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderUuid"])')"
echo "  order $order_uuid posted"

echo "--- 4. poll orders.status until CANCELLED (deadline ${DEADLINE_SECONDS}s)"
final_state=""
for i in $(seq 1 "$DEADLINE_SECONDS"); do
  final_state="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
      "SELECT status FROM orders WHERE order_uuid='${order_uuid}'")"
  echo "  t+${i}s status=${final_state}"
  if [ "$final_state" = "CANCELLED" ]; then
    break
  fi
  sleep 1
done

if [ "$final_state" != "CANCELLED" ]; then
  echo "FAILED: expected CANCELLED, got '${final_state}'"
  exit 1
fi

echo "--- 5. assert clean state (FAILED payment, no inventory reservations, no shipment)"
payment_status="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c "SELECT status FROM payments WHERE order_uuid='${order_uuid}'")"
inventory_count="$(docker exec outbox-postgres psql -U inventory_svc -d inventory_svc -t -A -c "SELECT count(*) FROM inventory_reservations WHERE order_uuid='${order_uuid}'")"
shipment_count="$(docker exec outbox-postgres psql -U shipping_svc -d shipping_svc -t -A -c "SELECT count(*) FROM shipments WHERE order_uuid='${order_uuid}'")"

echo "  payment_status=${payment_status} expected=FAILED"
echo "  inventory_reservations=${inventory_count} expected=0"
echo "  shipments=${shipment_count} expected=0"

if [ "$payment_status" != "FAILED" ] || [ "$inventory_count" != "0" ] || [ "$shipment_count" != "0" ]; then
  echo "FAILED: state did not match expectations"
  exit 1
fi

echo "payment-fail compensation verified -- order ${order_uuid} CANCELLED with clean state"
