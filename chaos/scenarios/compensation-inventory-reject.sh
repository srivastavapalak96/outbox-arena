#!/usr/bin/env bash
# Week 5 compensation scenario B: inventory insufficient -> refund -> CANCELLED.
#
# Posts an order requesting more stock than what's seeded. Payment authorises,
# inventory rejects, orchestrator emits PaymentRefundRequested, payment refunds,
# orchestrator transitions to CANCELLED. Expected end state: orders.status=CANCELLED,
# payment row REFUNDED, no reservations held.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEADLINE_SECONDS="${DEADLINE_SECONDS:-30}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

SERVICES=(order-service:8081 payment-service:8082 inventory-service:8083 shipping-service:8084 notifier-sink:8086)
LOG_DIR=".local/compensation-inventory-reject"
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

echo "--- 3. POST order requesting more stock than seeded (1001 of a SKU stocked to 1000)"
order_uuid="$(curl -fsS -X POST http://localhost:8081/orders \
    -H 'Content-Type: application/json' \
    -d '{"buyerId":"buyer-invreject","currency":"USD","items":[{"sellerId":"seller-a","sku":"sku-1","qty":1001,"unitPriceCents":50}]}' \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderUuid"])')"
echo "  order $order_uuid posted (1001 units of sku-1 from seller-a, total 50,050c)"

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

echo "--- 5. assert REFUNDED payment, no held reservations, no shipment"
payment_status="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c "SELECT status FROM payments WHERE order_uuid='${order_uuid}'")"
held_count="$(docker exec outbox-postgres psql -U inventory_svc -d inventory_svc -t -A -c "SELECT count(*) FROM inventory_reservations WHERE order_uuid='${order_uuid}' AND status='HELD'")"
shipment_count="$(docker exec outbox-postgres psql -U shipping_svc -d shipping_svc -t -A -c "SELECT count(*) FROM shipments WHERE order_uuid='${order_uuid}'")"

echo "  payment_status=${payment_status} expected=REFUNDED"
echo "  inventory_reservations_held=${held_count} expected=0"
echo "  shipments=${shipment_count} expected=0"

if [ "$payment_status" != "REFUNDED" ] || [ "$held_count" != "0" ] || [ "$shipment_count" != "0" ]; then
  echo "FAILED: state did not match expectations"
  exit 1
fi

echo "inventory-reject compensation verified -- order ${order_uuid} CANCELLED + REFUNDED + no held stock"
