#!/usr/bin/env bash
# Week 5 compensation scenario C: shipment fails -> release + refund -> CANCELLED.
#
# Forces shipping-service to fail dispatch by setting SHIPPING_FORCE_FAIL_PREFIX to the
# first byte of the order UUID we expect to mint. (UUIDs are random, so we set the prefix
# to the empty-but-non-blank string "0" and post until we get a 0-prefixed order. That
# would be flakey -- instead we force-fail every order in this run by setting prefix="".)
#
# Actually the simplest deterministic approach: set the force-fail prefix to a single
# character that every UUID's first character is one of {0..f}. We pick "f" and post
# orders until the orchestrator generates one starting with "f" (~1-in-16 chance, so
# usually 1-3 attempts). This is the cleanest way to keep the test fully self-contained
# without adding API surface to force individual orders to fail.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DEADLINE_SECONDS="${DEADLINE_SECONDS:-30}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"
# Force the shipping-service stub to fail any order whose UUID starts with "f".
# Force-fail any order whose UUID starts with 0-7 (half the hex space) so we hit the
# failure path on the first attempt with very high probability.
export SHIPPING_FORCE_FAIL_PREFIX="0,1,2,3,4,5,6,7"

SERVICES=(order-service:8081 payment-service:8082 inventory-service:8083 shipping-service:8084 notifier-sink:8086)
LOG_DIR=".local/compensation-shipment-fail"
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

echo "--- 2. start services (shipping-service runs with SHIPPING_FORCE_FAIL_PREFIX=f via SPRING_APPLICATION_JSON)"
for entry in "${SERVICES[@]}"; do
  svc="${entry%:*}"
  if [ "$svc" = "shipping-service" ]; then
    SPRING_APPLICATION_JSON="{\"shipping\":{\"force-fail-prefix\":\"${SHIPPING_FORCE_FAIL_PREFIX}\"}}" \
      ./gradlew ":modules:${svc}:bootRun" --no-daemon > "$LOG_DIR/${svc}.log" 2>&1 &
  else
    ./gradlew ":modules:${svc}:bootRun" --no-daemon > "$LOG_DIR/${svc}.log" 2>&1 &
  fi
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

echo "--- 3. POST orders until one mints with UUID starting in [0-7]"
order_uuid=""
for attempt in $(seq 1 30); do
  candidate="$(curl -fsS -X POST http://localhost:8081/orders \
      -H 'Content-Type: application/json' \
      -d '{"buyerId":"buyer-shipfail","currency":"USD","items":[{"sellerId":"seller-a","sku":"sku-1","qty":1,"unitPriceCents":50}]}' \
      | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderUuid"])')"
  if [[ "$candidate" =~ ^[0-7] ]]; then
    order_uuid="$candidate"
    echo "  attempt ${attempt}: matched -> ${order_uuid}"
    break
  fi
  echo "  attempt ${attempt}: ${candidate} (no match)"
done

if [ -z "$order_uuid" ]; then
  echo "FAILED: 30 attempts and no UUID started with [0-7] -- statistically impossible, look for a bug"
  exit 1
fi

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

echo "--- 5. assert REFUNDED payment, RELEASED reservations, FAILED shipment"
payment_status="$(docker exec outbox-postgres psql -U payment_svc -d payment_svc -t -A -c "SELECT status FROM payments WHERE order_uuid='${order_uuid}'")"
held_count="$(docker exec outbox-postgres psql -U inventory_svc -d inventory_svc -t -A -c "SELECT count(*) FROM inventory_reservations WHERE order_uuid='${order_uuid}' AND status='HELD'")"
shipment_status="$(docker exec outbox-postgres psql -U shipping_svc -d shipping_svc -t -A -c "SELECT status FROM shipments WHERE order_uuid='${order_uuid}'")"

echo "  payment_status=${payment_status} expected=REFUNDED"
echo "  inventory_reservations_held=${held_count} expected=0"
echo "  shipment_status=${shipment_status} expected=FAILED"

if [ "$payment_status" != "REFUNDED" ] || [ "$held_count" != "0" ] || [ "$shipment_status" != "FAILED" ]; then
  echo "FAILED: state did not match expectations"
  exit 1
fi

echo "shipment-fail compensation verified -- order ${order_uuid} CANCELLED + REFUNDED + RELEASED + FAILED shipment"
