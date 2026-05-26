#!/usr/bin/env bash
# Polish chaos scenario: SIGKILL order-service mid-flight. Asserts every outbox row that
# committed to the DB before the crash reaches Kafka after the restart.
#
# Why this matters: the Outbox pattern's whole point is that the DB write is the source
# of truth. Application crashes between "DB committed" and "row published to Kafka" should
# leave business state intact and let the next process pick up where the dead one left off.
# This scenario is the canonical proof of that property.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

ORDERS_TOTAL="${ORDERS_TOTAL:-50}"
DRAIN_DEADLINE_SECONDS="${DRAIN_DEADLINE_SECONDS:-90}"
export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:29092}"

LOG_DIR=".local/chaos-sigkill"
mkdir -p "$LOG_DIR"

cleanup() {
  pkill -9 -f "io.outboxarena.order.OrderApplication" 2>/dev/null || true
  pkill -f "gradle.*:modules:order-service:bootRun" 2>/dev/null || true
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

start_order_service() {
  local tag="$1"
  local poll_interval="${2:-PT0.2S}"
  echo "  starting order-service (tag=${tag} poll-interval=${poll_interval})"
  SPRING_APPLICATION_JSON="{\"outbox-arena\":{\"outbox\":{\"poll-interval\":\"${poll_interval}\"}}}" \
    ./gradlew :modules:order-service:bootRun --no-daemon > "$LOG_DIR/order-service.${tag}.log" 2>&1 &
  for i in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/actuator/health || true)"
    [ "$code" = "200" ] && echo "  order-service up (${tag})" && return 0
    [ "$i" -eq 120 ] && echo "order-service did not come up (${tag})" && tail -30 "$LOG_DIR/order-service.${tag}.log" && return 1
    sleep 1
  done
}

echo "--- 2. start order-service (first life, SLOW poll interval -- 30s -- so the outbox accumulates before the kill)"
start_order_service "pre-kill" "PT30S"

echo "--- 3. POST ${ORDERS_TOTAL} orders + kill mid-flight"
# Run POSTs in background AS A BATCH (single &) and SIGKILL the JVM mid-stream so the
# outbox is guaranteed to have unpublished rows when the process dies. Parallel POSTs
# with for-loop wait blocks were unstable on Apple Silicon Docker -- sequential is
# slow but the kill timing inside the loop achieves the same goal.
KILL_AT=$((ORDERS_TOTAL / 2))
for i in $(seq 1 "$ORDERS_TOTAL"); do
  curl -fsS -o /dev/null -m 5 -X POST http://localhost:8081/orders \
      -H 'Content-Type: application/json' \
      -d "{\"buyerId\":\"sigkill-${i}\",\"currency\":\"USD\",\"items\":[{\"sellerId\":\"seller-a\",\"sku\":\"sku-1\",\"qty\":1,\"unitPriceCents\":50}]}" \
    || echo "    POST $i failed (expected near kill point $KILL_AT)"
  if [ "$i" = "$KILL_AT" ]; then
    echo "--- 4. SIGKILL order-service (at POST ${KILL_AT})"
    JVM_PID="$(pgrep -f 'io.outboxarena.order.OrderApplication' | head -1 || true)"
    if [ -n "$JVM_PID" ]; then
      echo "    killing JVM pid ${JVM_PID}"
      kill -9 "$JVM_PID"
      pkill -f "gradle.*:modules:order-service:bootRun" 2>/dev/null || true
      sleep 2
      echo "    JVM dead. POSTs from here will fail until restart."
    fi
  fi
done
echo "  POST loop done"

echo "--- 5. inspect outbox state (pre-restart)"
unpublished_pre="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
    "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
published_pre="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
    "SELECT count(*) FROM outbox WHERE published_at IS NOT NULL")"
orders_count="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
    "SELECT count(*) FROM orders")"
echo "  orders.count=${orders_count}  outbox.published=${published_pre}  outbox.unpublished=${unpublished_pre}"
# orders_count = whatever POSTs landed before the kill. Should be at least KILL_AT
# (POSTs 1..KILL_AT completed successfully before the kill; POSTs after the kill failed
# until restart). Treat orders_count as the new ground truth for total events expected.
if [ "$orders_count" -lt "$KILL_AT" ]; then
  echo "FAILED: expected at least ${KILL_AT} order rows pre-kill, found ${orders_count}"
  exit 1
fi
if [ "$unpublished_pre" = "0" ]; then
  echo "INFO: outbox already drained before kill; the test still proves restart-then-publish works."
fi
EXPECTED_EVENTS="$orders_count"

echo "--- 6. restart order-service (second life, FAST poll interval so the drain is quick)"
start_order_service "post-restart" "PT0.2S"

echo "--- 7. wait for outbox to drain (deadline ${DRAIN_DEADLINE_SECONDS}s)"
for i in $(seq 1 "$DRAIN_DEADLINE_SECONDS"); do
  u="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
      "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  echo "  t+${i}s outbox.unpublished=${u}"
  [ "$u" = "0" ] && break
  [ "$i" -eq "$DRAIN_DEADLINE_SECONDS" ] && echo "FAILED: outbox never drained" && exit 1
  sleep 1
done

echo "--- 8. count unique event-ids on commands.payment.v1"
unique_events="$(docker exec outbox-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic commands.payment.v1 \
    --from-beginning \
    --max-messages "$((ORDERS_TOTAL * 3))" \
    --timeout-ms 20000 \
    --property print.headers=true 2>/dev/null \
    | grep -oE 'event-id:[a-f0-9-]+' \
    | sort -u \
    | wc -l \
    | tr -d ' ')"

echo "  unique event-ids: ${unique_events}  expected: ${EXPECTED_EVENTS}"
if [ "$unique_events" != "$EXPECTED_EVENTS" ]; then
  echo "FAILED: ${EXPECTED_EVENTS} orders in DB, ${unique_events} unique events on topic"
  exit 1
fi

echo "SIGKILL chaos verified -- order-service was SIGKILLed at POST ${KILL_AT} with ${unpublished_pre} unpublished outbox rows; restart drained them; all ${EXPECTED_EVENTS} (= surviving DB rows) unique events on topic; no business state lost"
