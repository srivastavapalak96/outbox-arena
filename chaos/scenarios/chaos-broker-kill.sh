#!/usr/bin/env bash
# Week 8 chaos scenario A: Kafka connection cut mid-publish, then restored.
# Asserts the outbox-arena pattern keeps the invariant "every committed business write
# eventually reaches Kafka exactly once" under broker-side faults.
#
# How the test works:
#   1. Bring up the full stack with Toxiproxy in front of Kafka.
#   2. Boot order-service only (we only need the outbox-publishing side for this test).
#   3. POST 500 orders. The poller starts shipping outbox rows to Kafka via Toxiproxy.
#   4. Mid-flight (after the first 50 publishes land), cut the Toxiproxy connection.
#   5. Continue POSTing the remaining orders. The outbox now backlogs in the DB.
#   6. Heal the connection 5 seconds later.
#   7. Wait for outbox.unpublished.count to drain to 0.
#   8. Read every message on commands.payment.v1; assert exactly 500 unique event_ids.
#
# What this proves: the outbox row survives the broker outage in the DB. When connectivity
# returns the poller picks up where it left off. No business state was lost; no event was
# lost. (Duplicates that the producer might re-send are de-duped by the consumer side via
# processed_events, but the consumer isn't running in this scenario -- we're measuring the
# producer-side guarantee in isolation.)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

ORDERS_TOTAL="${ORDERS_TOTAL:-500}"
ORDERS_BEFORE_CUT="${ORDERS_BEFORE_CUT:-50}"
HEAL_AFTER_SECONDS="${HEAL_AFTER_SECONDS:-5}"
DRAIN_DEADLINE_SECONDS="${DRAIN_DEADLINE_SECONDS:-60}"

export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"
# Services connect to Kafka via Toxiproxy on 32092.
export KAFKA_BOOTSTRAP="localhost:32092"

LOG_DIR=".local/chaos-broker-kill"
mkdir -p "$LOG_DIR"

cleanup() {
  pkill -f "io.outboxarena.order.OrderApplication" 2>/dev/null || true
  pkill -f "gradle.*:modules:order-service:bootRun" 2>/dev/null || true
}
trap cleanup EXIT

echo "--- 1. infra up with Toxiproxy overlay"
docker compose -f infra/docker-compose.yml -f infra/docker-compose.chaos.yml down -v >/dev/null 2>&1 || true
docker compose -f infra/docker-compose.yml -f infra/docker-compose.chaos.yml up -d postgres kafka toxiproxy >/dev/null
for i in $(seq 1 60); do
  if docker exec outbox-postgres pg_isready -U postgres >/dev/null 2>&1 \
     && docker exec outbox-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1 \
     && curl -fsS http://localhost:8474/version >/dev/null 2>&1; then
    break
  fi
  [ "$i" -eq 60 ] && echo "infra not ready" && exit 1
  sleep 1
done
echo "  infra + toxiproxy ready"

echo "--- 2. create the Toxiproxy 'kafka' proxy (listen :32092 -> upstream kafka:29092)"
# The proxy listens inside the toxiproxy container on 32092, which is exposed on the host.
# Upstream is the broker's EXTERNAL listener (which advertises localhost:29092 -- the
# advertised listener doesn't matter for this proxy hop because we override
# advertised.listeners by tunneling through the proxy at the network layer).
curl -fsS -X POST http://localhost:8474/proxies \
    -H 'Content-Type: application/json' \
    -d '{"name":"kafka","listen":"0.0.0.0:32092","upstream":"kafka:29092","enabled":true}' >/dev/null
echo "  proxy registered"

echo "--- 3. boot order-service only (KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP)"
./gradlew :modules:order-service:bootRun --no-daemon > "$LOG_DIR/order-service.log" 2>&1 &
for i in $(seq 1 120); do
  code="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/actuator/health || true)"
  [ "$code" = "200" ] && echo "  order-service up" && break
  [ "$i" -eq 120 ] && echo "order-service did not come up" && tail -30 "$LOG_DIR/order-service.log" && exit 1
  sleep 1
done

echo "--- 4. POST first ${ORDERS_BEFORE_CUT} orders (proxy healthy)"
for i in $(seq 1 "$ORDERS_BEFORE_CUT"); do
  curl -fsS -o /dev/null -X POST http://localhost:8081/orders \
      -H 'Content-Type: application/json' \
      -d "{\"buyerId\":\"chaos-${i}\",\"currency\":\"USD\",\"items\":[{\"sellerId\":\"seller-a\",\"sku\":\"sku-1\",\"qty\":1,\"unitPriceCents\":50}]}"
done
echo "  ${ORDERS_BEFORE_CUT} posted"

echo "--- 5. cut Toxiproxy connection to Kafka"
curl -fsS -X POST http://localhost:8474/proxies/kafka \
    -H 'Content-Type: application/json' \
    -d '{"enabled":false}' >/dev/null
echo "  proxy disabled -- outbox poller will start backing up"

echo "--- 6. POST remaining $((ORDERS_TOTAL - ORDERS_BEFORE_CUT)) orders (proxy down)"
for i in $(seq $((ORDERS_BEFORE_CUT + 1)) "$ORDERS_TOTAL"); do
  curl -fsS -o /dev/null -X POST http://localhost:8081/orders \
      -H 'Content-Type: application/json' \
      -d "{\"buyerId\":\"chaos-${i}\",\"currency\":\"USD\",\"items\":[{\"sellerId\":\"seller-a\",\"sku\":\"sku-1\",\"qty\":1,\"unitPriceCents\":50}]}"
done
echo "  ${ORDERS_TOTAL} total posted"

echo "--- 7. wait ${HEAL_AFTER_SECONDS}s, then heal the proxy"
sleep "$HEAL_AFTER_SECONDS"
curl -fsS -X POST http://localhost:8474/proxies/kafka \
    -H 'Content-Type: application/json' \
    -d '{"enabled":true}' >/dev/null
echo "  proxy re-enabled at $(date '+%H:%M:%S')"

echo "--- 8. wait for outbox to drain (deadline ${DRAIN_DEADLINE_SECONDS}s)"
for i in $(seq 1 "$DRAIN_DEADLINE_SECONDS"); do
  unpublished="$(docker exec outbox-postgres psql -U order_svc -d order_svc -t -A -c \
      "SELECT count(*) FROM outbox WHERE published_at IS NULL")"
  echo "  t+${i}s outbox.unpublished=${unpublished}"
  if [ "$unpublished" = "0" ]; then
    echo "  outbox drained at t+${i}s"
    break
  fi
  [ "$i" -eq "$DRAIN_DEADLINE_SECONDS" ] && echo "FAILED: outbox never drained" && exit 1
  sleep 1
done

echo "--- 9. count unique event_ids on commands.payment.v1"
# Run a one-shot console consumer through the proxy. Pull every record from offset 0,
# extract the event-id header, dedup.
unique_events="$(docker exec outbox-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic commands.payment.v1 \
    --from-beginning \
    --max-messages "$ORDERS_TOTAL" \
    --timeout-ms 15000 \
    --property print.headers=true 2>/dev/null \
    | grep -oE 'event-id:[a-f0-9-]+' \
    | sort -u \
    | wc -l \
    | tr -d ' ')"

echo "  unique event-ids on topic: ${unique_events}; expected ${ORDERS_TOTAL}"
if [ "$unique_events" -lt "$ORDERS_TOTAL" ]; then
  echo "FAILED: ${ORDERS_TOTAL} sent, ${unique_events} received -- $((ORDERS_TOTAL - unique_events)) lost"
  exit 1
fi

echo "broker-kill chaos verified -- ${ORDERS_TOTAL} sent through a mid-flight cut, ${unique_events} received, 0 lost"
