#!/usr/bin/env bash
# Regression sweep: run every chaos scenario in sequence and report pass/fail summary.
# Each scenario brings up its own (sub)stack, so we tear everything down between runs to
# avoid state bleed-through.

set -u  # no -e -- we want to keep running after a failure

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

export JAVA_HOME="/Users/shivangbelwariar/Library/Java/JavaVirtualMachines/amazon-corretto-21.jdk/Contents/Home"

SCENARIOS=(
  "happy-path.sh"
  "compensation-payment-fail.sh"
  "compensation-inventory-reject.sh"
  "compensation-shipment-fail.sh"
  "cdc-projection.sh"
  "chaos-broker-kill.sh"
  "chaos-multi-poller-race.sh"
  "chaos-sigkill-order-service.sh"
  "chaos-consumer-rebalance.sh"
)

LOG_ROOT=".local/regression-sweep"
mkdir -p "$LOG_ROOT"

declare -a results

for scenario in "${SCENARIOS[@]}"; do
  echo
  echo "================================================================"
  echo "=== RUNNING: $scenario"
  echo "================================================================"
  echo "--- pre-clean docker"
  docker compose -f infra/docker-compose.yml -f infra/docker-compose.chaos.yml down -v >/dev/null 2>&1 || true
  # Belt-and-braces: also kill any stray bootRun.
  pkill -f "io.outboxarena\." 2>/dev/null || true
  pkill -f "gradle.*bootRun" 2>/dev/null || true
  sleep 2

  start=$(date +%s)
  if "./chaos/scenarios/${scenario}" > "${LOG_ROOT}/${scenario}.log" 2>&1; then
    elapsed=$(($(date +%s) - start))
    echo "PASS: ${scenario} (${elapsed}s)"
    results+=("PASS  ${elapsed}s  ${scenario}")
  else
    elapsed=$(($(date +%s) - start))
    echo "FAIL: ${scenario} (${elapsed}s)"
    echo "--- last 30 lines of log:"
    tail -30 "${LOG_ROOT}/${scenario}.log"
    results+=("FAIL  ${elapsed}s  ${scenario}")
  fi
done

echo
echo "================================================================"
echo "=== SWEEP SUMMARY"
echo "================================================================"
for r in "${results[@]}"; do
  echo "  $r"
done

# Final cleanup
docker compose -f infra/docker-compose.yml -f infra/docker-compose.chaos.yml down -v >/dev/null 2>&1 || true

# Exit non-zero if any scenario failed.
for r in "${results[@]}"; do
  case "$r" in
    FAIL*) exit 1 ;;
  esac
done
exit 0
