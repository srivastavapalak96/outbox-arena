# outbox-arena -- one-command shortcuts for local development.
#
# Conventions: targets are verbs, never nouns. Targets that mutate state must say so
# in their help line.

.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose -f infra/docker-compose.yml

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: up
up: ## Bring the infra stack up in detached mode.
	$(COMPOSE) up -d
	@echo "Stack starting. Run 'make health-infra' in ~30s."

.PHONY: down
down: ## Tear the stack down (preserves volumes).
	$(COMPOSE) down

.PHONY: down-clean
down-clean: ## Tear the stack down AND wipe volumes. (DESTRUCTIVE)
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs for the infra stack.
	$(COMPOSE) logs -f

.PHONY: health-infra
health-infra: ## Check infra readiness (Postgres, Kafka, Connect).
	@$(COMPOSE) ps
	@echo "--- Kafka topics:"
	@$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list || true
	@echo "--- Connect:"
	@curl -fsS localhost:18083/ | head -c 200 && echo

.PHONY: health
health: ## Curl /actuator/health on all 6 services (8081-8086).
	@for port in 8081 8082 8083 8084 8085 8086; do \
		printf "  service on %s ... " $$port; \
		curl -fsS http://localhost:$$port/actuator/health || echo "DOWN"; \
		echo; \
	done

.PHONY: build
build: ## Compile all modules.
	./gradlew build

.PHONY: test
test: ## Unit tests.
	./gradlew test

.PHONY: check
check: ## Full quality gate (spotless + tests).
	./gradlew check

.PHONY: register-connector
register-connector: ## Register the Debezium connector against the running Kafka Connect (week 6+).
	curl -fsS -X POST -H 'Content-Type: application/json' \
		--data @infra/kafka-connect/debezium-connector.json \
		http://localhost:18083/connectors
