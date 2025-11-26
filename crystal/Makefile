.PHONY: help build run clean install test release docker-build docker-run

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install dependencies
	shards install

build: ## Build debug version
	crystal build consumer.cr

release: ## Build optimized release version
	crystal build consumer.cr \
		--release \
		--no-debug \
		-o consumer

run: ## Run the consumer
	crystal run consumer.cr

clean: ## Clean build artifacts
	rm -f consumer
	rm -rf lib/ .shards/

test: ## Run tests (placeholder)
	@echo "No tests configured yet"

docker-build: ## Build Docker image
	docker build -t kafka-consumer-crystal:latest .

docker-run: ## Run Docker container
	docker run --rm \
		--network host \
		-e KAFKA_BROKERS=localhost:9092 \
		-e DATABASE_URL=postgres://bench:bench@localhost/bench \
		kafka-consumer-crystal:latest

format: ## Format code
	crystal tool format

lint: ## Lint code
	crystal tool format --check

deps-update: ## Update dependencies
	shards update
