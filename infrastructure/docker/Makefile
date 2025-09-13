# Enterprise Data Platform - Docker Infrastructure
# This Makefile manages the containerized data engineering stack

.PHONY: help up down logs restart clean status airflow-only data-ingestion storage-only

# Default target
help: ## Show this help message
	@echo "Enterprise Data Platform - Docker Commands"
	@echo "=========================================="
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Core Infrastructure Commands
up: ## Start all services (Airflow + Airbyte + MinIO)
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml up -d
	@echo "🚀 Enterprise Data Platform started successfully!"
	@echo "📊 Airflow UI: http://localhost:8080"
	@echo "🔄 Airbyte UI: http://localhost:8000"
	@echo "💾 MinIO Console: http://localhost:9001"

down: ## Stop all services
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml down
	@echo "🛑 All services stopped"

restart: down up ## Restart all services

# Service-Specific Commands
airflow-only: ## Start only Airflow orchestration service
	docker-compose -f docker-compose.airflow.yml up -d
	@echo "🎯 Airflow service started: http://localhost:8080"

data-ingestion: ## Start Airflow + Airbyte for data ingestion
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml up -d
	@echo "📥 Data ingestion services started"

storage-only: ## Start only MinIO storage service
	docker-compose -f docker-compose.s3.yml up -d
	@echo "💾 Storage service started: http://localhost:9001"

# Monitoring & Debugging
logs: ## View logs from all services
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml logs -f

logs-airflow: ## View Airflow logs only
	docker-compose -f docker-compose.airflow.yml logs -f

logs-airbyte: ## View Airbyte logs only
	docker-compose -f docker-compose.airbyte.yml logs -f

status: ## Show status of all containers
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml ps

# Maintenance
clean: ## Clean up all containers, volumes, and images
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml down -v --remove-orphans
	docker system prune -f
	@echo "🧹 Cleanup completed"

# Development
dev: ## Start development environment with hot reload
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml up

# Production-like
prod: ## Start production-like environment
	docker-compose -f docker-compose.airflow.yml -f docker-compose.airbyte.yml -f docker-compose.s3.yml up -d --scale airflow-worker=3

# Health Checks
health: ## Check health of all services
	@echo "🔍 Checking service health..."
	@curl -f http://localhost:8080/health || echo "❌ Airflow not healthy"
	@curl -f http://localhost:8000/health || echo "❌ Airbyte not healthy"
	@curl -f http://localhost:9000/minio/health/live || echo "❌ MinIO not healthy"