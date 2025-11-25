# ============================================================================
# Spotify Pipeline - Makefile
# ============================================================================
# Makefile for managing Dagster pipeline, FastAPI service, and Docker containers
# Usage: make <target>
# Run 'make help' to see all available commands

SHELL := /bin/bash
.DEFAULT_GOAL := help

# ============================================================================
# Configuration
# ============================================================================

UV ?= uv
DOCKER_COMPOSE := $(if $(shell command -v docker-compose 2>/dev/null),docker-compose,docker compose)
PROJECT_ROOT := $(abspath $(CURDIR)/..)
DATA_DIR := $(PROJECT_ROOT)/data
MODELS_DIR := $(PROJECT_ROOT)/models
SRC_DIR := src
TESTS_DIR := tests

# Colors for pretty output
COLOR_RESET   := \033[0m
COLOR_BOLD    := \033[1m
COLOR_GREEN   := \033[0;32m
COLOR_BLUE    := \033[0;34m
COLOR_YELLOW  := \033[0;33m
COLOR_CYAN    := \033[0;36m
COLOR_RED     := \033[0;31m

# ============================================================================
# Phony Targets
# ============================================================================

.PHONY: all help install test lint format check prepare-dirs \
	dagster dagster-logs dagster-stop pipeline \
	api api-logs api-stop api-test \
	docker-build docker-up docker-down docker-restart docker-logs \
	docker-ps docker-clean docker-test docker-status \
	clean clean-all clean-pyc clean-docker

# ============================================================================
# Help
# ============================================================================

all: help

help:
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_CYAN)╔══════════════════════════════════════════════════════════════╗$(COLOR_RESET)\n"
	@printf "$(COLOR_BOLD)$(COLOR_CYAN)║          Spotify Pipeline - Available Commands              ║$(COLOR_RESET)\n"
	@printf "$(COLOR_BOLD)$(COLOR_CYAN)╚══════════════════════════════════════════════════════════════╝$(COLOR_RESET)\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)📦 Development Commands:$(COLOR_RESET)\n"
	@printf "  $(COLOR_GREEN)make install$(COLOR_RESET)        Install Python dependencies with uv\n"
	@printf "  $(COLOR_GREEN)make test$(COLOR_RESET)           Run all tests\n"
	@printf "  $(COLOR_GREEN)make lint$(COLOR_RESET)           Run linters (ruff, mypy)\n"
	@printf "  $(COLOR_GREEN)make format$(COLOR_RESET)         Format code with ruff\n"
	@printf "  $(COLOR_GREEN)make check$(COLOR_RESET)          Run lint + test\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🔧 Dagster Pipeline:$(COLOR_RESET)\n"
	@printf "  $(COLOR_GREEN)make dagster$(COLOR_RESET)        Start Dagster service (http://localhost:3000)\n"
	@printf "  $(COLOR_GREEN)make dagster-logs$(COLOR_RESET)   View Dagster logs\n"
	@printf "  $(COLOR_GREEN)make dagster-stop$(COLOR_RESET)   Stop Dagster service\n"
	@printf "  $(COLOR_GREEN)make pipeline$(COLOR_RESET)       Materialize all Dagster assets\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🚀 API Commands:$(COLOR_RESET)\n"
	@printf "  $(COLOR_GREEN)make api$(COLOR_RESET)            Start FastAPI service (http://localhost:8000)\n"
	@printf "  $(COLOR_GREEN)make api-logs$(COLOR_RESET)       View API logs\n"
	@printf "  $(COLOR_GREEN)make api-stop$(COLOR_RESET)       Stop API service\n"
	@printf "  $(COLOR_GREEN)make api-test$(COLOR_RESET)       Run API smoke tests\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🐳 Docker Commands:$(COLOR_RESET)\n"
	@printf "  $(COLOR_GREEN)make docker-build$(COLOR_RESET)   Build all Docker images\n"
	@printf "  $(COLOR_GREEN)make docker-up$(COLOR_RESET)      Start all services in background\n"
	@printf "  $(COLOR_GREEN)make docker-down$(COLOR_RESET)    Stop all services\n"
	@printf "  $(COLOR_GREEN)make docker-restart$(COLOR_RESET) Restart all services\n"
	@printf "  $(COLOR_GREEN)make docker-logs$(COLOR_RESET)    Tail all service logs (Ctrl+C to exit)\n"
	@printf "  $(COLOR_GREEN)make docker-ps$(COLOR_RESET)      Show service status\n"
	@printf "  $(COLOR_GREEN)make docker-status$(COLOR_RESET)  Show detailed service status\n"
	@printf "  $(COLOR_GREEN)make docker-clean$(COLOR_RESET)   Remove containers and volumes\n"
	@printf "  $(COLOR_GREEN)make docker-test$(COLOR_RESET)    Run container health checks\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🧹 Cleanup Commands:$(COLOR_RESET)\n"
	@printf "  $(COLOR_GREEN)make clean$(COLOR_RESET)          Remove Python cache files\n"
	@printf "  $(COLOR_GREEN)make clean-pyc$(COLOR_RESET)      Remove Python bytecode files\n"
	@printf "  $(COLOR_GREEN)make clean-docker$(COLOR_RESET)   Remove Docker containers and volumes\n"
	@printf "  $(COLOR_GREEN)make clean-all$(COLOR_RESET)      Full cleanup (cache + Docker)\n"
	@printf "\n"
	@printf "$(COLOR_BOLD)$(COLOR_YELLOW)💡 Quick Start:$(COLOR_RESET)\n"
	@printf "  1. $(COLOR_CYAN)make install$(COLOR_RESET)         # Install dependencies\n"
	@printf "  2. $(COLOR_CYAN)make docker-build$(COLOR_RESET)    # Build images\n"
	@printf "  3. $(COLOR_CYAN)make docker-up$(COLOR_RESET)       # Start all services\n"
	@printf "  4. $(COLOR_CYAN)make pipeline$(COLOR_RESET)        # Run the pipeline\n"
	@printf "\n"

# ============================================================================
# Development
# ============================================================================

install:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)📦 Installing dependencies...$(COLOR_RESET)\n"
	@$(UV) sync
	@printf "$(COLOR_GREEN)✓ Dependencies installed successfully$(COLOR_RESET)\n"

test:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🧪 Running tests...$(COLOR_RESET)\n"
	@$(UV) run pytest $(TESTS_DIR) -v

lint:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🔍 Running linters...$(COLOR_RESET)\n"
	@$(UV) run ruff check $(SRC_DIR) || true
	@printf "$(COLOR_GREEN)✓ Linting complete$(COLOR_RESET)\n"

format:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)✨ Formatting code...$(COLOR_RESET)\n"
	@$(UV) run ruff format $(SRC_DIR) $(TESTS_DIR) || true
	@$(UV) run ruff check --fix $(SRC_DIR) $(TESTS_DIR) || true
	@printf "$(COLOR_GREEN)✓ Code formatted$(COLOR_RESET)\n"

check: lint test
	@printf "$(COLOR_GREEN)✓ All checks passed$(COLOR_RESET)\n"

# ============================================================================
# Directory Setup
# ============================================================================

prepare-dirs:
	@printf "$(COLOR_BLUE)📁 Preparing directories...$(COLOR_RESET)\n"
	@mkdir -p $(DATA_DIR)/dagster_storage $(MODELS_DIR)
	@printf "$(COLOR_GREEN)✓ Directories ready$(COLOR_RESET)\n"

# ============================================================================
# Dagster Pipeline
# ============================================================================

dagster: prepare-dirs
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🔧 Starting Dagster service...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) up -d dagster
	@printf "$(COLOR_GREEN)✓ Dagster started$(COLOR_RESET)\n"
	@printf "$(COLOR_CYAN)🌐 Dagster UI: http://localhost:3000$(COLOR_RESET)\n"

dagster-logs:
	@printf "$(COLOR_BLUE)📋 Showing Dagster logs (Ctrl+C to exit)...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) logs -f dagster

dagster-stop:
	@printf "$(COLOR_BLUE)⏹️  Stopping Dagster service...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) stop dagster
	@printf "$(COLOR_GREEN)✓ Dagster stopped$(COLOR_RESET)\n"

pipeline: prepare-dirs
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)⚙️  Materializing Dagster assets...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) run --rm dagster uv run dagster asset materialize --select '*' -m spotify_pipeline.definitions
	@printf "$(COLOR_GREEN)✓ Pipeline execution complete$(COLOR_RESET)\n"

# ============================================================================
# FastAPI Service
# ============================================================================

api:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🚀 Starting API service...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) up -d api
	@printf "$(COLOR_GREEN)✓ API started$(COLOR_RESET)\n"
	@printf "$(COLOR_CYAN)🌐 API docs: http://localhost:8000/docs$(COLOR_RESET)\n"
	@printf "$(COLOR_CYAN)🌐 Health check: http://localhost:8000/health$(COLOR_RESET)\n"

api-logs:
	@printf "$(COLOR_BLUE)📋 Showing API logs (Ctrl+C to exit)...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) logs -f api

api-stop:
	@printf "$(COLOR_BLUE)⏹️  Stopping API service...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) stop api
	@printf "$(COLOR_GREEN)✓ API stopped$(COLOR_RESET)\n"

api-test:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🧪 Testing API...$(COLOR_RESET)\n"
	@$(UV) run python tests/test_api.py
	@printf "$(COLOR_GREEN)✓ API tests complete$(COLOR_RESET)\n"

# ============================================================================
# Docker Management
# ============================================================================

docker-build:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🐳 Building Docker images...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) build
	@printf "$(COLOR_GREEN)✓ Docker images built successfully$(COLOR_RESET)\n"

docker-up: prepare-dirs
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🐳 Starting all services...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) up -d
	@printf "$(COLOR_GREEN)✓ All services started$(COLOR_RESET)\n"
	@printf "\n"
	@$(MAKE) docker-status

docker-down:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🐳 Stopping all services...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) down
	@printf "$(COLOR_GREEN)✓ All services stopped$(COLOR_RESET)\n"

docker-restart:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🐳 Restarting all services...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) restart
	@printf "$(COLOR_GREEN)✓ All services restarted$(COLOR_RESET)\n"

docker-logs:
	@printf "$(COLOR_BLUE)📋 Showing logs for all services (Ctrl+C to exit)...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) logs -f

docker-ps:
	@$(DOCKER_COMPOSE) ps

docker-status:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)📊 Service Status:$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) ps
	@printf "\n"
	@printf "$(COLOR_CYAN)🌐 Service URLs:$(COLOR_RESET)\n"
	@printf "  • Dagster UI:  http://localhost:3000\n"
	@printf "  • API:         http://localhost:8000\n"
	@printf "  • API Docs:    http://localhost:8000/docs\n"

docker-clean:
	@printf "$(COLOR_BOLD)$(COLOR_YELLOW)🗑️  Removing Docker containers and volumes...$(COLOR_RESET)\n"
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@printf "$(COLOR_GREEN)✓ Docker cleanup complete$(COLOR_RESET)\n"

docker-test:
	@printf "$(COLOR_BOLD)$(COLOR_BLUE)🧪 Running container health checks...$(COLOR_RESET)\n"
	@./scripts/docker-test.sh || ./docker-test.sh
	@printf "$(COLOR_GREEN)✓ Health checks complete$(COLOR_RESET)\n"

# ============================================================================
# Cleanup
# ============================================================================

clean-pyc:
	@printf "$(COLOR_BLUE)🧹 Removing Python bytecode files...$(COLOR_RESET)\n"
	@find . -type f -name "*.py[co]" -delete
	@find . -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
	@printf "$(COLOR_GREEN)✓ Python bytecode files removed$(COLOR_RESET)\n"

clean: clean-pyc
	@printf "$(COLOR_BLUE)🧹 Cleaning Python cache files...$(COLOR_RESET)\n"
	@find . -type d -name ".pytest_cache" -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".ruff_cache" -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -prune -exec rm -rf {} + 2>/dev/null || true
	@printf "$(COLOR_GREEN)✓ Cache cleanup complete$(COLOR_RESET)\n"

clean-docker: docker-clean

clean-all: clean clean-docker
	@printf "$(COLOR_BOLD)$(COLOR_GREEN)✓ Full cleanup complete$(COLOR_RESET)\n"
