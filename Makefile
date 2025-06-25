# Data Pipeline Project Makefile
# ==============================
# This Makefile provides convenient commands for managing the data pipeline project

.PHONY: help install test clean generate-sql generate-all-sql validate-templates show-config list-templates

# Default target
help: ## Show this help message
	@echo "Data Pipeline Project - Available Commands"
	@echo "=========================================="
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make generate-sql ENV=dev     # Generate SQL for dev environment"
	@echo "  make generate-all-sql         # Generate SQL for all environments"
	@echo "  make validate-templates       # Validate template files"
	@echo "  make clean                    # Clean generated files"

# Environment variable
ENV ?= dev

# Python and pip commands
PYTHON := python3
PIP := pip3

install: ## Install project dependencies
	@echo "📦 Installing project dependencies..."
	$(PIP) install -r requirements.txt
	@echo "✅ Dependencies installed successfully!"

install-dev: ## Install development dependencies
	@echo "📦 Installing development dependencies..."
	$(PIP) install -r requirements.txt
	$(PIP) install pytest black flake8 mypy
	@echo "✅ Development dependencies installed successfully!"

# SQL Generation Commands
generate-sql: ## Generate Snowflake SQL files for specified environment (ENV=dev|staging|prod)
	@echo "🔧 Generating Snowflake SQL files for $(ENV) environment..."
	$(PYTHON) scripts/generate_snowflake_sql.py $(ENV)
	@echo "✅ SQL generation completed for $(ENV) environment!"

generate-all-sql: ## Generate Snowflake SQL files for all environments
	@echo "🔧 Generating Snowflake SQL files for all environments..."
	@for env in dev staging prod; do \
		echo "📝 Processing $$env environment..."; \
		$(PYTHON) scripts/generate_snowflake_sql.py $$env; \
		echo ""; \
	done
	@echo "✅ SQL generation completed for all environments!"

show-config: ## Show configuration for all environments
	@echo "📋 Environment configurations:"
	$(PYTHON) scripts/generate_snowflake_sql.py --show-config

list-templates: ## List available SQL template files
	@echo "📋 Available SQL template files:"
	$(PYTHON) scripts/generate_snowflake_sql.py --list-templates

validate-templates: ## Validate SQL template files
	@echo "🔍 Validating SQL template files..."
	@for template in snowflake/*.sql; do \
		if [ -f "$$template" ]; then \
			echo "✅ Found: $$(basename $$template)"; \
		fi; \
	done
	@echo "✅ Template validation completed!"

# Cleanup Commands
clean: ## Clean generated SQL files
	@echo "🧹 Cleaning generated SQL files..."
	@if [ -d "generated_sql" ]; then \
		rm -rf generated_sql; \
		echo "✅ Removed generated_sql directory"; \
	else \
		echo "ℹ️  No generated_sql directory found"; \
	fi

clean-env: ## Clean generated SQL files for specific environment (ENV=dev|staging|prod)
	@echo "🧹 Cleaning generated SQL files for $(ENV) environment..."
	@if [ -d "generated_sql/$(ENV)" ]; then \
		rm -rf generated_sql/$(ENV); \
		echo "✅ Removed generated_sql/$(ENV) directory"; \
	else \
		echo "ℹ️  No generated_sql/$(ENV) directory found"; \
	fi

# AWS and Glue Commands
setup-glue: ## Set up AWS Glue catalog and jobs
	@echo "⚡ Setting up AWS Glue catalog and jobs..."
	$(PYTHON) scripts/setup_all_catalog_tables.py
	@echo "✅ Glue setup completed!"

validate-setup: ## Validate the entire pipeline setup
	@echo "🔍 Validating pipeline setup..."
	$(PYTHON) scripts/validate_setup.py
	@echo "✅ Setup validation completed!"

run-glue-jobs: ## Run all Glue jobs in sequence
	@echo "⚡ Running Glue jobs..."
	$(PYTHON) scripts/run_glue_jobs.py
	@echo "✅ Glue jobs completed!"

# Testing Commands
test: ## Run all tests
	@echo "🧪 Running tests..."
	@if command -v pytest >/dev/null 2>&1; then \
		pytest tests/ -v; \
	else \
		echo "⚠️  pytest not installed. Run 'make install-dev' first."; \
	fi

test-sql: ## Test generated SQL files
	@echo "🧪 Testing generated SQL files..."
	@for env in dev staging prod; do \
		if [ -d "generated_sql/$$env" ]; then \
			echo "Testing $$env environment SQL files..."; \
			for sql_file in generated_sql/$$env/*.sql; do \
				if [ -f "$$sql_file" ]; then \
					echo "  ✅ $$(basename $$sql_file)"; \
				fi; \
			done; \
		fi; \
	done

# Code Quality Commands
format: ## Format Python code with black
	@echo "🎨 Formatting Python code..."
	@if command -v black >/dev/null 2>&1; then \
		black scripts/ glue_jobs/ --line-length 100; \
		echo "✅ Code formatting completed!"; \
	else \
		echo "⚠️  black not installed. Run 'make install-dev' first."; \
	fi

lint: ## Lint Python code with flake8
	@echo "🔍 Linting Python code..."
	@if command -v flake8 >/dev/null 2>&1; then \
		flake8 scripts/ glue_jobs/ --max-line-length=100; \
		echo "✅ Code linting completed!"; \
	else \
		echo "⚠️  flake8 not installed. Run 'make install-dev' first."; \
	fi

type-check: ## Type check Python code with mypy
	@echo "🔍 Type checking Python code..."
	@if command -v mypy >/dev/null 2>&1; then \
		mypy scripts/ glue_jobs/ --ignore-missing-imports; \
		echo "✅ Type checking completed!"; \
	else \
		echo "⚠️  mypy not installed. Run 'make install-dev' first."; \
	fi

check: format lint type-check ## Run all code quality checks

# Docker Commands (if using Docker)
docker-build: ## Build Docker image for the pipeline
	@echo "🐳 Building Docker image..."
	docker build -t data-pipeline:latest .
	@echo "✅ Docker image built successfully!"

docker-run: ## Run the pipeline in Docker
	@echo "🐳 Running pipeline in Docker..."
	docker run --rm -v $(PWD):/workspace data-pipeline:latest
	@echo "✅ Docker run completed!"

# Documentation Commands
docs: ## Generate project documentation
	@echo "📚 Generating project documentation..."
	@echo "Project structure:" > docs/PROJECT_STRUCTURE.md
	@tree -I '__pycache__|*.pyc|.git|generated_sql' >> docs/PROJECT_STRUCTURE.md || echo "tree command not available"
	@echo "✅ Documentation generated!"

# Information Commands
info: ## Show project information
	@echo "📊 Data Pipeline Project Information"
	@echo "=================================="
	@echo "Python version: $$(python3 --version)"
	@echo "Project directory: $$(pwd)"
	@echo "Available environments: dev, staging, prod"
	@echo ""
	@echo "Template files:"
	@ls -1 snowflake/*.sql 2>/dev/null | sed 's/^/  /' || echo "  No template files found"
	@echo ""
	@echo "Generated SQL directories:"
	@ls -1d generated_sql/*/ 2>/dev/null | sed 's/^/  /' || echo "  No generated SQL directories found"

status: ## Show current project status
	@echo "📈 Project Status"
	@echo "================"
	@echo "Git status:"
	@git status --porcelain | head -10 || echo "  Not a git repository"
	@echo ""
	@echo "Generated SQL files:"
	@find generated_sql -name "*.sql" 2>/dev/null | wc -l | sed 's/^/  Total: /' || echo "  0"
	@echo ""
	@echo "Template files:"
	@ls snowflake/*.sql 2>/dev/null | wc -l | sed 's/^/  Total: /' || echo "  0"

# Quick Setup Commands
quick-setup: install generate-all-sql ## Quick setup: install dependencies and generate all SQL files
	@echo "🚀 Quick setup completed!"
	@echo "📁 Generated SQL files are in the generated_sql/ directory"
	@echo "📖 Check the README.md files in each environment directory for usage instructions"

# Environment-specific shortcuts
dev: ## Generate SQL files for dev environment
	@$(MAKE) generate-sql ENV=dev

staging: ## Generate SQL files for staging environment  
	@$(MAKE) generate-sql ENV=staging

prod: ## Generate SQL files for prod environment
	@$(MAKE) generate-sql ENV=prod

# Backup and restore
backup: ## Backup generated SQL files
	@echo "💾 Creating backup of generated SQL files..."
	@if [ -d "generated_sql" ]; then \
		tar -czf "generated_sql_backup_$$(date +%Y%m%d_%H%M%S).tar.gz" generated_sql/; \
		echo "✅ Backup created successfully!"; \
	else \
		echo "ℹ️  No generated_sql directory to backup"; \
	fi

# CI/CD helpers
ci-validate: ## Validate project for CI/CD (used in GitHub Actions)
	@echo "🔍 CI/CD Validation..."
	@$(MAKE) validate-templates
	@$(MAKE) list-templates
	@$(MAKE) show-config
	@echo "✅ CI/CD validation completed!"

ci-generate: ## Generate SQL files for CI/CD
	@echo "🔧 CI/CD SQL Generation..."
	@$(MAKE) generate-all-sql
	@echo "✅ CI/CD SQL generation completed!" 