NAME=bruin
BUILD_DIR ?= bin
BUILD_SRC=.

NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m
TELEMETRY_OPTOUT=1
CURRENT_DIR=$(pwd)
TELEMETRY_KEY=""
FILES := $(wildcard *.yml *.txt *.py)

.PHONY: all clean test build build-no-duckdb tools format pre-commit tools-update
all: clean deps test build

deps: tools
	@printf "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)\n"
	@go mod tidy

build: deps
	@echo "$(OK_COLOR)==> Building the application...$(NO_COLOR)"
	@CGO_ENABLED=1 go build -v -tags="no_duckdb_arrow" -ldflags="-s -w -X main.Version=$(or $(tag), dev-$(shell git describe --tags --abbrev=0)) -X main.telemetryKey=$(TELEMETRY_KEY)" -o "$(BUILD_DIR)/$(NAME)" "$(BUILD_SRC)"

build-no-duckdb: deps
	@echo "$(OK_COLOR)==> Building the application without DuckDB support...$(NO_COLOR)"
	@CGO_ENABLED=0 go build -v -tags="bruin_no_duckdb" -ldflags="-s -w -X main.Version=$(or $(tag), dev-$(shell git describe --tags --abbrev=0)) -X main.telemetryKey=$(TELEMETRY_KEY)" -o "$(BUILD_DIR)/$(NAME)-no-duckdb" "$(BUILD_SRC)"

integration-test: build
	@rm -rf integration-tests/duckdb-files  # Clean up the directory if it exists
	@mkdir -p integration-tests/duckdb-files  # Recreate the directory
	@touch integration-tests/.git
	@touch integration-tests/bruin
	@rm -rf integration-tests/.git
	@rm integration-tests/bruin
	@echo "$(OK_COLOR)==> Running integration tests...$(NO_COLOR)"
	@cd integration-tests && git init
	@INCLUDE_INGESTR=1 go run integration-tests/integration-test.go

integration-test-light: build
	@rm -rf integration-tests/duckdb-files  # Clean up the directory if it exists
	@mkdir -p integration-tests/duckdb-files  # Recreate the directory
	@touch integration-tests/.git
	@touch integration-tests/bruin
	@rm -rf integration-tests/.git
	@rm integration-tests/bruin
	@echo "$(OK_COLOR)==> Running light integration tests...$(NO_COLOR)"
	@cd integration-tests && git init
	@INCLUDE_INGESTR=0 go run integration-tests/integration-test.go

clean:
	@rm -rf ./bin

test: test-unit

test-unit:
	@echo "$(OK_COLOR)==> Running the unit tests$(NO_COLOR)"
	@go test -race -cover -timeout 10m ./... 

format: tools lint-python
	@echo "$(OK_COLOR)>> [go vet] running$(NO_COLOR)" & \
	go vet ./... &

	@echo "$(OK_COLOR)>> [gci] running$(NO_COLOR)" & \
	gci write cmd pkg integration-tests/integration-test.go main.go &

	@echo "$(OK_COLOR)>> [gofumpt] running$(NO_COLOR)" & \
	gofumpt -w cmd pkg &

	@echo "$(OK_COLOR)>> [golangci-lint] running$(NO_COLOR)" & \
	golangci-lint run --timeout 10m60s ./...  & \
	wait

tools:
	@if ! command -v gci > /dev/null ; then \
		echo ">> [$@]: gci not found: installing"; \
		go install github.com/daixiang0/gci@latest; \
	fi

	@if ! command -v gofumpt > /dev/null ; then \
		echo ">> [$@]: gofumpt not found: installing"; \
		go install mvdan.cc/gofumpt@latest; \
	fi

	@if ! command -v golangci-lint > /dev/null ; then \
		echo ">> [$@]: golangci-lint not found: installing"; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.2; \
	fi

tools-update:
	go install github.com/daixiang0/gci@latest; \
	go install mvdan.cc/gofumpt@latest; \
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.2;

lint-python:
	pip install sqlglot ruff
	@echo "$(OK_COLOR)==> Running Python formatting with black...$(NO_COLOR)"
	@ruff format ./pythonsrc

	@echo "$(OK_COLOR)==> Running Python linting with flake8...$(NO_COLOR)"
	@ruff check --fix ./pythonsrc

refresh-integration-test-expectancies: build
	@echo "$(OK_COLOR)==> Refreshing integration test expectancies...$(NO_COLOR)"
	@echo "$(WARN_COLOR)Warning: This will overwrite existing expectation files$(NO_COLOR)"
	@echo "$(OK_COLOR)==> Creating expectancy refresh script...$(NO_COLOR)"
	@rm -rf integration-tests/duckdb-files  # Clean up the directory if it exists
	@mkdir -p integration-tests/duckdb-files  # Recreate the directory
	@touch integration-tests/.git
	@touch integration-tests/bruin
	@rm -rf integration-tests/.git
	@rm integration-tests/bruin
	@cd integration-tests && git init
	@echo "$(OK_COLOR)==> Running commands to capture fresh outputs...$(NO_COLOR)"
	@# Refresh connection schema expectations
	@./bin/bruin internal connections > integration-tests/expected_connections_schema.json || true
	@# Refresh connection list expectations  
	@cd integration-tests && ../bin/bruin connections list -o json . > expected_connections.json || true
	@# Refresh parse expectations for happy path assets
	@./bin/bruin internal parse-asset integration-tests/test-pipelines/parse-happy-path/assets/asset.py > integration-tests/test-pipelines/parse-happy-path/expectations/asset.py.json || true
	@./bin/bruin internal parse-asset integration-tests/test-pipelines/parse-happy-path/assets/chess_games.asset.yml > integration-tests/test-pipelines/parse-happy-path/expectations/chess_games.asset.yml.json || true
	@./bin/bruin internal parse-asset integration-tests/test-pipelines/parse-happy-path/assets/chess_profiles.asset.yml > integration-tests/test-pipelines/parse-happy-path/expectations/chess_profiles.asset.yml.json || true
	@./bin/bruin internal parse-asset integration-tests/test-pipelines/parse-happy-path/assets/player_summary.sql > integration-tests/test-pipelines/parse-happy-path/expectations/player_summary.sql.json || true
	@# Note: Additional expectations would need to be refreshed by running specific test scenarios
	@echo "$(OK_COLOR)==> Basic expectancy files have been refreshed$(NO_COLOR)"
	@echo "$(WARN_COLOR)Note: Some expectation files require running full integration test scenarios to refresh$(NO_COLOR)"
	@echo "$(OK_COLOR)==> Consider running 'make integration-test' to verify all expectations$(NO_COLOR)"