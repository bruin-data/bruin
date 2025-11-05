NAME=bruin$(shell if [ "$(shell go env GOOS)" = "windows" ]; then echo .exe; fi)
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
OS_ARCH:=$(shell go env GOOS)_$(shell go env GOARCH)

# Suppress CGO linker warnings on macOS (not needed on Linux/Windows)
ifeq ($(shell go env GOOS),darwin)
export CGO_LDFLAGS=-Wl,-w
export LDFLAGS=-Wl,-w
endif

JQ_REL_PATH = jq --arg prefix "$$(pwd)/" 'walk(if type == "object" and has("path") and (.path | type == "string") then .path |= (if startswith($$prefix) then .[($$prefix | length):] elif startswith("integration-tests/") then .[16:] else . end) else . end)'

.PHONY: all clean test build build-no-duckdb tools format pre-commit tools-update refresh-integration-expectations integration-test-cloud
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
	@rm -rf integration-tests/logs
	@mkdir -p integration-tests/logs
	@mkdir -p integration-tests/logs/exports
	@mkdir -p integration-tests/logs/runs
	@echo "$(OK_COLOR)==> Running integration tests...$(NO_COLOR)"
	@cd integration-tests && git init
	@cd integration-tests && env SILENT=1 go test -tags="no_duckdb_arrow" -v -count=1 .

integration-test-light: build
	@rm -rf integration-tests/duckdb-files  # Clean up the directory if it exists
	@mkdir -p integration-tests/duckdb-files  # Recreate the directory
	@touch integration-tests/.git
	@touch integration-tests/bruin
	@rm -rf integration-tests/.git
	@rm integration-tests/bruin
	@rm -rf integration-tests/logs
	@mkdir -p integration-tests/logs
	@mkdir -p integration-tests/logs/exports
	@mkdir -p integration-tests/logs/runs
	@echo "$(OK_COLOR)==> Running integration tests (skipping ingestr tasks)...$(NO_COLOR)"
	@cd integration-tests && git init
	@cd integration-tests && env SILENT=1 go test -tags="no_duckdb_arrow" -v -count=1 -run "^(TestIndividualTasks|TestWorkflowTasks)" .

integration-test-cloud: build
	@touch integration-tests/cloud-integration-tests/.git
	@touch integration-tests/cloud-integration-tests/bruin
	@rm -rf integration-tests/cloud-integration-tests/.git
	@rm integration-tests/cloud-integration-tests/bruin
	@echo "$(OK_COLOR)==> Running cloud integration tests...$(NO_COLOR)"
	@cd integration-tests && git init
	@cd integration-tests/cloud-integration-tests && env SILENT=1 go test -count=1 -v .

clean:
	@rm -rf ./bin

test: test-unit

test-unit:
	@echo "$(OK_COLOR)==> Running the unit tests$(NO_COLOR)"
	@go test -tags="no_duckdb_arrow" -race -cover -timeout 10m $(shell go list ./... | grep -v 'integration-tests') 

format: tools lint-python
	@echo "$(OK_COLOR)>> [go vet] running$(NO_COLOR)" & \
	go vet -tags="no_duckdb_arrow" ./... & 

	@echo "$(OK_COLOR)>> [gci] running$(NO_COLOR)" & \
	gci write cmd pkg integration-tests/integration_test.go main.go &

	@echo "$(OK_COLOR)>> [gofumpt] running$(NO_COLOR)" & \
	gofumpt -w cmd pkg &

	@echo "$(OK_COLOR)>> [golangci-lint] running$(NO_COLOR)" & \
	golangci-lint run --timeout 10m60s --build-tags="no_duckdb_arrow" ./...  & \
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

refresh-integration-expectations: build
	@echo "$(OK_COLOR)==> Refreshing integration expectations...$(NO_COLOR)"
	@cd integration-tests && git init
	@echo "$(OK_COLOR)==> Updating parse-whole-pipeline expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-pipeline test-pipelines/parse-whole-pipeline | $(JQ_REL_PATH) > test-pipelines/parse-whole-pipeline/expectations/pipeline.yml.json
	@echo "$(OK_COLOR)==> Updating parse-happy-path expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-pipeline test-pipelines/parse-happy-path | $(JQ_REL_PATH) > test-pipelines/parse-happy-path/expectations/pipeline.yml.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-happy-path/assets/asset.py | $(JQ_REL_PATH) > test-pipelines/parse-happy-path/expectations/asset.py.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-happy-path/assets/chess_games.asset.yml | $(JQ_REL_PATH) > test-pipelines/parse-happy-path/expectations/chess_games.asset.yml.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-happy-path/assets/chess_profiles.asset.yml | $(JQ_REL_PATH) > test-pipelines/parse-happy-path/expectations/chess_profiles.asset.yml.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-happy-path/assets/player_summary.sql | $(JQ_REL_PATH) > test-pipelines/parse-happy-path/expectations/player_summary.sql.json
	@echo "$(OK_COLOR)==> Updating parse-lineage-pipeline expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-pipeline -c test-pipelines/parse-lineage-pipeline | $(JQ_REL_PATH) > test-pipelines/parse-lineage-pipeline/expectations/lineage.json
	@echo "$(OK_COLOR)==> Updating parse-asset-lineage-pipeline expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-asset -c test-pipelines/parse-asset-lineage-pipeline/assets/example.sql | $(JQ_REL_PATH) > test-pipelines/parse-asset-lineage-pipeline/expectations/lineage-asset.json
	@echo "$(OK_COLOR)==> Updating parse-default-option expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-pipeline test-pipelines/parse-default-option | $(JQ_REL_PATH) > test-pipelines/parse-default-option/expectations/pipeline.yml.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-default-option/assets/asset.py | $(JQ_REL_PATH) > test-pipelines/parse-default-option/expectations/asset.py.json
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/parse-default-option/assets/chess_games.asset.yml | $(JQ_REL_PATH) > test-pipelines/parse-default-option/expectations/chess_games.asset.yml.json
	@echo "$(OK_COLOR)==> Updating parse-asset-extends expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-pipeline test-pipelines/parse-asset-extends | $(JQ_REL_PATH) > test-pipelines/parse-asset-extends/expectations/pipeline.json
	@echo "$(OK_COLOR)==> Updating run-seed-data expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal parse-asset test-pipelines/run-seed-data/assets/seed.asset.yml | $(JQ_REL_PATH) > test-pipelines/run-seed-data/expectations/seed.asset.yml.json
	@echo "$(OK_COLOR)==> Updating connection expectations...$(NO_COLOR)"
	@cd integration-tests && ../bin/bruin internal connections | $(JQ_REL_PATH) > expected_connections_schema.json
	@cd integration-tests && ../bin/bruin connections list -o json . | $(JQ_REL_PATH) > expected_connections.json
	@echo "$(OK_COLOR)==> Integration expectations refreshed successfully!$(NO_COLOR)"

# sometimes vendoring doesn't move the precompiled library
duck-db-static-lib:
	@mkdir vendor/github.com/marcboeker/go-duckdb/deps || true
	@mkdir vendor/github.com/marcboeker/go-duckdb/deps/$(OS_ARCH) || true
	@cp $$(go env GOPATH)/pkg/mod/github.com/marcboeker/go-duckdb@v1.8.2/deps/$(OS_ARCH)/libduckdb.a vendor/github.com/marcboeker/go-duckdb/deps/$(OS_ARCH)/libduckdb.a

