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
LINT_MERGE_BASE ?= origin/main
GCI_VERSION ?= v0.14.0
GOFUMPT_VERSION ?= v0.10.0
RUFF_VERSION ?= 0.15.4
# Pinned, not @latest. v2.12.x made its cache checkout-independent, but cached
# diagnostics still contain absolute paths from the checkout that produced
# them. That makes shared-cache results unsafe across worktrees. Re-test before
# bumping beyond the latest pre-change patch release.
GOLANGCI_LINT_VERSION ?= v2.11.4
# Build golangci-lint with the toolchain targeted by this module.
GOLANGCI_LINT_INSTALL := GOTOOLCHAIN=go$(shell awk '/^go /{print $$2; exit}' go.mod) \
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
# golangci-lint uses a single global lock in the system temporary directory.
# Its cache supports concurrent processes, so allow separate worktrees to lint
# at the same time without one runner failing on lock contention.
LINT_PARALLEL_FLAGS ?= --allow-parallel-runners
LINT_CONCURRENCY ?= 4
LINT_TIMEOUT ?= 10m
LINT_BUILD_TAGS ?= no_duckdb_arrow
LINT_FAST_LINTERS ?= errcheck,govet,ineffassign
TEST_CONCURRENCY ?= 4
GO_FORMAT_PATHS := cmd pkg semantic-engine integration-tests/integration_test.go main.go
LINT_MODULES := . $(patsubst %/go.mod,%,$(filter-out go.mod,$(shell git ls-files '*go.mod')))

# Suppress CGO linker warnings on macOS (not needed on Linux/Windows)
ifeq ($(shell go env GOOS),darwin)
export CGO_LDFLAGS=-Wl,-w
export LDFLAGS=-Wl,-w
endif

JQ_REL_PATH = jq --arg prefix "$$(pwd)" 'walk(if type == "object" and has("path") and (.path | type == "string") then .path |= (if . == $$prefix then "integration-tests" elif startswith($$prefix + "/") then .[($$prefix | length + 1):] elif startswith($$prefix) then .[($$prefix | length):] elif startswith("integration-tests/") then .[16:] else . end) else . end)'

.PHONY: all clean test test-full test-unit build build-no-duckdb docs-app format format-ci lint lint-fast lint-full lint-ci pre-commit refresh-integration-expectations integration-test-cloud integration-test-mssql validate-links setup tools-update
all: clean deps test build

deps: 
	@printf "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)\n"
	@go mod tidy

build: deps
	@echo "$(OK_COLOR)==> Building the application...$(NO_COLOR)"
	@$(MAKE) rustsqlparser-lib
	@CGO_ENABLED=1 go build -v -tags="no_duckdb_arrow" -ldflags="-s -w -X main.Version=$(or $(tag), dev-$(shell git describe --tags --abbrev=0)) -X main.telemetryKey=$(TELEMETRY_KEY)" -o "$(BUILD_DIR)/$(NAME)" "$(BUILD_SRC)"

build-no-duckdb: deps
	@echo "$(OK_COLOR)==> Building the application without DuckDB support...$(NO_COLOR)"
	@CGO_ENABLED=0 go build -v -tags="bruin_no_duckdb" -ldflags="-s -w -X main.Version=$(or $(tag), dev-$(shell git describe --tags --abbrev=0)) -X main.telemetryKey=$(TELEMETRY_KEY)" -o "$(BUILD_DIR)/$(NAME)-no-duckdb" "$(BUILD_SRC)"

docs-app:
	@echo "$(OK_COLOR)==> Building docs SPA bundle...$(NO_COLOR)"
	@npm run docs:app:build

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
	@cd integration-tests && env SILENT=1 SF_DISABLE_MINICORE=true go test -tags="no_duckdb_arrow" -v -count=1 .

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
	@cd integration-tests && env SILENT=1 SF_DISABLE_MINICORE=true go test -tags="no_duckdb_arrow" -v -count=1 -run "^(TestIndividualTasks|TestWorkflowTasks)" .

integration-test-cloud: build
	@touch integration-tests/cloud-integration-tests/.git
	@touch integration-tests/cloud-integration-tests/bruin
	@rm -rf integration-tests/cloud-integration-tests/.git
	@rm integration-tests/cloud-integration-tests/bruin
	@echo "$(OK_COLOR)==> Running cloud integration tests...$(NO_COLOR)"
	@cd integration-tests && git init
	@cd integration-tests/cloud-integration-tests && env SILENT=1 SF_DISABLE_MINICORE=true go test -count=1 -v .

integration-test-mssql: build
	@echo "$(OK_COLOR)==> Running MSSQL integration tests...$(NO_COLOR)"
	@cd integration-tests/mssql && env SILENT=1 SF_DISABLE_MINICORE=true go test -count=1 -v -timeout 15m .

clean:
	@rm -rf ./bin

test: test-unit

test-unit:
	@echo "$(OK_COLOR)==> Running the unit tests (fast)$(NO_COLOR)"
	@$(MAKE) rustsqlparser-lib
	@env SF_DISABLE_MINICORE=true go test -tags="no_duckdb_arrow" -p "$(TEST_CONCURRENCY)" -vet=off -timeout 10m ./cmd/... ./pkg/...
	@echo "$(OK_COLOR)==> Running the semantic-engine module tests$(NO_COLOR)"
	@cd semantic-engine && env SF_DISABLE_MINICORE=true go test -p "$(TEST_CONCURRENCY)" -timeout 10m ./...

test-full:
	@echo "$(OK_COLOR)==> Running the unit tests (full)$(NO_COLOR)"
	@$(MAKE) rustsqlparser-lib
	@env SF_DISABLE_MINICORE=true go test -tags="no_duckdb_arrow" -race -p "$(TEST_CONCURRENCY)" -vet=off -timeout 10m ./cmd/... ./pkg/...
	@echo "$(OK_COLOR)==> Running the semantic-engine module tests with race detection$(NO_COLOR)"
	@cd semantic-engine && env SF_DISABLE_MINICORE=true go test -race -p "$(TEST_CONCURRENCY)" -timeout 10m ./...

RUST_LIB = pkg/sqlparser/rustffi/target/release/libbruin_rustsqlparser.a

rustsqlparser-lib: $(RUST_LIB)

$(RUST_LIB): pkg/sqlparser/rustffi/Cargo.toml $(wildcard pkg/sqlparser/rustffi/src/*.rs)
	@echo "$(OK_COLOR)==> Building Rust SQL parser static library$(NO_COLOR)"
	@cargo build --release --manifest-path pkg/sqlparser/rustffi/Cargo.toml

format: lint-python
	@echo "$(OK_COLOR)>> [gci] formatting$(NO_COLOR)"
	@go tool gci write $(GO_FORMAT_PATHS)
	@echo "$(OK_COLOR)>> [gofumpt] formatting$(NO_COLOR)"
	@go tool gofumpt -w $(GO_FORMAT_PATHS)
	@$(MAKE) lint

# Fast edit-loop check on changed Go packages. `go vet` is deliberately absent
# because govet is already enabled by golangci-lint.
lint:
	@echo "$(OK_COLOR)==> Running fast linters on packages changed since $(LINT_MERGE_BASE)$(NO_COLOR)"
	@set -e; \
	for module in $(LINT_MODULES); do \
		build_tags=""; \
		if [ "$$module" = "." ]; then build_tags="$(LINT_BUILD_TAGS)"; fi; \
		LINT_MODULE_DIR="$$module" LINT_BUILD_TAGS="$$build_tags" LINT_MERGE_BASE="$(LINT_MERGE_BASE)" LINT_CONCURRENCY="$(LINT_CONCURRENCY)" LINT_TIMEOUT="$(LINT_TIMEOUT)" LINT_PARALLEL_FLAGS="$(LINT_PARALLEL_FLAGS)" LINT_ENABLE_ONLY="$(LINT_FAST_LINTERS)" ./hack/lint-changed.sh; \
	done

lint-fast: lint

# Full check for CI and pre-merge validation.
lint-full:
	@echo "$(OK_COLOR)==> Running all linters across the repository$(NO_COLOR)"
	@golangci-lint run --timeout "$(LINT_TIMEOUT)" --concurrency "$(LINT_CONCURRENCY)" $(LINT_PARALLEL_FLAGS) --build-tags="$(LINT_BUILD_TAGS)" ./...
	@cd semantic-engine && golangci-lint run --timeout "$(LINT_TIMEOUT)" --concurrency "$(LINT_CONCURRENCY)" $(LINT_PARALLEL_FLAGS) ./...

# Check Go formatting without modifying files.
format-ci:
	@echo "$(OK_COLOR)==> Checking Go formatting$(NO_COLOR)"
	@GCI_DIFF=$$(go tool gci list $(GO_FORMAT_PATHS)) || exit $$?; \
	GOFUMPT_DIFF=$$(go tool gofumpt -d $(GO_FORMAT_PATHS)) || exit $$?; \
	if [ -n "$$GCI_DIFF$$GOFUMPT_DIFF" ]; then \
		echo "$(ERROR_COLOR)Files need formatting:$(NO_COLOR)"; \
		[ -z "$$GCI_DIFF" ] || echo "$$GCI_DIFF"; \
		[ -z "$$GOFUMPT_DIFF" ] || echo "$$GOFUMPT_DIFF"; \
		exit 1; \
	fi
	@echo "$(OK_COLOR)All Go files are properly formatted$(NO_COLOR)"

lint-ci: format-ci lint-full
	@echo "$(OK_COLOR)All checks passed$(NO_COLOR)"

setup:
	@printf "$(OK_COLOR)==> Installing development tools$(NO_COLOR)\n"
	@current_version="$$(golangci-lint version 2>/dev/null | awk '{ for (i = 1; i <= NF; i++) if ($$i == "version") { print "v"$$(i+1); exit } }')"; \
	if [ "$$current_version" != "$(GOLANGCI_LINT_VERSION)" ]; then $(GOLANGCI_LINT_INSTALL); fi

tools-update:
	go get github.com/daixiang0/gci@$(GCI_VERSION)
	go get mvdan.cc/gofumpt@$(GOFUMPT_VERSION)
	$(GOLANGCI_LINT_INSTALL)
	@go mod tidy

lint-python:
	@[ -d .venv ] || uv venv --quiet
	@uv pip install --quiet sqlglot==30.7.0
	@echo "$(OK_COLOR)==> Running Python formatting with ruff...$(NO_COLOR)"
	@uvx ruff@$(RUFF_VERSION) format ./pythonsrc

	@echo "$(OK_COLOR)==> Running Python linting with ruff...$(NO_COLOR)"
	@uvx ruff@$(RUFF_VERSION) check --fix ./pythonsrc

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
	@cd integration-tests && ../bin/bruin internal connections | $(JQ_REL_PATH) > expectations/expected_connections_schema.json
	@cd integration-tests && ../bin/bruin connections list -o json . | $(JQ_REL_PATH) > expectations/expected_connections.json
	@echo "$(OK_COLOR)==> Integration expectations refreshed successfully!$(NO_COLOR)"

validate-links:
	@echo "$(OK_COLOR)==> Validating web links in repository...$(NO_COLOR)"
	@if ! command -v python3 > /dev/null 2>&1; then \
		echo "$(ERROR_COLOR)Python 3 not found. Please install Python 3 to validate links.$(NO_COLOR)"; \
		exit 1; \
	fi
	@python3 scripts/validate_links.py . || (echo "$(ERROR_COLOR)Link validation found broken links. Please fix them.$(NO_COLOR)" && exit 1)

# sometimes vendoring doesn't move the precompiled library
duck-db-static-lib:
	@mkdir vendor/github.com/marcboeker/go-duckdb/deps || true
	@mkdir vendor/github.com/marcboeker/go-duckdb/deps/$(OS_ARCH) || true
	@cp $$(go env GOPATH)/pkg/mod/github.com/marcboeker/go-duckdb@v1.8.2/deps/$(OS_ARCH)/libduckdb.a vendor/github.com/marcboeker/go-duckdb/deps/$(OS_ARCH)/libduckdb.a
