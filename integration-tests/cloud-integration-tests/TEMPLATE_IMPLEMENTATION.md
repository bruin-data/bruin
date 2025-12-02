# Templated Cloud Integration Tests - Implementation

This document describes the implementation of templated cloud integration tests.

## Files Created

### 1. `platform_config.go`
Defines platform-specific configurations for templated tests.

**Key Features:**
- `PlatformConfig` struct with platform-specific settings
- Configurations for Postgres, Snowflake, and BigQuery
- Settings include:
  - Connection names
  - Schema prefixes
  - Query flag patterns (--asset vs --connection)
  - Exit code expectations
  - Temp directory usage
  - Error message patterns

### 2. `cloud-template_test.go`
Templated test implementation for SCD2-by-column workflow.

**Key Features:**
- `buildSCD2ByColumnWorkflow()` function that creates platform-agnostic workflows
- `TestTemplatedSCD2ByColumn()` test that runs the templated test for all available platforms
- Handles platform differences:
  - Temp directory vs current folder usage
  - Query flag variations
  - Exit code expectations
  - Setup steps (git init, file copying)

## How It Works

### Platform Configuration
Each platform has a configuration that defines:
- How to construct query commands
- Expected exit codes for operations
- File path patterns
- Setup requirements

### Workflow Building
The `buildSCD2ByColumnWorkflow()` function:
1. Determines file paths based on platform (tempDir vs currentFolder)
2. Builds query commands based on platform flags (--asset vs --connection)
3. Constructs all workflow steps with platform-specific values
4. Returns a complete `e2e.Workflow` ready to execute

### Test Execution
The `TestTemplatedSCD2ByColumn()` test:
1. Checks for cloud configuration file
2. Gets available platforms from config
3. Runs the templated test for each platform (postgres, snowflake, bigquery)
4. Each platform test runs in parallel

## Platform Differences Handled

### Postgres
- Uses temp directory
- Uses `--connection` flag (not `--asset`)
- DROP TABLE returns exit code 1
- Requires git init and file copying setup

### Snowflake
- Uses current folder (no temp directory)
- Uses `--asset` flag with `--env`
- DROP TABLE returns exit code 0
- Requires restoring menu asset to initial state

### BigQuery
- Uses temp directory
- Uses `--asset` flag with `--env`
- DROP TABLE returns exit code 1
- Has extra initial step to drop table if exists
- Requires git init and file copying setup

## Usage

Run the templated tests:
```bash
cd integration-tests/cloud-integration-tests
go test -v -run TestTemplatedSCD2ByColumn
```

Or run all cloud integration tests:
```bash
make integration-test-cloud
```

## Next Steps

1. **Validate with 2-3 platforms**: Test with Postgres, Snowflake, and BigQuery
2. **Iterate based on learnings**: Fix any issues discovered during testing
3. **Template more tests**: Once SCD2-by-column works, template other common tests:
   - SCD2-by-time
   - Products-create-and-validate
   - Other shared workflows

## Benefits

- **Single source of truth**: Test logic defined once
- **Consistent coverage**: All platforms get the same test
- **Easier maintenance**: Changes made once affect all platforms
- **Easier platform addition**: Add new platform by adding config entry

## Known Limitations

- Currently only templates SCD2-by-column test
- Platform-specific tests (like BigQuery's drop-on-mismatch) remain separate
- SQL files are not templated yet (using existing platform-specific files)

