# Integration Tests

This directory contains integration tests for the Bruin data pipeline tool. These tests verify that Bruin works correctly with real pipelines, databases, and various configurations.

## Overview

The integration tests are designed to test Bruin's functionality in realistic scenarios, including:

- **Individual Tasks**: Testing specific Bruin commands and features
- **Workflow Tests**: Testing multi-step workflows and state management
- **Ingestr Tests**: Testing integration with the Ingestr data ingestion tool

## Test Structure

### Test Files

- `integration_test.go`: Main test file containing all integration tests
- `test-pipelines/`: Directory containing test pipeline configurations

### Test Categories

1. **Individual Tasks** (`TestIndividualTasks`): Tests for specific Bruin commands

2. **Workflow Tests** (`TestWorkflowTasks`): Tests for multi-step workflows

3. **Ingestr Tests** (`TestIngestrTasks`): Tests for Ingestr integration

### Make Commands

The following make commands are available for running integration tests:

#### Full Integration Tests
```bash
# Run all integration tests (including ingestr tests)
make integration-test
# Short form:
make it-all
# Run with serial execution for better readability
make it-all SERIAL=1
```

#### Individual Test Categories

```bash
# Run only individual task tests
make integration-test-individual
# Alternativ short form
make it-indivdual
# To run with serial execution
make it-individual SERIAL=1

# Run only workflow tests
make integration-test-workflow
# Alternative short form:
make it-workflow
# To run with serial execution
make it-workflow SERIAL=1

# Run only ingestr tests
make integration-test-ingestr
# Alternative shoft form:
make it-ingestr
# To run with serial execution
make it-ingester SERIAL=1
```

## Test Pipeline Structure

Each test pipeline in `test-pipelines/` should follow a standard structure:

```
test-pipelines/
├── pipeline-name/
│   ├── pipeline.yml          # Pipeline configuration
│   ├── assets/               # Pipeline assets (SQL, Python, etc.)
│   │   ├── asset1.sql
│   │   └── asset2.py
│   └── expectations/         # Expected outputs (if applicable)
│       ├── expected.json
│       └── expected.csv
```

### Test Isolation
Each test runs in isolation with:
- Clean temporary directories
- Fresh database instances
- Isolated file systems

## Troubleshooting

### Common Issues

1. **Binary Not Found**: Ensure Bruin is built before running tests
   ```bash
   make build
   ```

2. **Database Connection Issues**: Check that test databases are accessible

3. **Permission Issues**: Ensure write permissions for temporary directories

4. **Ingestr Tests Failing**: Rerun tests. If that doesn't help, verify Ingestr is properly configured if running ingestr tests


