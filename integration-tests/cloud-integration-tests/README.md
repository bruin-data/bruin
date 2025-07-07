# Cloud Integration Tests

This directory contains cloud platform integration tests for Bruin. Tests are organized by platform and run independently based on available credentials.

## Architecture

### Directory Structure
```
cloud-integration-tests/
├── .bruin.cloud.yml                  # Cloud connections configuration
├── cloud-integration-test.go         # Central test orchestrator
├── README.md                         # This documentation
├── bigquery/                         # BigQuery platform tests
│   ├── bigquery_test.go             # BigQuery-specific test logic
│   ├── big-test-pipes/              # BigQuery test pipelines
│   ├── resources/                   # BigQuery test resources
│   └── *.csv                        # Expected test results
└── snowflake/                       # Snowflake platform tests
    └── snowflake_test.go            # Snowflake-specific test logic
```

### Platform Detection

The test runner automatically detects which platforms are available by parsing the `.bruin.cloud.yml` configuration:

- **Available Platforms**: Tests run normally
- **Missing Platforms**: Tests are gracefully skipped with informative logging
- **No Configuration**: All tests are skipped with helpful guidance

### Platform Mapping

The following platforms are currently supported:

| Platform  | Connection Type in Config | Status |
|-----------|---------------------------|---------|
| BigQuery  | `gcp`                    | ✅ Active |
| Snowflake | `snowflake`              | 🚧 Example |

## Configuration

### Setting Up Credentials

1. **Copy the configuration file**:
   ```bash
   cp integration-tests/cloud-integration-tests/.bruin.cloud.yml.example integration-tests/cloud-integration-tests/.bruin.cloud.yml
   ```

2. **Add your platform credentials**:
   ```yaml
   default_environment: default
   environments:
     default:
       connections:
         gcp:  # For BigQuery tests
           - name: bigquery-connection
             # ... your BigQuery credentials
         
         snowflake:  # For Snowflake tests
           - name: snowflake-connection
             # ... your Snowflake credentials
   ```

3. **Platform Detection**: Only platforms with configured connections will run tests.

## Running Tests

### All Cloud Tests
```bash
make integration-test-cloud
```

### Example Output
```
☁️  Running Cloud Integration Tests
=====================================

🧪 Running bigquery tests...
  📋 Running 1 tasks
  🔄 Running 2 workflows
  ✅ bigquery tests completed successfully

⏭️  Skipping snowflake tests (no connection configured)

📊 Cloud Integration Test Summary
==================================
✅ Platforms tested: 1
⏭️  Platforms skipped: 1
🧪 Total tests: 1
🔄 Total workflows: 2

💡 To run tests for skipped platforms, add their connections to:
   /path/to/integration-tests/cloud-integration-tests/.bruin.cloud.yml

🎉 All cloud integration tests completed successfully!
```

## Adding New Platforms

To add a new cloud platform (e.g., Athena):

### 1. Create Platform Directory
```bash
mkdir -p integration-tests/cloud-integration-tests/athena
```

### 2. Create Platform Test File
```go
// integration-tests/cloud-integration-tests/athena/athena_test.go
package athena

import (
    "path/filepath"
    "github.com/bruin-data/bruin/pkg/e2e"
)

func GetTasks(binary string, currentFolder string) []e2e.Task {
    configFlags := []string{"--config-file", filepath.Join(currentFolder, "cloud-integration-tests/.bruin.cloud.yml")}
    
    tasks := []e2e.Task{
        {
            Name:    "[athena] your test name",
            Command: binary,
            Args:    []string{"query", "--env", "athena-env", "--connection", "athena", "--query", "SELECT 1"},
            Expected: e2e.Output{ExitCode: 0},
            Asserts: []func(*e2e.Task) error{e2e.AssertByExitCode},
        },
    }
    
    for i := range tasks {
        tasks[i].Args = append(tasks[i].Args, configFlags...)
    }
    
    return tasks
}

func GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
    // Implement your workflows here
    return []e2e.Workflow{}
}
```

### 3. Register Platform Provider
Add to `cloud-integration-test.go`:

```go
// Import the package
import "github.com/bruin-data/bruin/integration-tests/cloud-integration-tests/athena"

// Add to platformConnectionMap
var platformConnectionMap = map[string]string{
    "bigquery":  "gcp",
    "snowflake": "snowflake", 
    "athena":    "athena",     // <- Add this line
}

// Add provider
type AthenaProvider struct{}

func (p AthenaProvider) GetTasks(binary string, currentFolder string) []e2e.Task {
    return athena.GetTasks(binary, currentFolder)
}

func (p AthenaProvider) GetWorkflows(binary string, currentFolder string) []e2e.Workflow {
    return athena.GetWorkflows(binary, currentFolder)
}

// Register in platformProviders map
platformProviders := map[string]PlatformTestProvider{
    "bigquery":  BigQueryProvider{},
    "snowflake": SnowflakeProvider{},
    "athena":    AthenaProvider{},  // <- Add this line
}
```

That's it! The new platform will automatically be detected and tested if credentials are available.

## Features

- ✅ **Automatic Platform Detection**: Only runs tests for configured platforms
- ✅ **Graceful Skipping**: Missing platforms are skipped with clear logging
- ✅ **Modular Architecture**: Each platform is self-contained
- ✅ **Easy Extension**: Adding new platforms requires minimal changes
- ✅ **Comprehensive Reporting**: Clear summary of what ran and what was skipped
- ✅ **Isolated Execution**: Cloud tests run independently from local tests

## Benefits

1. **Developer Friendly**: Developers don't need credentials for all platforms
2. **CI/CD Ready**: Can run different platforms in different pipeline stages
3. **Scalable**: Easy to add new cloud platforms
4. **Maintainable**: Platform-specific logic is contained and organized
5. **Robust**: Graceful handling of missing configurations 