# R Assets

Bruin brings R statistical computing capabilities to your data pipelines:
- Run R scripts with full access to R's powerful statistical and data analysis packages
- Automatic dependency management with renv integration
- Access to connection credentials and secrets via environment variables
- Execute complex statistical computations alongside SQL and Python assets

R assets allow you to leverage R's extensive ecosystem for statistical analysis, machine learning, data visualization, and more within your Bruin pipelines.

```r
# @bruin
#
# name: statistical_analysis
# type: r
# connection: postgres
#
# @bruin

library(dplyr)
library(ggplot2)

# Perform statistical analysis
cat("Running R statistical analysis\n")

# Your R code here
results <- data.frame(
  metric = c("mean", "median", "sd"),
  value = c(42.5, 40.0, 5.2)
)

print(results)
```

## Dependency Management

R assets support dependency management through [renv](https://rstudio.github.io/renv/), R's standard dependency management tool. Bruin searches for the closest `renv.lock` file in the file tree and automatically restores the environment with the specified packages.

For example, assume you have a file tree such as:
```
* folder1/
    * folder2/
        * analysis.r
        * renv.lock
    * folder3/
        * report.r
    * renv.lock
* folder4/
    * folder5/
        * folder6/
            * model.r
* renv.lock
```

* When Bruin runs `analysis.r`, it will use `folder1/folder2/renv.lock` since they are in the same folder
* For `report.r`, since there is no `renv.lock` in the same folder, Bruin goes up one level and finds `folder1/renv.lock`
* Similarly, `renv.lock` in the main folder is used for `model.r` since none of `folder6`, `folder5`, or `folder4` have any `renv.lock` files

### Using renv

To create an `renv.lock` file for your R assets:

```r
# In your R console, navigate to your asset directory
renv::init()               # Initialize renv for the project
renv::install("dplyr")     # Install packages you need
renv::install("ggplot2")
renv::snapshot()           # Create renv.lock file
```

### Manual Dependency Management

If you don't use `renv.lock`, you can manage dependencies directly in your R script using `install.packages()`:

```r
# @bruin
#
# name: manual_deps_example
# type: r
#
# @bruin

# Check if package is installed, install if not
if (!require("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite", repos = "https://cloud.r-project.org")
}

library(jsonlite)

# Your code here
```

## Asset Definition

R assets use R comments with the `@bruin` marker to define metadata:

```r
# @bruin
#
# name: asset_name
# type: r
# depends:
#   - upstream_asset
#
# secrets:
#   - key: MY_SECRET
#     inject_as: R_SECRET
#
# @bruin

# Your R code starts here
cat("Hello from R!\n")
```

### Parameters

| Parameter | Required | Description |
|:----------|:---------|:------------|
| `name` | Yes | The unique identifier for the asset |
| `type` | No | Asset type, automatically set to `r` for `.r` files |
| `depends` | No | List of upstream assets this asset depends on |
| `secrets` | No | List of secrets/connections to inject as environment variables |
| `connection` | No | Default connection to use for this asset |

## Secrets

Bruin supports injecting connections into your R assets as environment variables.

You can define secrets in your asset definition using the `secrets` key, and Bruin will automatically make them available as environment variables during execution.
The injected secret is a JSON representation of the connection model.

This is useful for API keys, passwords, database credentials, and other sensitive information.

```r
# @bruin
#
# name: r_with_secrets
# secrets:
#   - key: postgres_connection
#
# @bruin

# Access the secret from environment variable
connection_json <- Sys.getenv("postgres_connection")

# Parse JSON if needed
library(jsonlite)
conn_details <- fromJSON(connection_json)

# Use connection details
cat(sprintf("Connecting to: %s\n", conn_details$host))
```

By default, secrets are injected as environment variables using the key name. If you want to inject a secret under a different environment variable name, you can use the `inject_as` field:

```r
# @bruin
#
# name: r_renamed_secret
# secrets:
#   - key: postgres_connection
#     inject_as: DB_CREDS
#
# @bruin

# Access using the custom name
db_credentials <- Sys.getenv("DB_CREDS")
```

This allows you to map a secret key to any environment variable name you prefer inside your R code.

## Environment Variables

Bruin introduces a set of environment variables by default to every R asset.

### Builtin

The following environment variables are available in every R asset execution:

| Environment Variable    | Description                                                                                                       |
|:------------------------|:------------------------------------------------------------------------------------------------------------------|
| `BRUIN_START_DATE`      | The start date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)                                     |
| `BRUIN_START_DATETIME`  | The start date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`)          |
| `BRUIN_START_TIMESTAMP` | The start timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`) |
| `BRUIN_END_DATE`        | The end date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)                                       |
| `BRUIN_END_DATETIME`    | The end date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`)            |
| `BRUIN_END_TIMESTAMP`   | The end timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`)   |
| `BRUIN_RUN_ID`          | The unique identifier for the pipeline run                                                                        |
| `BRUIN_PIPELINE`        | The name of the pipeline being executed                                                                           |
| `BRUIN_FULL_REFRESH`    | Set to `1` when the pipeline is running with the `--full-refresh` flag, empty otherwise                           |
| `BRUIN_THIS`            | The name of the R asset                                                                                           |
| `BRUIN_ASSET`           | The name of the R asset (same as BRUIN_THIS)                                                                      |

### Pipeline

Bruin supports user-defined variables at a pipeline level. These become available as a JSON document in your R asset as `BRUIN_VARS`. When no variables exist, `BRUIN_VARS` is set to `{}`. See [pipeline variables](/getting-started/pipeline-variables) for more information on how to define and override them.

Here's an example:

```r
# @bruin
#
# name: r_with_variables
#
# @bruin

library(jsonlite)

# Access pipeline variables
vars_json <- Sys.getenv("BRUIN_VARS")
vars <- fromJSON(vars_json)

# Use variables in your R code
cat(sprintf("Environment: %s\n", vars$environment))
cat(sprintf("Region: %s\n", vars$region))
```

## Examples

### Basic R Script

The simplest R asset with no dependencies:

```r
# @bruin
#
# name: hello_r
#
# @bruin

cat("Hello from R!\n")
result <- 2 + 2
cat(sprintf("2 + 2 = %d\n", result))
```

### Statistical Analysis with Dependencies

Using R packages for statistical analysis:

```r
# @bruin
#
# name: statistical_summary
# depends:
#   - raw_data
#
# @bruin

library(dplyr)

# Assuming you have data from upstream
cat("Performing statistical analysis\n")

# Generate sample data
data <- data.frame(
  value = rnorm(1000, mean = 50, sd = 10),
  category = sample(c("A", "B", "C"), 1000, replace = TRUE)
)

# Statistical summary
summary_stats <- data %>%
  group_by(category) %>%
  summarise(
    mean = mean(value),
    median = median(value),
    sd = sd(value),
    min = min(value),
    max = max(value)
  )

print(summary_stats)
```

### Working with Database Connections

Accessing database credentials via environment variables:

```r
# @bruin
#
# name: db_analysis
# secrets:
#   - key: postgres-default
#     inject_as: PG_CONN
#
# @bruin

library(jsonlite)
library(DBI)
library(RPostgres)

# Parse connection details
conn_json <- Sys.getenv("PG_CONN")
conn_details <- fromJSON(conn_json)

# Connect to database
con <- dbConnect(
  RPostgres::Postgres(),
  host = conn_details$host,
  port = conn_details$port,
  dbname = conn_details$database,
  user = conn_details$username,
  password = conn_details$password
)

# Query data
result <- dbGetQuery(con, "SELECT COUNT(*) FROM users")
cat(sprintf("Total users: %d\n", result$count))

# Clean up
dbDisconnect(con)
```

### Time-Series Analysis

Using R's extensive time-series capabilities:

```r
# @bruin
#
# name: time_series_forecast
# depends:
#   - historical_data
#
# @bruin

library(forecast)
library(lubridate)

cat("Running time-series forecast\n")

# Example time series
ts_data <- ts(rnorm(100), frequency = 12, start = c(2020, 1))

# Fit ARIMA model
model <- auto.arima(ts_data)

# Forecast next 12 periods
forecast_result <- forecast(model, h = 12)

cat("Forecast complete!\n")
print(summary(forecast_result))
```

## Installation

R assets require R to be installed on your system. Install R using one of these methods:

- **macOS**: `brew install r`
- **Ubuntu/Debian**: `sudo apt-get install r-base`
- **Windows**: Download from [CRAN](https://cran.r-project.org/bin/windows/base/)
- **Other platforms**: See [CRAN installation guides](https://cran.r-project.org/)

To verify R is installed correctly:
```bash
R --version
```

## Best Practices

1. **Use renv for reproducibility**: Create an `renv.lock` file to ensure consistent package versions across environments
2. **Handle errors gracefully**: Use R's error handling (`tryCatch`) to provide clear error messages
3. **Log progress**: Use `cat()` or `print()` statements to provide visibility into your R script's execution
4. **Clean up resources**: Always close database connections and file handles when done
5. **Test locally**: Run your R scripts locally before integrating them into pipelines

## Limitations

- R assets currently support script execution only (no table materialization in the initial version)
- Column checks and custom SQL checks are not available for R assets
- R version must be installed on the system (automatic R version management is not yet supported)
