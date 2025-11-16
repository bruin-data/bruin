# Bruin - R Template

This pipeline is a simple example of a Bruin pipeline that demonstrates R statistical computing capabilities.

The pipeline includes R assets that showcase:
- Basic R script execution
- Statistical computations and data analysis
- Dependency management with renv
- Environment variable access

This template is a good example of Bruin's R execution abilities.

A couple of remarks:
- R assets can use packages managed through renv for reproducible environments
- Scripts have access to environment variables for connections and secrets
- R scripts can perform complex statistical analysis alongside SQL and Python assets

## Prerequisites

You need to have R installed on your system:
- **macOS**: `brew install r`
- **Ubuntu/Debian**: `sudo apt-get install r-base`
- **Windows**: Download from [CRAN](https://cran.r-project.org/bin/windows/base/)

To verify R is installed:
```shell
R --version
```

## Running the pipeline

Run the following command:
```shell
bruin run .
```

```shell
Starting the pipeline execution...

[2024-01-15T18:25:14Z] [worker-0] Running: basic_stats
[2024-01-15T18:25:14Z] [worker-0] [basic_stats] >> Running basic statistical analysis
[2024-01-15T18:25:14Z] [worker-0] [basic_stats] >> Mean: 50.123
[2024-01-15T18:25:14Z] [worker-0] Completed: basic_stats (142ms)

Executed 2 tasks in 1.2s
```

You can also run a single task:

```shell
bruin run assets/basic/analysis.r
```

```shell
Starting the pipeline execution...

[2024-01-15T18:26:00Z] [worker-0] Running: basic_stats
[2024-01-15T18:26:00Z] [worker-0] [basic_stats] >> Analysis complete!
[2024-01-15T18:26:00Z] [worker-0] Completed: basic_stats (98ms)

Executed 1 tasks in 98ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

## Using renv for dependencies

The `with-deps` example includes an `renv.lock` file that specifies exact package versions. Bruin will automatically:
1. Detect the renv.lock file
2. Install the specified packages
3. Use the isolated environment for execution

That's it, good luck!
