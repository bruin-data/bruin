# Bruin - Athena Template

This pipeline template is designed for data processing workflows using Amazon Athena. It demonstrates how to use Bruin to define and execute SQL queries in Athena with a predefined schedule and set of assets.

---

## Pipeline Overview

The **Athena Pipeline** includes several SQL assets and a configuration file (`pipeline.yml`) that define the pipeline’s structure and schedule.

## Pipeline Assets

The pipeline includes the following SQL assets located in the `assets/` folder:

- **`cars.sql`**: Defines an Athena cars table with schema, constraints, and sample data.
- **`drivers.sql`**: Creates an Athena drivers table with schema, constraints, and data.
- **`payments.sql`**:  Defines an Athena view for payments with positive amount checks.
- **`travellers.sql`**: Creates an Athena travellers table with schema, constraints, and sample data.

---

## Running the pipeline

Run these commands from the generated `athena` pipeline directory. To run the whole pipeline:

```shell
bruin run --start-date 2024-01-01 .
```

You can also run a single task:

```shell
bruin run assets/cars.sql
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!
