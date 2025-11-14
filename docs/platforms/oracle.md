# Oracle Database

Bruin supports Oracle Database for cataloging, lineage tracking, database importing, and querying capabilities. Currently, Oracle support is focused on data discovery and documentation rather than full data pipeline workloads.

## Connection

Oracle Database requires a connection configuration, which can be added to `connections` in the `.bruin.yml` file complying with the following schema:


### Required Configuration

To connect to Oracle Database, you need the following required parameters:

- `username`: Oracle database username
- `password`: Oracle database password  
- `host`: Database server hostname or IP address
- `service_name`: Oracle service name (required if `sid` is not provided)
- `sid`: Oracle System Identifier (required if `service_name` is not provided)

**Note**: You must provide either `service_name` or `sid`, but not both.

### Additional Configuration Options

The following parameters are optional and can be used to customize your connection:

- `port`: Database port (optional, defaults to 1521)
- `ssl`: Enable SSL connection (optional, defaults to false)
- `ssl_verify`: Verify SSL certificates (optional, defaults to true)
- `prefetch_rows`: Number of rows to prefetch for performance optimization (optional)
- `trace_file`: Path to trace file for debugging (optional)
- `wallet`: Path to Oracle wallet for SSL connections (optional)
- `role`: Database role for privileged connections like SYSDBA (optional)

### Example Configuration

```yaml
connections:
  oracle:
    - name: "oracle-default"
      username: "hr"
      password: "hr123"
      host: "oracle-server.company.com"
      port: "1521"
      service_name: "PROD"
```

## Testing Connection

You can test your Oracle connection using the following command:

```bash
bruin connections test --name oracle-default
```

This will verify that Bruin can successfully connect to your Oracle database using the provided configuration.

## Querying Data

You can query your Oracle tables directly using Bruin's query command:

```bash
bruin query --connection oracle-default --query "SELECT * FROM employees FETCH FIRST 10 ROWS ONLY"
```

Alternatively, you can use the query preview feature in the VSCode extension for an interactive querying experience.

## Import Database

Bruin supports importing your existing Oracle database structure into your pipeline. This command will scan your Oracle database and create asset files for all tables:

```bash
bruin import database --connection oracle-default path/to/your/pipeline
```

This will:
- Connect to your Oracle database
- Scan all accessible schemas and tables (excluding system schemas)
- Create corresponding asset definition files in your pipeline's `assets/` folder
- Generate proper naming conventions for your Oracle assets

You can also import from a specific schema:

```bash
bruin import database --connection oracle-default --schema HR path/to/your/pipeline
```

## Oracle Assets

### `oracle.sql`

Defines Oracle SQL assets for documentation and query preview purposes. Currently, these assets are no-op (they don't execute), but they're useful for:

- Documenting your Oracle queries, tables, users, databases, and business logic
- Using the [Definition Schema](../assets/definition-schema.md) to document columns, descriptions, and dependencies
- Query preview functionality in the VSCode extension
- Adding business context with tags, domains, and ownership information
- Defining data quality checks and validation rules
- Setting up custom checks for entire assets
- Organizing assets with metadata for better discoverability


#### Example: Document a query with column descriptions

```bruin-sql
/* @bruin
name: hr.employees_summary
type: oracle.sql
description: "Employee summary by department with salary analytics"
columns:
  department_id:
    description: "Department identifier"
    type: "NUMBER"
  employee_count:
    description: "Number of employees in the department"
    type: "NUMBER"
  avg_salary:
    description: "Average salary across the department"
    type: "NUMBER"
  latest_hire:
    description: "Most recent hire date in the department"
    type: "DATE"
@bruin */

SELECT 
    department_id,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MAX(hire_date) as latest_hire
FROM hr.employees
GROUP BY department_id
```

### `oracle.source`

Defines Oracle source assets for documenting existing tables and views in your Oracle database. These assets are also no-op but useful for:

- Documenting existing Oracle tables and views
- Adding column descriptions and metadata
- Establishing lineage relationships
- Query preview functionality

#### Example: Document an existing Oracle table

```yaml
name: hr.employees
type: oracle.source
description: "Employee master data table containing all employee information including personal details, job information, and salary data"
connection: oracle-default

# Tags and domains for categorization
tags:
  - hr
  - master-data
  - employee
domains:
  - human-resources
  - employee-management

# Metadata for additional context
meta:
  business_owner: "HR Department"
  data_steward: "hr-admin@company.com"
  refresh_frequency: "daily"
  retention_policy: "7 years"

# Dependencies on other assets
depends:
  - hr.departments
  - hr.jobs

# Column definitions with comprehensive metadata
columns:
  - name: employee_id
    type: "NUMBER"
    description: "Unique identifier for each employee"

  - name: first_name
    type: "VARCHAR2(50)"
    description: "Employee's first name"

  - name: last_name
    type: "VARCHAR2(50)"
    description: "Employee's last name"
```

#### Visualizing Lineage in VSCode

When you define dependencies between your Oracle assets, you can visualize the lineage relationships directly in VSCode. Here's how the `hr.employees` asset dependencies would appear:

![Oracle Asset Lineage](/oracle-lineage.png)


In this diagram, you can see that `hr.employees` depends on both `hr.departments` and `hr.jobs` assets, which is defined in the `depends` section of the asset configuration. The VSCode extension provides this visual representation to help you understand data flow and relationships between your Oracle tables and views.

## Ingesting Data from Oracle

While the `oracle.sql` and `oracle.source` asset types are useful for documentation and lineage, you can use [Ingestr assets](../assets/ingestr.md) to actually move data from Oracle to your data warehouse platforms like BigQuery, Snowflake, Redshift, or Synapse.

### Example: Ingest Oracle table to BigQuery

This example shows how to copy a table from Oracle to BigQuery:

```yaml
name: raw.employees
type: ingestr
parameters:
  source_connection: oracle-default
  source_table: hr.employees
  destination: bigquery
```

### Example: Incremental load from Oracle to BigQuery

This example demonstrates how to use an `updated_at` column to incrementally load data from Oracle to BigQuery:

```yaml
name: raw.orders
type: ingestr
parameters:
  source_connection: oracle-default
  source_table: sales.orders
  destination: bigquery
  incremental_strategy: merge
  incremental_key: updated_at

columns:
  - name: order_id
    type: number
    primary_key: true
  - name: updated_at
    type: date
```

In this example:
- The `incremental_strategy: merge` ensures that existing records are updated based on the primary key
- The `incremental_key: updated_at` tells ingestr to only load records that have been updated since the last run
- The `primary_key` column definition is used by ingestr to identify unique records during merge operations

For more information about ingestr capabilities and configuration options, see the [Ingestr Assets documentation](../assets/ingestr.md).
