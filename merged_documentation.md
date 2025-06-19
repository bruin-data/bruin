# Bruin Documentation

# What is Bruin?

End-to-end data framework: data ingestion + transformations + quality. 

If dbt, Airbyte, and Great Expectations had a lovechild, it would be Bruin.

<img alt="Bruin - Demo" src="/demo.gif" />

Bruin is packed with features:
- 📥 ingest data with [ingestr](https://github.com/bruin-data/ingestr) / Python
- ✨ run SQL & Python transformations on [many platforms](#supported-platforms)
- 📐 table/view [materializations](assets/materialization.md), incremental tables
- 🐍 run Python in isolated environments using [uv](https://github.com/astral-sh/uv)
- 💅 built-in data quality checks
- 🚀 Jinja templating to avoid repetition
- ✅ validate pipelines end-to-end via dry-run
- 👷 run on your local machine, an EC2 instance, or [GitHub Actions](cicd/github-action.md)
- 🔒 secrets injection via environment variables
- [VS Code extension](vscode-extension/overview.md) for a better developer experience
- ⚡ written in Golang
- 📦 [easy to install](getting-started/introduction/installation.md) and use

I know, that's a lot. Let's dive into the details.

You can get started with Bruin [via installing it](getting-started/introduction/installation.md) with a single command.


## Supported Platforms

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const platformsGroup = sidebarGroups.value.find(group => group.text === 'Data Platforms')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">

Bruin supports many data platforms out-of-the-box and as a first-class citizen. Feel free to get started with your favorite platform:

<ul>
<li v-for="platform in platformsGroup.items" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>


## Getting-Started


### Concepts

# Concepts

Bruin has a few simple concepts that enable you to make the most out of it.

## Asset
Anything that carries value that is derived from data is an asset from our perspective.

In more concrete terms, an asset can be:
- a table/view in your database
- a file in S3 / GCS
- a machine learning model
- an MS Excel/Google Sheets/Airbyte/Notion document

This abstraction enables building multi-language data pipelines that are agnostic of a database/destinations.
You will primarily be interacting with assets when using Bruin.

Here's an example SQL asset:

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

depends:
   - hello_python

materialization:
   type: table

columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

@bruin */

select 1 as one
union all
select 2 as one
```
### Building Blocks
An asset consists of two primary components:
- definition: the metadata that enables Bruin to learn about the asset and its metadata
- content: the actual query/logic that creates the asset

The details on the asset definition can be seen [here](../assets/definition-schema.md).

## Pipeline

A pipeline is a group of assets that are executed together in the right order. 
For instance, if you have an asset that ingests data from an API, and another one that creates another table from the ingested data, you have a pipeline.
Asset executions occur on a pipeline level.

A pipeline is defined with a `pipeline.yml` file, and all the assets need to be under a folder called `assets` next to this file:
```
- my-pipeline/
  ├─ pipeline.yml
  └─ assets/
    ├─ asset1.sql
    └─ asset2.py
```

Here's an example `pipeline.yml`:
```yaml
name: bruin-init
schedule: daily # relevant for Bruin Cloud deployments

default_connections:
  google_cloud_platform: "gcp"
  snowflake: "snowflake"
```

## Pipeline Run
When you run a pipeline, you create a "pipeline run". A pipeline run contains one or more asset instances that are executed in a given time with a specific configuration.

You can run a pipeline in the folder `my-pipeline` with the following command:
```shell
bruin run my-pipeline
```

## Asset Instance
An asset instance is a single execution of an asset at a given time. 
For instance, if you have a Python asset and you run it, Bruin creates an asset instance that executes your code.

Asset instance is an internal concept, although it is relevant to understand since actual executions are based on asset instances.

You can run an asset with the following command:
```shell
bruin run /path/to/the/asset/file.sql
```

## Connection
A connection is a set of credentials that enable Bruin to communicate with an external platform. 

Platform specific connections have specific schemas, and "generic" connections are built as key-value pairs to inject secrets into your assets from outside. 

Connections are defined in the `.bruin.yml` file locally. A connection has a name and the credentials.

When you run a pipeline, Bruin will find this file in the repo root, parse the connections there, build client objects internally to interact with these external platforms and then run your assets.

> [!INFO]
> The first time you run `bruin validate` or `bruin run`, Bruin will create an empty `.bruin.yml` file and add it to `.gitignore` automatically.


## Default Connections
Default connections are top-level defaults that reduces repetition by stating what connections to use on types of assets.
For instance, a pipeline might have SQL queries that run on Google BigQuery or Snowflake, and based on the type of an asset Bruin picks the appropriate connection.

## Defaults

Defaults allow you to set baseline values for a pipeline, which apply to all assets within it. If an asset has its own specified value, it will take precedence over the default. Otherwise, the default value will be used. below is an example of a default:

```yaml
name: bruin-init
schedule: daily 
default:
  type: ingestr
  parameters:
    source_connection: chess-default
    destination: duckdb
  secrets:
    - key: KEY1
      inject_as: INJECTED1
  interval_modifiers:
      start: 2h
      end: 2h
```

For more details, please check the example from the template [here](https://github.com/bruin-data/bruin/blob/main/templates/chess/pipeline.yml).

## Sensors
Sensors are a special type of assets that are used to wait on certain external signals. Sensors are useful to wait on external signals such as a table being created in an external database, or a file being uploaded to S3. A common usecase for sensors is when there are datasets/files/tables that are created by a separate process and you need to wait for them to be created before running your assets.



### Credentials

# Credentials

Bruin allows you to store all of your credentials in a non-versioned file called `.bruin.yml`. This file is not checked into your version control system and is not shared with your team.

The `.bruin.yml` file contains a list of credentials for each connection type, as well as among different environments.

Here's an example of a `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      generic:
        - name: MY_SECRET
          value: secretvalue
      duckdb:
        - name: "duckdb-default"
          path: "chess.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

  another_environment:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
```

When you run a pipeline, Bruin will find this file in the repo root, parse the connections there, build client objects internally to interact with these external platforms and then run your assets.

> [!INFO]
> The first time you run `bruin validate` or `bruin run`, Bruin will create an empty `.bruin.yml` file and add it to `.gitignore` automatically.

## Credential Types
Bruin supports many different types of credentials for each platform. Please visit the corresponding platform page from the sidebar to see the supported credential types.

## Generic Credentials
Generic credentials are key-value pairs that can be used to inject secrets into your assets from outside.

```yaml
default_environment: default
environments:
  default:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
```

Common use cases for generic credentials are API keys, passwords, etc.

## Environment Variables

Bruin supports injecting environment variables into your credentials in case you have the secrets defined elsewhere as well.

```yaml
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: my_postgres_connection
          username: ${POSTGRES_USERNAME}
          password: ${POSTGRES_PASSWORD}
          host: ${POSTGRES_HOST}
          port: ${POSTGRES_PORT}
          database: ${POSTGRES_DATABASE}
```

> [!INFO]
> Environment variables are not expanded in the `.bruin.yml` file. They are expanded when Bruin runs your assets.

## Custom Credentials File
Bruin looks for a `.bruin.yml` file in the project root by default; however, in some cases you might want to override the value per project.

In order to do that, you can simply use the `--config-file` flag in many commands, or you can use `BRUIN_CONFIG_FILE` environment variable.


### Design Principles

# Design Principles

Bruin is an opinionated data framework, which contains:
- various open-source components,
  - [Bruin CLI](https://github.com/bruin-data/bruin)
  - [ingestr](https://github.com/bruin-data/ingestr)
  - [Bruin VS Code extension](https://github.com/bruin-data/bruin-vscode)
- a framework to build scalable data products, both in terms of data size as well as the _data team size_ 
- and a managed data platform as a service, [Bruin Cloud](https://getbruin.com),

Below is a living list of design principles that outlines the vision behind Bruin, both as an open-source product as well as a business.

---

1. **Everything should be done via some form of a version-controllable text.** This means there should be no UI/database to configure anything in terms of how Bruin pipelines run, which enables proper reproducible builds for data pipelines.
2. **Real-world data pipelines use multiple technologies.** While SQL is incredibly powerful, there are a lot of usecases that are not doable via just SQL. Bruin supports both Python and SQL natively, as well as pre-built images/binaries to support more complex usecases.
3. **Real-world data pipelines depend on multiple sources and destinations.** A complete data platform needs to be able to fulfill the needs of various business functions, and enable creating a single source of truth, be it for marketing platforms, CRM systems, data warehouses, data lakes and many more. Data pipelines must be built to support the diverse set of sources and destinations.
4. **Real-world data pipelines need the ability to mix-and-match technologies, sources, and destinations.** Consequently, it is not enough for a platform to support multiple source and destinations if they cannot be combined. A single pipeline must be able to mix and match technologies, sources and destinations as needed without any disruption.
5. **Avoid lock-in.** Data is core, and lock-in in data should be avoided. This is why Bruin CLI runs on any environment, and supports all the core features out of the box via the open-source, Apache-licensed product.
6. **Anything can be a data asset.** A real-world data pipeline consists of many types of assets: tables, views, spreadsheets, files on S3, an ML model, anything. Anything that creates value with data is an asset, and the data platform should support incorporating different types of assets into the existing workflows gradually.
7. **Data must be treated as a product.** This means providing it as a trustworthy asset to the rest of the organization with the best customer experience by making it discoverable, interoperable, addressable, and self-describing. Taking care of the quality, governance, and maintainability of every data asset is a must for any forward-looking data team, and the tools must be built to support that.
8. **Built-in support for multiple environments is a must.** It should be possible to execute the same pipeline against a production database, as well as a development environment, without having to change the code.
9. **There should be not a single line of glue code.** Building data assets should have just the necessary code, and no glue. No pipelining code, no environment setup, no secret fetching, nothing additional.
10. **Data analysts & data scientists should be able to productionize their assets/pipelines by themselves.** This means taking the complexity away from these folks, and letting them focus on the core business logic.
11. **Data quality is a first-class citizen.** It is not, and it won't be an afterthought. The quality must be a part of every development process.
12. **Quick feedback cycle is key for better data development.** This means every solution we build should have the ability to quickly test things out, and run locally as much as possible. This is why we have a blazing-fast open-source CLI tool, and a local development environment that replicates the production.
13. **Top-notch developer experience is a must.** People that work with these tools may not know the inner-workings, the tools we built must go further in explaining what went wrong, and how to fix it, as well as making the data developers' day-to-day lives easier.

The list of principles here can be considered as a lens to look at [Bruin](https://getbruin.com) as a whole.



### Devenv

# Developer Environments

Bruin has native support for running the same pipeline against multiple environments. This feature is very useful for testing and development, as it allows you to quickly switch between different environments without having to change your code.

You can think of development environments as:
- **Staging**: A staging environment is a replica of the production environment. It is used to test new features and changes before they are deployed to production.
  - Staging environments often replicate the production data, using different databases and platforms to test and develop features. 
  - Teams may decide on whether to have shared staging environments vs separate environments per person.
- **Production**: The production environment is the live environment where the application is running. It is used by end users and customers.

You can define as many environments as you would like with Bruin, and run the same code against them.

## Setting up a development environment
Bruin manages all the credentials for different environments in your `.bruin.yml` file. In this file, you can define as many environments as you would like under the `environments` key.

```yaml
default_environment: staging
environments:
  staging:
    connections:
      generic:
        - name: MY_SECRET
          value: secretvalue
      duckdb:
        - name: "duckdb-default"
          path: "chess.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

  production:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
  
  some_other_env:
    connections:
      generic:
        - name: MY_SECRET
          value: someotherdifferentsecret
```

You can use different environments per each invocation by simply giving the name of the environment with the `--environment` flag.

```bash
bruin run --environment staging
```

### Limitations
Bruin is built around asset names having `<schema>.<table>` naming convention, which means that if you want to run the same code against a different environment, that new environment needs to be a separate database.

Imagine having Postgres as your data warehouse, you would effectively set up a different database within postgres for each development environment, that way you can create the same schema/table/view in each environment without impacting them.

While this approach works fine, there are some scenarios where it might not be beneficial, especially where:
- having replica environments is not possible since the data is too large;
- or, the source data is not accessible from other databases via cross-database queries, e.g. Redshift.

In these scenarios, we recommend using different schemas per environment within the same database, as described below.


## Schema-based Environments

Due to the limitations of separate database environments, Bruin also supports schema-based environments.

In this scenario, Bruin will simply modify the asset names and the SQL queries to add a prefix to the schema names, and intelligently modify the queries to use the correct schema.

You can enable schema-based environments by putting `schema_prefix` as a key in your `.bruin.yml` file.


```yaml
default_environment: staging
environments:
  staging:
    schema_prefix: jane_
    connections:
      generic:
        - name: MY_SECRET
          value: secretvalue
      duckdb:
        - name: "duckdb-default"
          path: "chess.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

  production:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
```

### How does it work?

Imagine running an asset called `mart.table1`:
```bruin-sql
/* @bruin
name: mart.table1
type: pg.sql

depends:
  - raw.table1
  - raw.table2
@bruin */

SELECT
  player,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
FROM raw.table1 t1
JOIN raw.table2 t2
  ON t1.player_id = t2.player_id
GROUP BY 1
```

If you were to visualize the data flow, this is what it looks like:
```mermaid
flowchart LR
  subgraph raw
  raw.table1
  raw.table2
  end
  subgraph mart
  mart.table1
  end
  raw.table1 --> mart.table1
  raw.table2 --> mart.table1
```


When you run this asset in a schema-prefixed environments, Bruin will do a few things:
- First of all, it will analyze the query and identify the used tables. 
  - In this example, it will identify that `chess.players` is found.
- In parallel, it will gather a list of available schemas and tables within the given database.
- It will check which of the referenced schemas and tables exist with the prefix.
- For each table that has a copy in a prefixed schema, Bruin will rewrite the query and the asset to use the prefixed schema names instead.

Imagine running this asset in an environment where `schema_prefix` is set to `dev1`.

#### No prefixed source exists
Bruin will check the database and identify that there is no `dev1_raw` schema, therefore the query will stay as is.
```sql
SELECT
  player,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
FROM raw.table1 t1
JOIN raw.table2 t2
  ON t1.player_id = t2.player_id
GROUP BY 1
```

The data flow is almost the same, just the destination changed:
```mermaid
flowchart LR
  subgraph raw
  raw.table1
  raw.table2
  end
  subgraph dev1_mart
  dev1_mart.table1
  end
  raw.table1 --> dev1_mart.table1
  raw.table2 --> dev1_mart.table1
```

#### Prefixed source exists for `raw.table1`

When analyzing the database, Bruin noticed that there is already a table called `dev1_raw.table1`, therefore it will rewrite the query to use the prefixed table.

```sql
SELECT
  player,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
FROM dev1_raw.table1 t1
JOIN raw.table2 t2
  ON t1.player_id = t2.player_id
GROUP BY 1
```

The data flow is almost the same, just the destination changed:
```mermaid
flowchart LR
  subgraph raw
  raw.table1
  raw.table2
  end
  subgraph dev1_raw
  dev1_raw.table1
  end
  subgraph dev1_mart
  dev1_mart.table1
  end
  dev1_raw.table1 --> dev1_mart.table1
  raw.table2 --> dev1_mart.table1
```

> [!IMPORTANT]
> Notice how Bruin rewrote the query to change the source from `raw.table1` to `dev1_raw.table1`.



#### Prefixed source exists for both `raw.table1` and `raw.table2`

When analyzing the database, Bruin noticed that there are already tables called `dev1_raw.table1` and `dev1_raw.table2`, therefore it will rewrite the query to use the prefixed tables.

```sql
SELECT
  player,
  COUNT(*) AS games_played,
  SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS wins,
FROM dev1_raw.table1 t1
JOIN dev1_raw.table2 t2
  ON t1.player_id = t2.player_id
GROUP BY 1
```

The data flow is almost the same, just the destination changed:
```mermaid
flowchart LR
  subgraph raw
  raw.table1
  raw.table2
  end
  subgraph dev1_raw
  dev1_raw.table1
  dev1_raw.table2
  end
  subgraph dev1_mart
  dev1_mart.table1
  end
  dev1_raw.table1 --> dev1_mart.table1
  dev1_raw.table2 --> dev1_mart.table1
```

> [!IMPORTANT]
> Bruin rewrote the references to both `raw.table1` and `raw.table2` to `dev1_raw.table1` and `dev1_raw.table2`, respectively. In this scenario, the tables from the original `raw` schema are not used at all.


---


This approach has a few advantages:
- It allows creating development tables without having to replicate source tables in different schemas.
- It allows partially overriding some tables in a developer environment without having to modify queries.
- In cases where the organization wants to manage the developer environments as schemas due to governance reasons, this approach allows to do so without having to modify the code.




### Features

# Features

Bruin is feature-packed and built to cover the majority needs of a data team, while staying extensible.

## Data ingestion

Bruin has built-in data ingestion capabilities, utilizing [ingestr](https://github.com/bruin-data/ingestr) internally. The basic idea is simple:
- you have data sources
- each source may have one or more tables/streams
- you want to load these to a destination data platform

Using Bruin, you can load data from any source into your data platforms as a regular asset.

```yaml
name: shopify_raw.customers
type: ingestr

columns:
  - name: id
    type: integer
    description: "Customer unique identifier"
    primary_key: true
    checks:
        - name: not_null
        - name: positive

parameters:
  source_connection: shopify-default
  source_table: customers
  destination: bigquery
  loader_file_format: jsonl
```

## Data transformation
Bruin supports SQL & Python data transformations natively.

Naturally, after you ingest the data into your data warehouse/lake, you'll want to transform this data. This transformation can be a simple SQL query, or a more complicated logic written in Python. Bruin supports both SQL & Python natively across [many data platforms](/#supported-platforms).

```bruin-sql
/* @bruin
name: chess_playground.player_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.games
   - chess_playground.profiles

columns:
  - name: total_games
    type: integer
    description: "the games"
    checks:
      - name: positive
@bruin */

WITH game_results AS (
    SELECT
        CASE
            WHEN g.white__result = 'win' THEN g.white__aid
            WHEN g.black__result = 'win' THEN g.black__aid
            ELSE NULL
            END AS winner_aid,
        g.white__aid AS white_aid,
        g.black__aid AS black_aid
    FROM chess_playground.games g
)

SELECT
    p.username,
    p.aid,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS white_wins,
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS black_wins,
    COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END) AS white_games,
    COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END) AS black_games,
FROM chess_playground.profiles p
LEFT JOIN game_results g
       ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
ORDER BY total_games DESC
```

### Running Python
Bruin takes the Python data development experience to the next level:
- Bruin runs assets in isolated environments: mix and match Python versions & dependencies
- It installs & manages Python versions automatically, so you don't have to have anything installed
- You can return dataframes and it uploads them to your destination
- You can run quality checks on it just as a regular asset

Bruin uses the amazing [`uv`](https://astral.sh/uv) under the hood to abstract away all the complexity.  

```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.13
connection: bigquery

materialization:
  type: table
  strategy: merge

columns:
  - name: col1
    type: integer
    checks:
      - name: unique
      - name: not_null
@bruin"""

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

## Data quality checks

Bruin supports data quality checks by default, and it has a handful of [built-in data quality checks](../quality/available_checks.md). You can also write your own custom quality checks using SQL if you'd like.

## Jinja templating

Bruin supports [Jinja templates](../assets/templating/templating.md) out of the box to reduce repetition. You can simply write any templating logic for your SQL assets and let Bruin take care of it.

```sql
{% set days = [1, 3, 7, 15, 30, 90, 120] %}

SELECT
    conversion_date,
    cohort_id,
    {% for day_n in days %}
    SUM(IFF(days_since_install < {{ day_n }}, revenue, 0)) AS revenue_{{ day_n }}_days
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM user_cohorts
GROUP BY 1,2
```

This will render into the following SQL query:

```sql
SELECT
    conversion_date,
    cohort_id,
    SUM(IFF(days_since_install < 1, revenue, 0)) AS revenue_1_days,
    SUM(IFF(days_since_install < 3, revenue, 0)) AS revenue_3_days,
    SUM(IFF(days_since_install < 7, revenue, 0)) AS revenue_7_days,
    SUM(IFF(days_since_install < 15, revenue, 0)) AS revenue_15_days,
    SUM(IFF(days_since_install < 30, revenue, 0)) AS revenue_30_days,
    SUM(IFF(days_since_install < 90, revenue, 0)) AS revenue_90_days,
    SUM(IFF(days_since_install < 120, revenue, 0)) AS revenue_120_days
FROM user_cohorts
GROUP BY 1,2
```

## Secrets management
Bruin allows you to define all of your credentials in a gitignored file called `.bruin.yml`, and it takes care of injecting secrets into your assets as environment variables automatically. You can define multiple environments, run the same asset against your staging or prod environments.

```bruin-python
"""@bruin
name: tier1.my_custom_api
secrets:
    - name: MY_SNOWFLAKE_CONN
@bruin"""

import os
import json

snowflake_creds = json.loads(os.environ["MY_SNOWFLAKE_CONN"])

# do whatever you want from here on
```

## VS Code extension

Bruin has an [open-source VS Code extension](../vscode-extension/overview.md) that allows you to visually work on your data pipelines. It has a local data catalog, query rendering, lineage, running & backfilling pipelines and more.

![Bruin VS Code extension overview](/vscode-extension/render-asset.gif)

## Glossaries

Bruin focuses on enabling independent teams designing independent data products. These products ought to be built and developed independently, while all working towards a cohesive data strategy. Different teams have the same name for different concepts, and aligning on these concepts are a crucial part of building successful data products.

In order to align on different teams on building on a shared language, Bruin has a feature called "[glossary](../getting-started/glossary.md)".

Glossaries allow:
- defining a shared language across teams/projects/pipelines
- reducing repetition for documentation and metadata

```yaml
entities:  
  Customer:
    description: Customer is an individual/business that has registered on our platform.
    attributes:
      ID:
        type: integer
        description: The unique identifier of the customer in our systems.
      Email:
        type: string
        description: the e-mail address the customer used while registering on our website.
      Language:
        type: string
        description: the language the customer picked during registration.
```


### Glossary

# Glossary

Bruin focuses on enabling independent teams designing independent data products. These products ought to be built and developed independently, while all working towards a cohesive data strategy. 

One of the most important aspects of this aligned approach to building data products is agreeing on the language. For instance, if you go to an e-commerce company and ask different individuals in different teams "what is a customer for you?", you will get different answers:
- a CRM manager might say: "a customer is someone who signed up to our email list.
- a backend engineer could say: "a customer is a row in the `customers` table in the backend.
- a marketing manager could say: "a customer is everyone that successfully finished signing up to our platform"
- a product manager could say: "a customer is someone who purchased at least one item through us"

The list can go on further, but you get the idea: different teams have the same name for different concepts, and aligning on these concepts are a crucial part of building successful data products.

In order to align on different teams on building on a shared language, Bruin has a feature called "glossary".

> [!INFO]
> Glossary is a beta feature, there may be unexpected behavior or mistakes while utilizing them in assets.

## Entities & Attributes

Glossaries in Bruin support two primary concepts at the time of writing:
- Entity: a high-level business entity that is not necessarily tied to a data asset, e.g. `Customer` or `Order`
- Attribute: the logical attributes of an entity, e.g. `ID` for a `Customer`, or `Address` for an `Order`.
  - Attributes have names, types and descriptions.

An entity can have zero or more attributes, while an attribute must always be within an entity. 

> [!INFO]
> Glossaries are primarily utilized for entities in its first version. In the future they will be used to incorporate further business concepts.


## Defining a glossary

In order to define your glossary, you need to put a file called `glossary.yml` at the root of the repo:
- The file `glossary.yml` must be at the root of the repo.
- The file must be named `glossary.yml` or `glossary.yaml`, nothing else.

Below is an example `glossary.yml` file that defines 2 entities, a `Customer` entity and an `Address` entity:
```yaml
# The `entities` key is used to define entities within the glossary, which can then be referred by different assets.
entities:  
  Customer:
    description: Customer is an individual/business that has registered on our platform.
    attributes:
      ID:
        type: integer
        description: The unique identifier of the customer in our systems.
      Email:
        type: string
        description: the e-mail address the customer used while registering on our website.
      Language:
        type: string
        description: the language the customer picked during registration.
  
  # You can define multi-line descriptions, and give further details or references for others. 
  Address:
    description: |
      An address represent a physical, real-world location that is used across various entities, such as customer or order.
      
      These addresses can be anywhere in the world, there is no country/geography limitation. 
      The addresses are not validated beforehand, therefore the addresses are not guaranteed to be real.
    attributes:
      ID:
        type: integer
        description: The unique identifier of the address in our systems.
      Street:
        type: string
        description: The given street name for the address, depending on the country it may have a different structure.
      Country:
        type: string
        description: The country of the address, represents a real country.
      CountryCode:
        type: string
        description: The ISO country code for the given country.
```

The file structure is flexible enough to allow conceptual attributes to be defined here. You can define unlimited number of entities and attributes.

### Schema
The `glossary.yml` file has a rather simple schema:
- `entities`: must be key-value pairs of string - an Entity object
- `Entity` object:
  - `description`: string, supports markdown descriptions
  - `attributes`: a key-value map, where the key is the name of the attribute, the value is an object.
    - `type`: the data type of the attribute
    - `description`: the markdown description of the given column

Take a look at the example above and modify it as per your needs.

## Utilizing entities in assets via `extends`
One of the early uses of entities is addressing & documenting concepts that repeat in multiple places. For instance, let's say you have a "age" property that is used across 5 different assets, this means you would have to go and document each of these columns one by one inside the assets. Using "entities", you can instead refer them all to a single attribute using the `extends` keyword.

```yaml
name: raw.customers

columns:
  - name: customer_id
    extends: Customer.ID
```

Let's take a look at the `extends` key here:
- The format it follows is `<entity>.<attribute>`, e.g. `Customer.ID` refers to the `ID` attribute in the `Customer` entity.
- In this example, the column `id` extends the attribute `ID` of `Customer`, which means it will take the attribute definition as the default:
  - There's already a `name` defined, `id`, that takes priority.
  - There's no `description` for the column, and the attribute has that, therefore take the `description` from the `Customer.ID` attribute.
  - There's no `type` defined for the column, therefore take the `type` from the `Customer.ID` attribute too. 


In the end, the resulting asset will behave as if it is defined this way:
```yaml
name: raw.customers

columns:
  - name: customer_id
    description: The unique identifier of the customer in our systems. # this comes from the `ID` attribute of the `Customer` entity
    type: integer # this comes from the `ID` attribute of the `Customer` entity
```

Thanks to the entities, you don't have to repeat definitions across assets.

### Order of priority
Entities are used as defaults when there are no explicit definitions on an asset column. Entity attributes support the following fields:
- `name`: the name of the attribute
- `type`: the type of the attribute
- `description`: the human-readable description of the attribute, ideally in relation to the business

Bruin will take all of the fields from the attribute, and combine them with the asset column:
- if the column definition already has a value for a field, use that.
- if not, and the entity attribute has that, use that.
- if neither has the value for the field, leave it empty.

This means, the following asset definition will still produce a valid asset:
```yaml
name: raw.customers

columns:
  - extends: Customer.ID # use `ID` as the column name, `integer` as the `type`, and `description` from the attribute
  - extends: Customer.Email # use `Email` as the column name, `string` as the `type`, and `description` from the attribute
```

Bruin will parse the `extends` references, and merge them with the corresponding attribute definitions.


## Using extends to generate columns in assets

In addition to extending individual attributes, you can also use an extends directly to automatically include all of its attributes as columns in your asset. This is done using the `extends` key at the asset level:

```yaml
name: raw.customers
extends: 
  - Customer  # This will automatically include all Customer attributes as columns
```

This is equivalent to explicitly extending each attribute of the Customer entity:

```yaml
name: raw.customers
columns:
  - extends: Customer.ID
  - extends: Customer.Email 
  - extends: Customer.Language
```

The `extends` approach is particularly useful when:
- Your asset represents a complete entity from your glossary
- You want to ensure all attributes of an entity are included
- You want to reduce boilerplate in your asset definitions


> [!WARNING]
> Explicit definition of a column will always take priority over entity attributes. Attributes are there to provide defaults, not to override explicit definitions on an asset level. Asset has higher priority than the glossary.





### Policies

# Policies

Bruin supports **policies** to verify that data transformation jobs follow best practices and organization-wide conventions. In addition to built-in lint rules, Bruin also allows users to define **custom lint rules** using a `policy.yml` file.

This document explains how to define, configure, and use custom linting policies.

> [!NOTE]
> For the purpose of this document, a `resource` means either an `asset` or a `pipeline`.
## Quick Start

1. Create a `policy.yml` file in your project root.
2. Define custom rules under `custom_rules` (optional if only using built-in rules).
3. Group rules into `rulesets`, specifying which resource they should apply to using selectors.

Example:

```yaml
rulesets:
  - name: ruleset-1
    selector:
      - path: .*/foo/.*
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
      - asset-has-description
```

🚀 That's it! Bruin will now lint your assets according to these policies.

To verify that your assets satisfy your policies, you can run:

```sh
$ bruin validate /path/to/pipelines
```

> [!tip]
> `bruin run` normally runs lint before pipeline execution. So you can rest assured that any non-compliant resources will get stopped in it's tracks.

## Rulesets

A **ruleset** groups one or more rules together and specifies which resources they apply to, based on selectors.

Each ruleset must include:
- **name**: A unique name for the ruleset.
- **selector** (optional): One or more predicates to select the applicable resources.
- **rules**: List of rule names (built-in or custom) to apply.

If a **selector** is not specified, the ruleset applies to **all resources**.

>[!NOTE]
> Names be must alphanumeric or use dashes (`-`). This applies to both `rulesets` and `rules`.


### Selector Predicates

Selectors determine which resources a ruleset should apply to. Supported predicates are:

| Predicate | Target | Description |
| :--- | :--- | :--- |
| `path` | `asset`, `pipeline` | path of the asset/pipeline |
| `pipeline` | `asset`, `pipeline` | name of the pipeline |
| `asset` | `asset` | name of the asset |
| `tag` | `asset` | asset tags |

Each predicate is a regex string.

::: info
If multiple selectors are specified within a ruleset, **all selectors must match** for the ruleset to apply
:::

If no selectors are defined for a ruleset, **the ruleset applies to all resources**. Some selectors only work with certain
rule targets. For instance `tag` selector only works for rules that [target](#targets) assets. Pipeline level rules will just ignore
this selector. 

> [!TIP]
> If your ruleset only contains asset selectors, but uses `pipeline` rules, then those pipeline rules will apply to all pipelines. Make sure to define a `pipeline` or `path` selector if you don't intend for that to happen.

### Example

```yaml
rulesets:
  - name: production
    selector:
      - path: .*/prod/.*
      - tag: critical
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
```

In this example:
- `production` applies **only** to resources that match both:
  - path regex `.*/prod/.*`
  - and have a tag matching `critical`.

## Custom Rules

Custom lint rules are defined inside the `custom_rules` section of `policy.yml`.

Each rule must include:
- **name**: A unique name for the rule. 
- **description**: A human-readable description of the rule.
- **criteria**: An [expr](https://expr-lang.org/) boolean expression. If the expression evalutes to `true` then the resource passes validation.

### Example

```yaml
custom_rules:
  - name: asset-has-owner
    description: every asset should have an owner
    criteria: asset.Owner != ""
```

### Targets

Custom rules can have an optional `target` attribute that defines what resource the rule acts on. Valid values are:
- `asset` (default)
- `pipeline`

**Example**
```yaml
custom_rules:

  - name: pipline-must-have-prefix-acme
    description: Pipeline names must start with the prefix 'acme'
    criteria: pipeline.Name startsWith 'acme'
    target: pipeline

  - name: asset-name-must-be-layer-dot-schema-dot-table
    description: Asset names must be of the form {layer}.{schema}.{table}
    criteria: len(split(asset.Name, '.')) == 3
    target: asset # optional

ruleset:
  - name: std
    rules:
      - pipeline-must-have-prefix-acme
      - asset-name-must-be-layer-dot-schema-dot-table
```

### Variables

`criteria` has the following variables available for use in your expressions:

| Name | Target | 
| ---  | --- | 
| [asset](https://github.com/bruin-data/bruin/blob/f9c7d0083d2f53538102e77126e55f9dfc8840a5/pkg/pipeline/pipeline.go#L622-L645) | `asset` | 
| [pipeline](https://github.com/bruin-data/bruin/blob/f9c7d0083d2f53538102e77126e55f9dfc8840a5/pkg/pipeline/pipeline.go#L1106-L1121) | `asset`, `pipeline` |

::: warning
The variables exposed here are direct Go structs, therefore it is recommended to check the latest version of these given structs. 

In the future we will create dedicated schemas for custom rules with standards around them.
:::

## Built-in Rules

Bruin provides a set of built-in lint rules that are ready to use without requiring a definition.

<table>
  <thead>
    <tr>
      <th width="45%">Rule</th>
      <th>Target</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>asset-name-is-lowercase</code></td>
      <td><code>asset</code></td>
      <td>Asset names must be in lowercase.</td>
    </tr>
    <tr>
      <td><code>asset-name-is-schema-dot-table</code></td>
      <td><code>asset</code></td>
      <td>Asset names must follow the format <code>schema.table</code>.</td>
    </tr>
    <tr>
      <td><code>asset-has-description</code></td>
      <td><code>asset</code></td>
      <td>Assets must have a description.</td>
    </tr>
    <tr>
      <td><code>asset-has-owner</code></td>
      <td><code>asset</code></td>
      <td>Assets must have an owner assigned.</td>
    </tr>
    <tr>
      <td><code>asset-has-columns</code></td>
      <td><code>asset</code></td>
      <td>Assets must define their columns.</td>
    </tr>
    <tr>
      <td><code>asset-has-primary-key</code></td>
      <td><code>asset</code></td>
      <td>Assets must define at least one column as a primary key.</td>
    </tr>
    <tr>
      <td><code>asset-has-checks</code></td>
      <td><code>asset</code></td>
      <td>Asset must have at least one check (<code>column</code> or <code>custom_checks</code>).</td>
    </tr>
    <tr>
      <td><code>asset-has-tags</code></td>
      <td><code>asset</code></td>
      <td>Asset must have at least one tag.</td>
    </tr>
    <tr>
      <td><code>column-has-description</code></td>
      <td><code>asset</code></td>
      <td>All columns declared by Asset must have description.</td>
    </tr>
    <tr>
      <td><code>column-name-is-snake-case</code></td>
      <td><code>asset</code></td>
      <td>Column names must be in <code>snake_case</code>.</td>
    </tr>
    <tr>
      <td><code>column-name-is-camel-case</code></td>
      <td><code>asset</code></td>
      <td>Column names must be in <code>camelCase</code>.</td>
    </tr>
    <tr>
      <td><code>column-type-is-valid-for-platform</code></td>
      <td><code>asset</code></td>
      <td>Ensure that column types declared by asset are valid types in the relevant platform (BigQuery and Snowflake only).</td>
    </tr>
    <tr>
      <td><code>description-must-not-be-placeholder</code></td>
      <td><code>asset</code></td>
      <td><code>asset</code> and <code>column</code> descriptions must not contain placeholder strings</td>
    </tr>
    <tr>
      <td><code>asset-has-no-cross-pipeline-dependencies</code></td>
      <td><code>asset</code></td>
      <td>Assets must not depend on assets in other pipelines.</td>
    </tr>
    <tr>
      <td><code>pipeline-has-notifications</code></td>
      <td><code>pipeline</code></td>
      <td>Pipeline must declare at least one notification channel</td>
    </tr>
    <tr>
      <td><code>pipeline-has-retries</code></td>
      <td><code>pipeline</code></td>
      <td>Pipeline must have retries > 0</td>
    </tr>
    <tr>
      <td><code>pipeline-has-start-date</code></td>
      <td><code>pipeline</code></td>
      <td>Pipeline must have a `start_date`</td>
    </tr>
    <tr>
      <td><code>pipeline-has-metadata-push</code></td>
      <td><code>pipeline</code></td>
      <td>Pipeline must push it's metadata</td>
    </tr>
  </tbody>
</table>


You can directly reference these rules in `rulesets[*].rules`.

## Full Example

```yaml
custom_rules:
  - name: asset-has-owner
    description: every asset should have an owner
    criteria: asset.Owner != ""

rulesets:
  - name: production
    selector:
      - path: .*/production/.*
      - tag: critical
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
      - asset-has-description
  - name: staging
    selector:
      - asset: stage.*
      - pipeline: staging
    rules:
      - asset-name-is-lowercase
```


## Further Reading

- [Expr Language Documentation](https://expr-lang.org/)



### Telemetry

# Telemetry
Bruin uses a very basic form of **anonymous telemetry** to be able to keep track of the usage on a high-level.
- It uses anonymous machine IDs that are hashed to keep track of the number of unique users.
- It sends the following information:
    - Bruin version
    - machine ID (Anomymous)
    - OS info: OS, architecture
    - command executed
    - success/failure
    - Stats on types of assets

The information collected here is used to understand the usage of Bruin and to improve the product. We use [Rudderstack](https://www.rudderstack.com/) to collect the events and we do not store any PII.

The questions we answer with this information are:
- How many unique users are using Bruin?
- How many times is Bruin being used and in what way?

## Disabling Telemetry
If you'd like to disable telemetry, simply set the `TELEMETRY_OPTOUT` environment variable to `true`.


### Templates

# Templates 

We simplify the creation of pipelines through the use of **templates**. Templates provide a starting point with predefined structures, configurations, and connections to help you quickly set up and standardize your pipelines.

## What are templates?

Templates in Bruin are blueprints for initializing new pipelines. They:
- define the structure and essential files for a pipeline.
- contain boilerplate code and configurations tailored to specific use cases.
- allow users to quickly set up pipelines without starting from scratch.

By using templates, teams can focus on their pipeline logic rather than worrying about setup and configuration.


## How to use templates ?

Using templates in Bruin is straightforward with the `bruin init` command. It offers both an interactive wizard and direct command-line options.

### Running the wizard

To initialize a project using the wizard, simply run:
```bash
bruin init 
```
As shown below, this will launch a terminal-based wizard:

<img alt="Bruin - init" src="/init-wizard.gif" style="margin: 10px;" />

You can navigate through the available templates using the up and down arrow keys. Once you've selected your desired template, press Enter to confirm. 
The wizard will automatically create the folder and set up the project for you.
### Manual template selection
If you'd prefer to specify your choices directly, you can use:
```bash
bruin init [template-name] [folder-name]
```
Arguments:

- **template-name** (optional):  
  The name of the template to use. If omitted, the default template is used.

- **folder-name** (optional):  
  The name of the folder where the project will be created. If omitted, the folder name will be:  
    - `bruin-pipeline` for the default template.  
    - The template name for custom templates.

### Available templates

To see a list of available templates, run:

```bash
bruin init --help
```
The output will display a list of available templates.

---

## Example template: Notion-to-BigQuery pipeline

The **Notion-to-BigQuery Pipeline** template is an example of a Bruin pipeline designed to copy data from Notion to BigQuery. This template demonstrates how to structure a simple pipeline with Bruin and provides prebuilt configurations for both Notion and BigQuery assets.

### Template overview

This template includes:

- **Preconfigured Assets**:
  - `raw.notion`: An ingestr asset that extracts data from a Notion table and loads it into BigQuery.
  - `myschema.example`: A SQL asset that creates a table in BigQuery.

- **Project Structure**:
  A structured layout with folders and files to organize pipeline assets, configurations, and documentation.

### Folder structure

```plaintext
notion/
├── assets/
│     ├── example.sql             # Example SQL asset for BigQuery table creation
│     └── notion.asset.yml        # Configuration for the Notion asset
├── pipeline.yml                  # Main pipeline configuration
└── README.md                     # Documentation for the pipeline
```

### How to use this template?

**1. Initialize the project**:  
   Use the template to create a new pipeline project:
```bash
bruin init notion
``` 
**2. Configure connections**:

Update the `.bruin.yml` file with your Notion API key and Google Cloud credentials.

**3. Run the pipeline**:

Use the `bruin` CLI to execute the pipeline, moving data from Notion to BigQuery based on the preconfigured assets.

---

This example template provides a starting point for building similar data pipelines with Notion and BigQuery. Modify the assets or add custom scripts to expand its functionality as needed.



## Getting-Started > Introduction


### Installation

# Installation
Bruin has two high-level components that makes it work the best:
- a command-line interface (CLI) application: [Bruin CLI](https://github.com/bruin-data/bruin)
- a Visual Studio Code extension: [VS Code Extension](https://marketplace.visualstudio.com/items?itemName=bruin.bruin)

We recommend installing both of these components to make the most of the Bruin experience.

## Prerequisites
Bruin requires a few components to be installed:
- [Git](https://git-scm.com/downloads)
- [Visual Studio Code](https://code.visualstudio.com/download): optional, but highly recommended

In addition, there are some optional components:
- [DuckDB](https://duckdb.org/docs/installation/): optional, useful for the tutorials in this documentation


## Bruin CLI

### MacOS
If you are on macOS, you can use `brew` to install Bruin:

```shell
brew install bruin-data/tap/bruin
```

### Windows, Linux and MacOS

If you are on macOS, linux or windows, you can use `curl` to install Bruin:

```shell
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```

Or you can also use `wget` to install Bruin:

```shell
wget -qO- https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```

> [!IMPORTANT]
> If you are on Windows, make sure to run the command in the [Git Bash](https://git-scm.com/downloads/win) or [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) terminal.


## Bruin VSCode Extension

1. **Open Visual Studio Code**: Launch VS Code on your computer.
2. **Go to Extensions**: Click the Extensions icon in the Activity Bar.
3. **Search for "Bruin"**: type "Bruin" in the Extensions search bar.
4. **Install the Extension**: Click the Install button next to the Bruin extension in the results. You can also check the "Auto Update" option to ensure you don't miss out on new features and fixes.

   ![Bruin Extension](../../public/vscode-extension/bruin-extension.png)

5. **Check Bruin CLI Installation**:
    - When you click the **Bruin Launch** button, the extension checks if Bruin CLI is installed.
   - If the CLI is **not installed**, a message will appear in the **Settings** tab with a button to install it.
    
![Install Bruin CLI](../../public/vscode-extension/install-cli.png)

That's it, you are ready to roll with Bruin.


## Troubleshooting

### 'Permission Denied' error during the CLI installation

**Issue**  
When installing the Bruin CLI, you may encounter a `'Permission Denied'` error. This typically happens if the user doesn't have permission to write the binary to the `~/.local/bin` directory.

**Solution**  
To resolve this, ensure that you have the necessary write permissions for the `~/.local/bin` directory. You can do this by running the following command with sudo:

```shell
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sudo sh
```


### Quickstart

# Quickstart
Make sure you have [installed](./installation.md) Bruin CLI already in your device.

> [!INFO]
> The steps here assume you have DuckDB installed. You can replace to any reference to `duckdb.sql` with your data warehouse if you'd like to run the examples elsewhere.

## Create your first pipeline

Bruin includes a handy command called `init`, you can simply run that command to create a new Bruin project.
      
```bash
bruin init default my-pipeline   
```

This command will:
- Create a project named `my-pipeline`.
- Generate a folder called `my-pipeline` containing the following:
    - An `assets` folder
    - `.bruin.yml` file (where you will add connection and credentials )
    - `pipeline.yml` file to manage your pipeline.


Once you have the project structure, you can run the whole pipeline:
```
bruin run
```

## Adding a new asset

In a nutshell, an asset is anything that generates value with data. In practice, an asset can be a table in your data warehouse, a Python script, or a file in S3. Bruin represents assets in code, put under the `assets` folder.

In order to create a new asset, you can simply create a new file under the `assets` folder. Let's start with an ingestr asset.

### Creating a `ingestr` asset
Let's start by ingesting some data from an external source.

ingestr is an [open-source CLI tool](https://github.com/bruin-data/ingestr) that allows ingesting data from any source into any destination.

You can create ingestr assets with a file `assets/players.asset.yml`

```yaml
name: dataset.players
type: ingestr

parameters:
  destination: duckdb
  source_connection: chess-default
  source_table: profiles

```

The configuration in the YAML file has a few bits:
- `name`: the name of the asset, also the table that will store the ingested data
- `connection`: the destination that will be used to load the data
- `parameters`: the configuration for ingestr
  - `source_connection`: the connection that will be used for the source data
  - `source_table`: the table that will be ingested

That's it, this asset will load data from the `chess` source and load it into your DuckDB database.

### Setting up your `.bruin.yml` file
To ensure the asset works correctly, configure your environments and connections by editing your ` .bruin.yml`  file. This file specifies environments and the connections your pipeline will use.

Modify the `.bruin.yml` file as follows:
```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

```
You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/players.asset.yml
```

### Creating a SQL asset

Let's create a new SQL asset with a file `assets/player_stats.sql`:

```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

This asset have a few lines of configuration at the top:
- `name`: the name of the asset, needs to be unique within a pipeline
- `type`: `duckdb.sql` means DuckDB SQL, Bruin supports many other types of assets.
- `materialization`: take the query result and materialize it as a table

Bruin will take the result of the given query, and will create a `dataset.player_stats` table on DuckDB with it. You can also use `view`
materialization type instead of `table` to create a view instead.

> [!INFO]
> Bruin supports many asset types, including BigQuery, Snowflake, Python, Redshift, Databricks, and more.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/player_stats.sql
```

### Creating a Python asset
Similar to SQL, Bruin supports running Python natively as well.

You can create a Python asset with a file `assets/my_python_asset.py`:

```bruin-python
"""@bruin
name: my_python_asset
@bruin"""

print('hello world')
```

- The sections between `"""@bruin` and `@bruin"""` are comment markers, they are the configuration for the asset.
- The rest of the body is a regular Python script.

You can run this asset either via the Bruin VS Code extension, or in the terminal:
```bash
bruin run assets/my_python_asset.sql
```

At this point, you can also run the whole pipeline:
```bash
bruin run
```

## Data Quality Checks
Bruin supports data quality checks natively, as part of the asset definition. It includes a handful of data quality checks, and it also supports custom checks.

Let's add a few data quality checks to our table:
```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

# you can define column metadata and quality checks
columns: // [!code ++]
  - name: name // [!code ++]
    type: string // [!code ++]
    description: this column contains the player names // [!code ++]
    checks: // [!code ++]
      - name: not_null // [!code ++]
      - name: unique // [!code ++]
  - name: player_count // [!code ++]
    type: int // [!code ++]
    description: the number of players with the given name // [!code ++]
    checks: // [!code ++]
      - name: not_null // [!code ++]
      - name: positive // [!code ++]

# you can also define custom checks 
custom_checks:
  - name: row count is greater than zero // [!code ++]  
    description: this check ensures that the table is not empty // [!code ++]  
    query: SELECT count(*) > 1 FROM dataset.player_count // [!code ++]  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

We have added a new `columns` section in the asset, it contains a list of all the columns and the checks applied to them.

Under the `checks` section, each column defines some quality checks:
- `name` column is marked to be not null and unique
- `player_count` column is marked to be not null and consisting of positive numbers

Under the `custom_checks` section, we have added a new custom quality check that checks if the table is empty or not.

You can refresh the asset & run the quality checks via simply running the asset:
```bash
bruin run assets/player_stats.sql
```

If you'd like to run only the checks, you can run:
```bash
bruin run --only checks assets/player_stats.sql
```

## Next steps
You have created your first pipeline, ingested data from a new source, added a bunch of assets, and ran quality checks on that, you are ready to dig deeper. Jump into the [Concepts](../concepts.md) to learn more about the underlying concepts Bruin use for your data pipelines.


## Getting-Started > Templates-Docs


### Athena Readme

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

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin --start-date 2024-01-01 run ./athena/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!



### Chess Readme

# Bruin - Chess Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three sample assets already:
- `chess_games.asset.yml`: Transfers chess game data from source database to DuckDB.
- `chess_profiles.asset.yml`: Transfers chess player profiles data from source to DuckDB.
- `player_summary.sql`:Creates a summary table of chess player stats, including games, wins, and win rates as white/black.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb-default"
                  path: "/path/to/your/database.db"

            chess:
                - name: "chess-connection"
                  players:
                      - "erik"
                      - "vadimer2"
```

You can simply switch the environment using the `--environment` flag, e.g.:


## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run ./chess/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Clickhouse Readme

# Bruin - Clickhouse Template

This pipeline is a simple example of a Bruin pipeline for Clickhouse, 
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints 
like `not_null`, `unique`, and `primary_key`.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).
You will need a clickhouse server. You can run one locally with docker running the following:

```bash
docker run -e CLICKHOUSE_DB=default -e CLICKHOUSE_USER=username -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 -e CLICKHOUSE_PASSWORD=password -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

Here's a sample `.bruin.yml` file that would work with the docker container above:


```yaml
default_environment: default
environments:
  default:
    connections:
      clickhouse:
        - name: clickhouse-default
          username: username
          password: password
          host: 127.0.0.1
          port: 19000
          database: my_database
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell

bruin run ./clickhouse/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Duckdb Readme

# Bruin - DuckDB Template

This pipeline is a simple example of a Bruin pipeline for DuckDB, 
featuring `example.sql`—a SQL asset that creates a table with sample data and enforces schema constraints 
like `not_null`, `unique`, and `primary_key`.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).

Here's a sample `.bruin.yml` file:


```yaml
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb_default"
          path: "/path/to/your/database.db"
      
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell

bruin run ./duckdb/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Firebase Readme

# Bruin - Firebase to GCP Template

This pipeline is a simple example of a Bruin pipeline for Firebase. 

The pipeline includes several sample assets:

- `events/events.asset.yaml`: Monitors for new events data in BigQuery to trigger downstream tasks when new data is detected.
- `events/events.sql`: Defines a BigQuery view for formatted Firebase Analytics event data to support ad-hoc analysis.
- `fn/date_in_range.sql`: A function asset that checks if a date is within a specified range.
- `fn/get_params_to_json.sql`: A function asset that converts parameter data to JSON format.
- `fn/get_param_bool.sql`, `fn/get_param_double.sql`, `fn/get_param_int.sql`, `fn/get_param_str.sql`: SQL assets that retrieve specific types of parameters (boolean, double, integer, string).
- `fn/parse_version.sql`: A function asset for parsing version information from a string.
- `fn/user_properties_to_json.sql`: A function asset that converts user properties into JSON format, excluding certain fields.
- `user_model/cohorts.sql`: A SQL asset that defines cohort-based aggregations for user data.
- `user_model/users.sql`: A SQL asset that defines the users table structure.
- `user_model/users_daily.sql`: A SQL asset that manages daily updates for user data.

For a more detailed description of each asset, refer to the **description** section within each sql asset. Each file provides specific details and instructions relevant to its functionality.

## Setup
The pipeline includes a `.bruin.yml` file where you need to configure your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` configuration:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-id"
 ```         
          
##  Important Notes
Review TODOs: The SQL files events/events.sql, user_model/users_daily.sql, and events_json.sql contain TODO comments. These indicate sections where you should make adjustments based on your data and project requirements.


## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run ./firebase/pipeline.yml
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Frankfurter Readme

# Bruin - Frankfurter Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.
This video shows the template in action:

The pipeline includes the following example assets:
- `frankfurter_raw/rates.asset.yml`: An ingestr asset which copies raw exchange rate data to DuckDB.
- `frankfurter_raw/currencies.asset.yml`: An ingestr asset which copies the list of available currencies to DuckDB.
- `frankfurter/currency_names.sql`: An SQL-asset which creates a table from the list of available currencies.
- `frankfurter/daily_rates.sql`: An SQL-asset which cleans the raw currency exchange data by filling in missing rates for weekends and public holidays.
- `fx_insights/currency_performance.sql`: An SQL-asset which shows the latest exchange rates as well as each currency's performance over the past 30 days from `frankfurter/daily_rates.sql` and matches each currency code to the currencies full name from `frankfurter/currency_names.sql`.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      frankfurter:
        - name: "frankfurter-default"
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
```

You can simply switch the environment using the `--environment` flag, e.g.:


## Running the pipeline

bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run ./frankfurter/pipeline.yml
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it! Good luck!


### Gorgias Readme

# Bruin - Gorgias to Bigquery Template

This pipeline is a simple example of a Bruin pipeline that copies data from Gorgias to BigQuery. It copies data from the following resources:
- `customers`
- `tickets`
- `ticket_messages`
- `satisfaction_surveys`

> [!CAUTION]
> Gorgias has very strict rate limits as of the time of building this pipeline, 2 req/s. This means that we cannot extract data from Gorgias in parallel, therefore all of these steps here are built to run sequentially. This is not a problem for small datasets, but it can be a bottleneck for larger datasets.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/gorgias.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      gorgias:
        - name: "gorgias"
          domain: "my-shop"
          email: "my-email@myshop.com"
          api_key: "XXXXXXXX"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
# this will get all the satisfaction surveys starting from 2024-01-01
bruin --start-date 2024-01-01 run assets/satisfaction_surveys.asset.yml
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Gsheet Bigquery Readme

# Bruin - GSheet to BigQuery Template

This pipeline is a simple example of a Bruin pipeline that copies data from GSheet to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `gsheet_raw.customers`: A simple ingestr asset that copies a table from GSheet to BigQuery

## Setup

Example Sheet: https://docs.google.com/spreadsheets/d/1p40qR9t6DM5a1IskTkqEX9eZYZmBeILzUX_AdMkg__A/edit?usp=sharing

The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/ingestion/google_sheets.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: "gcp-default"
                  service_account_file: "<Path to your Google Cloud service account JSON file>"
                  project_id: "bruin-common-health-check"
            google_sheets:
                - name: "gsheet-default"
                  credentials_path: "<Path to your Google Sheets credentials JSON file>"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
❯ bruin run ./templates/gsheet-bigquery/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can also run a single task:
```shell
❯ bruin run ./templates/gsheet-bigquery/                                                     (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Gsheet Duckdb Readme

# Bruin - GSheet to DuckDB Template

This pipeline is a simple example of a Bruin pipeline that copies data from GSheet to DuckDB. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `gsheet_raw.customers`: A simple ingestr asset that copies a table from GSheet to DuckDB

## Setup

Example Sheet: https://docs.google.com/spreadsheets/d/1p40qR9t6DM5a1IskTkqEX9eZYZmBeILzUX_AdMkg__A/edit?usp=sharing

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/ingestion/google_sheets.html).
Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb-default"
                  path: "<Path to your DuckDB database file>"
            google_sheets:
                - name: "gsheet-default"
                  credentials_path: "<Path to your Google Sheets credentials JSON file>"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:


```shell
❯ bruin run ./templates/gsheet-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can also run a single task:


```shell
❯ bruin run ./templates/gsheet-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Notion Readme

# Bruin - Notion to Bigquery Template

This pipeline is a simple example of a Bruin pipeline that copies data from Notion to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.notion`: A simple ingestr asset that takes copies a table from Notion to BigQuery
- `myschema.example`: A simple SQL asset that creates a table in BigQuery.
  - Feel free to change the type from `bq.sql` to anything.

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
environments:
  default:
    connections:
      google_cloud_platform:
        - name: "gcp"
          service_account_file: "/path/to/my/key.json"
          project_id: "my-project-dev"
      notion:
        - name: "my-notion-connection"
          api_key: "XXXXXXXX"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run assets/notion.asset.yml
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Python Readme

# Bruin - Python Template

This pipeline is a simple example of a Bruin pipeline. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes three Python assets that uses the same dependencies, but with 3 different versions of Python:
- 3.11
- 3.12
- 3.13

This pipeline is a good example of Bruin's Python execution abilities.

A couple of remarks:
- All of these assets are executed in isolated environments.
- All of the assets have separate `requirements.txt` files, which may contain different dependencies.

## Running the pipeline

Run the following command:
```shell
bruin run .
```

> [!NOTE]
> The first execution might take a bit longer since we'll need to install Python and the dependencies. The next executions should be super fast.

```shell
Starting the pipeline execution...

[2023-03-16T18:25:14Z] [worker-0] Running: dashboard.bruin-test
[2023-03-16T18:25:16Z] [worker-0] Completed: dashboard.bruin-test (1.681s)
[2023-03-16T18:25:16Z] [worker-4] Running: hello
[2023-03-16T18:25:16Z] [worker-4] [hello] >> Hello, world!
[2023-03-16T18:25:16Z] [worker-4] Completed: hello (116ms)

Executed 2 tasks in 1.798s
```

You can also run a single task:

```shell
bruin run assets/python311/asset.py                            
```

```shell
Starting the pipeline execution...

[2023-03-16T18:25:59Z] [worker-0] Running: hello
[2023-03-16T18:26:00Z] [worker-0] [hello] >> Hello, world!
[2023-03-16T18:26:00Z] [worker-0] Completed: hello (103ms)


Executed 1 tasks in 103ms
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


### Shopify Bigquery Readme

# Bruin - Shopify to BigQuery Template

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to BigQuery. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that takes copies a table from Notion to BigQuery

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: "gcp"
                  service_account_file: "<path to service account file>"
                  project_id: "bruin-common-health-check"

            shopify:
                - name: "my-shopify-connection"
                  api_key: "********"
                  url: "******.myshopify.com"
```

## Running the pipeline

Bruin CLI can run the whole pipeline or any task with the downstreams:

```shell
bruin run assets/shopify.asset.yml
```

```shell
❯ bruin run ./templates/shopify-bigquery/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...



Executed 1 tasks in 9.656s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
❯ bruin run ./templates/shopify-bigquery/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!

NOTE: Please find assets example for balance and transactions table in the [Shopify Cookbook](https://bruin-data.github.io/docs/cookbook/shopify-pipelines.html)

- Balance Table
```yaml
name: shopify_raw.balance
type: ingestr

description: This asset manages the ingestion of Shopify customer data into BigQuery. It captures comprehensive customer information including personal details, contact information, order history, marketing preferences, and address data. The asset includes data quality checks to ensure critical fields like customer ID and email are properly populated.

parameters:
  source_connection: shopify-default
  source_table: balance
  destination: duckdb

```

- Transactions Table
```yaml
name: shopify_raw.transactions
type: ingestr

description: This asset manages the ingestion of Shopify transaction data into BigQuery. It captures detailed payment transaction information including transaction ID, order ID, amount, currency, payment method, status, and processed date. This data is essential for financial reporting, reconciliation, and analyzing payment patterns across the Shopify store.

parameters:
  source_connection: shopify-default
  source_table: transactions
  destination: duckdb

```


### Shopify Duckdb Readme

# Bruin - Shopify to DuckDB Template

This pipeline is a simple example of a Bruin pipeline that copies data from Shopify to DuckDB. It demonstrates how to use the `bruin` CLI to build and run a pipeline.

The pipeline includes two sample assets already:
- `raw.shopify`: A simple ingestr asset that copies a table from Shopify to DuckDB

## Setup
The pipeline already includes an empty `.bruin.yml` file, fill it with your connections and environments. You can read more about connections [here](https://bruin-data.github.io/bruin/connections/overview.html).

Here's a sample `.bruin.yml` file:

```yaml
default_environment: default
environments:
    default:
        connections:
            duckdb:
                - name: "duckdb-default"
                  path: "<path to database>"

            shopify:
                - name: "my-shopify-connection"
                  api_key: "********"
                  url: "******.myshopify.com"
```
## Running the pipeline
Bruin CLI can run the whole pipeline or any task with the downstreams:
```shell
bruin run assets/shopify.asset.yml
```

```shell
❯ bruin run ./templates/shopify-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...

Executed 1 tasks in 9.656s
```

You can also run a single task:

```shell
bruin run assets/hello.py                            
```

```shell
❯ bruin run ./templates/shopify-duckdb/                                                       (bruin) 
Analyzed the pipeline 'bruin-init' with 1 assets.

Pipeline: bruin-init (.)
  No issues found

✓ Successfully validated 1 assets across 1 pipeline, all good.

Starting the pipeline execution...


Executed 1 tasks in 9.656s
```

You can optionally pass a `--downstream` flag to run the task with all of its downstreams.

That's it, good luck!


NOTE: Please find assets example for balance and transactions table in the [Shopify Cookbook](https://bruin-data.github.io/docs/cookbook/shopify-pipelines.html)

- Balance Table
```yaml
name: shopify_raw.balance
type: ingestr

description: This asset manages the ingestion of Shopify customer data into BigQuery. It captures comprehensive customer information including personal details, contact information, order history, marketing preferences, and address data. The asset includes data quality checks to ensure critical fields like customer ID and email are properly populated.

parameters:
  source_connection: shopify-default
  source_table: balance
  destination: duckdb

```

- Transactions Table
```yaml
name: shopify_raw.transactions
type: ingestr

description: This asset manages the ingestion of Shopify transaction data into BigQuery. It captures detailed payment transaction information including transaction ID, order ID, amount, currency, payment method, status, and processed date. This data is essential for financial reporting, reconciliation, and analyzing payment patterns across the Shopify store.

parameters:
  source_connection: shopify-default
  source_table: transactions
  destination: duckdb

```


## Getting-Started > Tutorials


### First Tutorial

# Getting Started with Your First Bruin Pipeline: Chess Data to DuckDB

Welcome! 👋 This guide is your starting point for building data pipelines with Bruin—beginning with an easy, hands-on project. You'll learn how to set up a pipeline that fetches chess data from a simple, no-credentials-needed API and stores it in DuckDB. The chess API provides a hassle-free way to work with real-world data, making it the perfect starting project for experimenting with Bruin. Think of this as your playground for exploring Bruin’s capabilities.

**By the end of this tutorial, you’ll:**
- Understand the basics of Bruin CLI 
- Build a pipeline that ingests data from the chess API
- Store and query data in DuckDB, ready for analysis or integration

Whether you're new to Bruin, pipelines, or even data engineering in general, this guide is designed to help you take your first steps confidently. You’ll finish with a working pipeline that you can use as a foundation for more advanced projects. Let's dive in and start building! 🚀

## Prerequisites
Before you start, make sure you have the Bruin CLI installed. [Get instructions](../introduction/installation.md).

## Step 1: Initialize Your Pipeline 

Run the `bruin init chess` command to set up your Bruin project using the `chess` template. This template brings in the necessary project structure and configuration files for a pipeline specific to chess data.

```bash 
bruin init chess
```
After running the command you will see the following folder appear on your project :

```plaintext
chess/
├─ assets/
│  ├─ chess_games.asset.yml
│  ├─ chess_profiles.asset.yml
│  ├─ player_summary.sql
│
├─ .bruin.yml
├─ pipeline.yml  
├─ .gitignore
└─ README.md
```

## Step 2: Edit Your `.bruin.yml` file
After initializing your project with `bruin init`, edit the `.bruin.yml` file to configure your environments and connections. This file specifies the default environment settings and connections your pipeline will use.

Add or modify the `.bruin.yml` file as follows:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: "duckdb-default"
          path: "duckdb.db"
      chess:
        - name: "chess-default"
          players:
            - "erik"
            - "vadimer2"

```
## Step 3: Take a look at your assets
Since we initialized our project with the `chess template`, the necessary assets are already included in the assets folder,
pre-configured for chess data ingestion. Open this file to take a look at your assets, you should see :

- **`chess_games.asset.yml`**: Ingests chess game data from the source connection into DuckDB.
- **`chess_profiles.asset.yml`**: Ingests player profile data from the source connection into DuckDB.
- **`player_summary.sql`**: A SQL asset that creates a summary table of player statistics, aggregating game results, wins, and win rates for each player based on the ingested data.
> [!INFO]
> **What is an Asset?**  
> An asset in Bruin is a configuration that details specific data processing tasks, such as ingestion, transformation. Learn more about [Bruin assets](../../assets/definition-schema.md).

## Step 4: Check Your `pipeline.yml` file
Just like in Step 3, the pipeline.yml file also comes pre-configured for our task, and it follows this structure:
### Example `pipeline.yml`:
```yaml
name: chess_duckdb
default_connections:
    duckdb: "duckdb_default"  
    chess: "chess_connection"
```
> [!INFO]
> **What is a Pipeline?**  
>A pipeline is a group of assets that are executed together in the right order.  Learn more about [Bruin Pipelines](../concepts.md#pipeline).
## Step 5: Run Your Pipeline and Ingest the Data
Now that your pipeline is set up, you're ready to run it and ingest the data into DuckDB. Use the following command to execute the pipeline:

```bash
bruin run ./chess/pipeline.yml
```
<img alt="Chess - Pipeline" src="/chesspipeline.gif" />

## Step 6: Query Your Data in DuckDB
Now that the data is in DuckDB, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:


```bash
bruin query --c duckdb-default  --q "SELECT * FROM chess_playground.player_summary LIMIT 10;"
```
After executing the query, you will see the following results displayed in your terminal in a clear and organized table format:
```plaintext
| USERNAME      | AID                                            | TOTAL_GAMES | WHITE_WINS | BLACK_WINS | WHITE_GAMES | BLACK_GAMES | WHITE_WIN_RATE | BLACK_WIN_RATE |
|---------------|------------------------------------------------|-------------|------------|------------|-------------|-------------|----------------|----------------|
| vadimer2        | https://api.chess.com/pub/player/vadimer2        | 59573       | 24243      | 22731      | 30029       | 29544       | 80.73          | 76.94          |
| erik | https://api.chess.com/pub/player/erik | 6298        | 2267       | 2027       | 3169        | 3129        | 71.54          | 64.78          |

```
🎉Congratulations!

You've successfully created and run your first Bruin pipeline! Your chess data is now ingested into DuckDB, ready for you to query and explore. This is just the beginning—feel free to extend this pipeline, add more data sources, or incorporate data transformations to suit your needs. For more advanced features and customization, check out the Bruin documentation.

Happy data engineering! 🚀



### Load Notion Postgres

# Loading Notion Data To PostgreSQL Database Using Bruin
Welcome! 👋 This tutorial is a simple, step-by-step guide to help you load data from Notion into a PostgreSQL database using Bruin. Notion is an incredibly versatile tool for organizing projects, taking notes, and managing databases. However, when you want to leverage this data for analytics, reporting, or integration with other systems, you often need to move it into a structured database like PostgreSQL. That’s where Bruin comes in. Bruin makes it easy to automate and manage data pipelines, letting you move data from Notion (and other sources) to destinations like PostgreSQL with minimal effort. If you’re just starting out, don’t worry—this guide is designed for beginners. You’ll learn the basics of setting up a Bruin project, creating a pipeline, and connecting Notion with PostgreSQL. By the end, you’ll have a working example that you can extend or customize for your own needs.

### Prerequisites
#### Bruin CLI

To install Bruin CLI depending upon your machine, follow the installation instructions [here](../introduction/installation.md)

#### Bruin VS Code Extension
To install the Bruin VS Code extension, follow the instructions [here](../../vscode-extension/overview.md)

<img width="685" alt="bruin_extension" src="./tutorial-media/bruin-extension.png">


### Getting Started
To create the basic structure of a Bruin project, you can simply run:

   ```
   bruin init {folder name} [template name]
   ```

If you don't define the folder name and template name, it will create `default` template with folder name `bruin-pipeline`. This command will:

    Create a project named bruin-pipeline 
    Generate a folder called bruin-pipeline containing the following:
        -  An assets folder (where you add asset file)
        - .bruin.yml file (where connection and credentials are added)
        -  pipeline.yml file to manage your pipeline

#### Adding a new asset

Adding a new [asset](../../assets/definition-schema.md) is as simple as creating a new file inside the assets folder. Let's create a new [ingestr asset](../../assets/ingestr.md) file `notion.asset.yml` inside the assets folder and add :

 ```yaml
name: public.notion
type: ingestr
connection: my-postgres
parameters:
  source_connection: my-notion
  source_table: 'database_id'
  destination: postgres
 ```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will always be of the `ingestr` type for Notion.
- `connection`: The name of the connection
- `source_connection`: The name of the Notion connection defined in .bruin.yml.
- `source_table`: The database id of Notion you want to ingest.
- `destination`: The name of the destination, you want to store. Here, we are using PostgreSQL.

#### Adding connection and credentials in .bruin.yml
Make sure, you have [Notion credentials](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion#setup-guide) and Postgres credentials.

Using Bruin vscode extension
- Add destination connection - PostgreSQL
- Add source connection - Notion

<video width="685" height="auto" controls poster="./tutorial-media/vscode-poster.png">
  <source src="./tutorial-media/vscode-video.mp4">
  Your browser does not support the video tag.
</video>


You can customize your pipeline by making changes in the pipeline.yml file. You can set the schedule to daily, weekly, or monthly.

#### Validate and Run your pipeline
- Bruin CLI can run the whole pipeline or any task with the downstreams.
  `Make sure, asset file is open`

<video width="685" height="auto" controls poster="./tutorial-media/run-pipeline-poster.png">
  <source src="./tutorial-media/run-pipeline-video.mp4" type="video/mp4">
  Your browser does not support the video tag.
</video>
<br>


#### This is what Notion data looks like at the destination:
<br>

<img alt="notion-dest" src="./tutorial-media/notion-table.png" />

🎉 Congratulations!

You've successfully created and run your first Bruin pipeline! Your Notion data is now ingested into PostgreSQL, ready for you to query and explore. This is just the beginning—feel free to extend this pipeline, add more data sources, or incorporate data transformations to suit your needs. For more advanced features and customization, check out the Bruin documentation.

Happy data engineering! 🚀



### Template Tutorial

# Templates

We simplify the creation of pipelines through the use of **templates**. Templates provide a starting point with predefined structures, configurations, and connections to help you quickly set up and standardize your pipelines.

## What are templates?

Templates in Bruin are blueprints for initializing new pipelines. They:
- define the structure and essential files for a pipeline.
- contain boilerplate code and configurations tailored to specific use cases.
- allow users to quickly set up pipelines without starting from scratch.

By using templates, teams can focus on their pipeline logic rather than worrying about setup and configuration.

Watch the video below to see how to use templates in action:

<iframe width="685" height="385"
src="https://www.youtube.com/embed/8RuVoSCNHQI"
frameborder="0"
allowfullscreen>
</iframe>


## Getting-Started > Tutorials > Tutorial-Media


## Assets


### Columns

# Columns

Bruin supports column definitions inside assets to make them a part of your data pipelines:

- you can document the existing columns in an asset and add further metadata, e.g. `primary_key`
- you can define column-level quality checks
- you can define whether or not a column should be updated as a result of a [
  `merge` materialization](./materialization.md#merge)

## Definition Schema

The top level `columns` key is where you can define your columns. This is a list that contains all the columns defined
with the asset, along with their quality checks and other metadata.

Here's an example column definition:

```yaml
columns:
  - name: col1
    type: integer
    description: "Just a number"
    primary_key: true
    checks:
      - name: unique
      - name: not_null
      - name: positive
  - name: col2
    type: string
    description: |
      some multi-line definition for this column
    update_on_merge: true
    checks:
      - name: not_null
      - name: accepted_values
        value: [ 'value1', 'value2' ]
```

Each column will have the following keys:

| key               | type    | req? | description                                                                     |
|-------------------|---------|------|---------------------------------------------------------------------------------|
| `name`            | String  | yes  | The name of the column                                                          |
| `type`            | String  | no   | The column type in the DB                                                       |
| `description`     | String  | no   | The description for the column                                                  |
| `primary_key`     | Bool    | no   | Whether the column is a primary key                                             |
| `update_on_merge` | Bool    | no   | Whether the column should be updated with [`merge`](./materialization.md#merge) |
| `checks`          | Check[] | no   | The quality checks defined for the column                                       |

### Quality Checks

The structure of the quality checks is rather simple:

| key        | type   | req? | description                                                       |
|------------|--------|------|-------------------------------------------------------------------|
| `name`     | String | yes  | The name of the quality check, see [Quality](../quality/overview) |
| `blocking` | Bool   | no   | Whether the check should block the downstreams, default `true`    |
| `value`    | Any    | no   | Check-specific expected value                                     |                                     
For more details on the quality checks, please refer to the  [Quality](../quality/overview) documentation.


### Definition Schema

# Asset Definition
Assets are defined in a YAML format in the same file as the asset code. 
This enables the metadata to be right next to the code, reducing the friction when things change and encapsulating the relevant details in a single file. 
The definition includes all the details around an asset from its name to the quality checks that will be executed.

Here's an example asset definition:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

owner: my-team@acme-corp.com

depends:
   - hello_python

materialization:
   type: table

tags:
   - dashboard
   - team:xyz
   
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
        - name: unique
        - name: not_null
        - name: positive
        - name: accepted_values
          value: [1, 2]

@bruin */

select 1 as one
union all
select 2 as one
```

::: info
Bruin has [an open-source Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=bruin.bruin) extension that does syntax-highlighting for the definition syntax and more.
:::

::: warning
Assets that are defined as YAML files have to have file names as `<name>.asset.yml` or `<name>.asset.yaml`. The regular `.yml` files are not considered as assets, since they might be configuration used within the repo.
:::

## `name`
The name of the asset, used for many things including dependencies, materialization and more. Corresponds to the `schema.table` convention.
Must consist of letters and dot `.` character.
- **Type:** `String`

## `type`
The type of the asset, determines how the execution will happen. Must be one of the types [here](https://github.com/bruin-data/bruin/blob/main/pkg/executor/defaults.go).
- **Type:** `String` 

## `owner`
The owner of the asset, has no functional implications on Bruin CLI as of today, allows documenting the ownership information. On [Bruin Cloud](https://getbruin.com), it is used to analyze ownership information, used in governance reports and ownership lineage.  
- **Type:** `String` 

## `tags`
As the name states, tags that are applied to the asset. These tags can then be used while running assets, e.g.:
```bash
bruin run --tags client1
```
- **Type:** `String[]` 

## `depends`
The list of assets this asset depends on. This list determines the execution order.
In other words, the asset will be executed only when all of the assets in the `depends` list have succeeded.
- **Type:** `String[]`

## `interval_modifiers`
Controls how the processing window is adjusted by shifting the start and end times. Requires the `--apply-interval-modifiers` flag when running the pipeline.

```yaml
interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour
```

Supported time units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days), `M` (months).
See [interval modifiers](./interval-modifiers) for more details.
- **Type:** `Object`

## `materialization`
This option determines how the asset will be materialized. Refer to the docs on [materialization](./materialization) for more details.

## `columns`

This is a list that contains all the columns defined with the asset, along with their quality checks and other metadata. Refer to the [columns](./columns.md) documentation for more details.

## `custom_checks`
This is a list of custom data quality checks that are applied to an asset. These checks allow you to define custom data quality checks in SQL, enabling you to encode any business logic into quality checks that might require more power.

```yaml
custom_checks:
  - name: Client X has 15 credits calculated for June 2024
    description: This client had a problem previously, therefore we want to ensure the numbers make sense, see the ticket ACME-1234 for more details. 
    value: 15
    query: |
      SELECT
        count(*)
      FROM `tier2.client_credits`
      where client="client_x" 
        and date_trunc(StartDateDt, month) = "2024-06-01"
        and credits_spent = 1
```


### Ingestr

# Ingestr Assets
[Ingestr](https://github.com/bruin-data/ingestr) is a CLI tool that allows you to easily move data between platforms. Bruin supports `ingestr` natively as an asset type.

Using Ingestr, you can move data from:
* your production databases like:
    * MSSQL
    * MySQL
    * Oracle
* your daily tools like:
    * Notion
    * Google Sheets
    * Airtable
* from other platforms such as:
    * Hubspot
    * Salesforce
    * Google Analytics
    * Facebook Ads
    * Google Ads

to your data warehouses:
* Google BigQuery 
* Snowflake 
* AWS Redshift 
* Azure Synapse 
* Postgres 

> [!INFO]
> You can read more about the capabilities of ingestr [in its documentation](https://bruin-data.github.io/ingestr/).



## Asset Structure
```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
parameters:
  source: string # optional, used when inferring the source from connection is not enough, e.g. GCP connection + GSheets source
  source_connection: string
  source_table: string
  destination: bigquery | snowflake | redshift | synapse
  
  # optional
  incremental_strategy: replace | append | merge | delete+insert
  incremental_key: string
  sql_backend: pyarrow | sqlalchemy
  loader_file_format: jsonl | csv | parquet
```

##  Examples
The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

### Copy a table from MySQL to BigQuery
```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: public.transactions
  destination: bigquery
```

### Copy a table from Microsoft SQL Server to Snowflake incrementally
This example shows how to use `updated_at` column to incrementally load the data from Microsoft SQL Server to Snowflake.
```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: dbo.transactions
  destination: snowflake
  incremental_strategy: append
  incremental_key: updated_at
```

### Copy data from Google Sheets to Snowflake
This example shows how to copy data from Google Sheets into your Snowflake database

```yaml
name: raw.manual_orders
type: ingestr
parameters:
  source: gsheets
  source_connection: gcp-default
  source_table: <mysheetid>.<sheetname>
  destination: snowflake
```



### Interval Modifiers

# Interval Modifiers: Adjusting Your Processing Window

Bruin supports adjusting the start and end of your data processing window using `interval_modifiers`. This is useful in situations where events might arrive late (e.g. offline mobile game events) or slightly ahead of schedule.

You can shift the start or end of the interval either forward or backward, depending on how you want to widen the processing window.

This works for both SQL and Python assets and is primarily designed for regularly scheduled pipeline runs.

## How It Works

Use the `interval_modifiers` block in your asset definition to control how much to shift the window and supports both lookback and lookahead:

```yaml
interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour
```

## Pipeline-Level Defaults

You can set default interval modifiers at the pipeline level that will apply to all assets unless explicitly overridden. Define these in your pipeline's `default` section:

```yaml
default:
  interval_modifiers:
    start: 2h    # Default 2-hour shift for all assets
    end: 2h      # Default 2-hour shift for all assets
```

Individual assets can override these defaults by specifying their own `interval_modifiers`. If an asset doesn't specify interval modifiers, it will inherit the pipeline defaults.

### Examples:
- `start: -2h` → shift start time back 2 hours
- `end: -1h` →  shift end time back 1 hour

Each interval is a scalar string with a single unit:

| Unit | Meaning |
|------|---------|
| s    | Seconds |
| m    | Minutes |
| h    | Hours   |
| d    | Days    |
| M    | Months  |

✅ Valid: `5m`, `2h`, `1d`,`-1M`
❌ Invalid: `1h30m`, `2h:30m`, `90min`

## Usage

To apply interval modifiers when running your pipeline, you must include the `--apply-interval-modifiers` flag in your run command:

```bash
bruin run --apply-interval-modifiers my_asset 
```

Without this flag, the pipeline will use the default interval without any modifications.

## Example

```yaml
name: example.products
type: bq.sql

interval_modifiers:
  start: -2h    # Shift start time back 2 hours
  end: 1h       # Shift end time forward 1 hour

columns:
  - name: product_id
    type: INTEGER
    primary_key: true
  - name: created_at
    type: TIMESTAMP
```

Let's say your pipeline is scheduled to run daily. Here's how the interval modifiers affect your processing window:

**Original interval:**
- Start: 2025-01-10 00:00:00
- End: 2025-01-11 00:00:00

**Modified interval:**
- Start: 2025-01-09 22:00:00 (shifted back 2 hours)
- End: 2025-01-11 01:00:00 (shifted forward 1 hour)

This ensures you capture any late-arriving data from the previous day and any early events from the next interval.




### Materialization

# Materialization

Materialization is the idea of taking a simple `SELECT` query, and applying the necessary logic to materialize the results into a table or view.

Bruin supports various materialization strategies catered to different use cases.

Here's a sample asset with materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

## Definition Schema
The top level `materialization` key determines how the asset will be materialized.

Here's an example materialization definition:
```yaml
materialization:
  type: table
  strategy: delete+insert
  incremental_key: dt
  partition_by: dt
  cluster_by:
    - dt
    - user_id
```

### `materialization > type`
The type of the materialization, can be one of the following:
- `table`
- `view`

**Default:** none

### `materialization > strategy`
The strategy used for the materialization, can be one of the following:
- `create+replace`: overwrite the existing table with the new version.
- `delete+insert`: incrementally update the table by only refreshing a certain partition.
- `append`: only append the new data to the table, never overwrite.
- `merge`: merge the existing records with the new records, requires a primary key to be set.
- `time_interval`: incrementally load time-based data within specific time windows.
- `DDL`: create a new table using a DDL (Data Definition Language) statement.
- `scd2_by_column`: implement SCD2 logic that tracks changes based on column value differences.
- `scd2_by_time`: implement SCD2 logic that tracks changes based on time-based incremental key.

### `materialization > partition_by`
Define the column that will be used for the partitioning of the resulting table. This is used to instruct the data warehouse to set the column for the partition key.

- **Type:** `String`
- **Default:** none


### `materialization > cluster_by`
Define the columns that will be used for the clustering of the resulting table. This is used to instruct the data warehouse to set the columns for the clustering.

- **Type:** `String[]`
- **Default:** `[]`

### `materialization > incremental_key`

This is the column of the table that will be used for incremental updates of the table.
- **Type:** `String`
- **Default:** `[]`

## Strategies
Bruin supports various materialization strategies that take your code and convert it to another structure behind the scenes to materialize the execution results of your assets.

### Default: no materialization

By default, Bruin does not apply any materialization to the assets. This means that the query will be executed every time the asset is run, and you are responsible for storing the results in a table via a `CREATE TABLE` or a similar statement in your SQL asset.

### `create+replace`

This materialization strategy is useful when you want to create a table if it does not exist, and replace the contents of the table with the results of the query. This is useful when you want to ensure that the table is always up-to-date with the query results.

`create+replace` strategy does not do any incremental logic, which means it's a full refresh every time the asset is run. This can be expensive for large tables.

Here's an example of an asset with `create+replace` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

The result will be a table `dashboard.hello_bq` with the result of the query.

### `delete+insert`
`delete+insert` strategy is useful for incremental updates. It deletes the rows that are no longer present in the query results and inserts the new rows. This is useful when you have a large table and you want to minimize the amount of data that needs to be written.

This strategy requires an `incremental_key` to be specified. This key is used to determine which rows to delete and which rows to insert.

Bruin implements `delete+insert` strategy in the following way:
- run the asset query, put the results in a temp table
- run a `SELECT DISTINCT` query on the temp table to get the unique values of the `incremental_key`
- run a `DELETE` query on the target table to delete all the rows that match the `incremental_key` values determined above
- run an `INSERT` query to insert the new rows from the temp table

Here's an example of an asset with `delete+insert` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: UserId

@bruin */

select 1 as UserId, 'Alice' as Name
union all
select 2 as UserId, 'Bob' as Name
```

### `append`
`append` strategy is useful when you want to add new rows to the table without overwriting the existing rows. This is useful when you have a table that is constantly being updated and you want to keep the history of the data.

Bruin will simply run the query, and insert the results into the destination table.

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: append

@bruin */

select 1 as one
union all
select 2 as one
```

### `merge`
`merge` strategy is useful when you want to merge the existing rows with the new rows. This is useful when you have a table with a primary key and you want to update the existing rows and insert the new rows, helping you avoid duplication while keeping the most up-to-date version of the data in the table incrementally.

Merge strategy requires columns to be defined and marked with `primary_key` or `update_on_merge`.
- `primary_key` is used to determine which rows to update and which rows to insert.
- `update_on_merge` is used to determine which columns to update when a row already exists. By default, this is considered to be `false`.

> [!INFO]
> An important difference between `merge` and `delete+insert` is that `merge` will update the existing rows, while `delete+insert` will delete the existing rows and insert the new rows. This means if your source has deleted rows, `merge` will not delete them from the destination, whereas `delete+insert` will if their `incremental_key` matches.

Here's a sample asset with `merge` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: UserId
    type: integer
    primary_key: true
  - name: UserName
    type: string
    update_on_merge: true

@bruin */

select 1 as UserId, 'Alice' as UserName
union all
select 2 as UserId, 'Bob' as UserName
```

### `time_interval`

The `time_interval` strategy is designed for incrementally loading time-based data. It's useful when you want to process data within specific time windows, ensuring efficient updates of historical data while maintaining data consistency.

This strategy requires the following configuration:
- `incremental_key`: The column used for time-based filtering
- `time_granularity`: Must be either 'date' or 'timestamp'
  - Use 'date' when your incremental_key is a DATE column (e.g., '2024-03-20')
  - Use 'timestamp' when your incremental_key is a TIMESTAMP column (e.g., '2024-03-20 15:30:00')

When running assets with time_interval strategy, you can specify the time window using the start and end date flags:
```bash
bruin run --start-date "2024-03-01" --end-date "2024-03-31" path/to/your/asset
```

By default:
- `start-date`: Beginning of yesterday (00:00:00.000000)
- `end-date`: End of yesterday (23:59:59.999999)

Here's a sample asset with `time_interval` materialization:
```bruin-sql
/* @bruin
name: dashboard.hello_bq
type: bq.sql

materialization:
  type: table
  strategy: time_interval
  time_granularity: date
  incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
  - name: dt
    type: DATE
    description: "Date when the product was last updated"
@bruin */


SELECT
    1 AS product_id,
    'Laptop' AS product_name,
    999.99 AS price,
    10 AS stock,
    DATE '2025-03-15' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    699.99 AS price,
    50 AS stock,
    DATE '2024-03-16' AS dt;
```

The strategy will:
1. Begin a transaction
2. Delete existing records within the specified time interval
3. Insert new records from the query given in the asset

### `DDL`

The `DDL` (Data Definition Language) strategy is used to create a new table using the information provided in the 
embedded YAML section of the asset. This is useful when you want to create a new table with a specific schema and structure
and ensure that this table is only created once.

The `DDL` strategy defines the table structure via column definitions in the columns field of the asset. 
For this reason, you should not include any query after the embedded YAML section.

Here's an example of an asset with `DDL` materialization:

```bruin-sql
/* @bruin
name: dashboard.products
type: bq.sql

materialization:
  type: table
  strategy: ddl
  partition_by: product_category

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_category
    type: VARCHAR
    description: "Category of the product"
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: price
    type: FLOAT
    description: "Price of the product in USD"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
@bruin */

```

This strategy will:
- Create a new empty table with the name `dashboard.products`
- Use the provided schema to define the column names, column types as well as optional primary key constraints and descriptions.

The strategy also supports partitioning and clustering for data warehouses that support these features. You can specify
in the materialization definition with the following keys:
- `partition_by`
- `cluster_by`

### `scd2_by_column`

The `scd2_by_column` strategy implements [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension) logic, which maintains a full history of data changes over time. This strategy is useful when you want to track changes to records and preserve the historical state of your data.

This strategy automatically detects changes in non-primary key columns and creates new versions of records when changes occur, while marking previous versions as historical.

**Requirements:**
- At least one column must be marked as `primary_key: true`
- The column names `_valid_from`, `_valid_until`, and `_is_current` are reserved and cannot be used in your column definitions

**How it works:**
When changes are detected in non-primary key columns:
1. The existing record is marked as historical (`_is_current: false`) and gets an end timestamp in `_valid_until`
2. A new record is inserted with the updated values (`_is_current: true`) and `_valid_until` set to '9999-12-31'
3. Records that no longer exist in the source are marked as historical

**Automatically added columns:**
- `_valid_from`: TIMESTAMP when the record version became active (set to `CURRENT_TIMESTAMP()`)
- `_valid_until`: TIMESTAMP when the record version became inactive (set to `TIMESTAMP('9999-12-31')` for current records)
- `_is_current`: BOOLEAN indicating if this is the current version of the record

Here's an example of an asset with `scd2_by_column` materialization:

```bruin-sql
/* @bruin
name: test.product_catalog
type: bq.sql

materialization:
  type: table
  strategy: scd2_by_column

columns:
  - name: ID
    type: INTEGER
    description: "Unique identifier for Product"
    primary_key: true
  - name: Name
    type: VARCHAR
    description: "Name of the Product"
  - name: Price
    type: FLOAT
    description: "Price of the Product"
@bruin */

SELECT 1 AS ID, 'Wireless Mouse' AS Name, 29.99 AS Price
UNION ALL
SELECT 2 AS ID, 'USB Cable' AS Name, 12.99 AS Price
UNION ALL
SELECT 3 AS ID, 'Keyboard' AS Name, 89.99 AS Price
```

**Example behavior:**

Let's say you want to create a new table to track product catalog with SCD2. If the table doesn't exist yet, you'll need an initial run with the `--full-refresh` flag:

```bash
bruin run --full-refresh path/to/your/product_catalog.sql
```

This initial run creates:
```
ID | Name          | Price | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse| 29.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
2  | USB Cable     | 12.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
3  | Keyboard      | 89.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
```

Now lets say you have new incoming data that updates Wireless Mouse price to 39.99, removes Keyboard from the catalog, and adds a new item Monitor. When you run the asset again:

```bash
bruin run path/to/your/product_catalog.sql
```

The table becomes:
```
ID | Name          | Price | _is_current | _valid_from         | _valid_until
1  | Wireless Mouse| 29.99 | false       | 2024-01-01 10:00:00| 2024-01-02 14:30:00
1  | Wireless Mouse| 39.99 | true        | 2024-01-02 14:30:00| 9999-12-31 23:59:59
2  | USB Cable     | 12.99 | true        | 2024-01-01 10:00:00| 9999-12-31 23:59:59
3  | Keyboard      | 89.99 | false       | 2024-01-01 10:00:00| 2024-01-02 14:30:00
4  | Monitor       | 199.99| true        | 2024-01-02 14:30:00| 9999-12-31 23:59:59
```

Notice how:
- Wireless Mouse (ID=1) now has two records: the old price (marked as historical) and the new price (current)
- USB Cable (ID=2) remains unchanged with its original record still current
- Keyboard (ID=3) is marked as historical since it's no longer in the source data
- Monitor (ID=4) is added as a new current record

### `scd2_by_time`

The `scd2_by_time` strategy implements [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension) logic based on a time-based incremental key. This strategy is ideal when your source data includes timestamps or dates that indicate when records were last modified, and you want to maintain historical versions based on these time changes.

**Requirements:**
- At least one column must be marked as `primary_key: true`
- An `incremental_key` must be specified that references a column of type `TIMESTAMP` or `DATE`
- The column names `_valid_from`, `_valid_until`, and `_is_current` are reserved and cannot be used in your column definitions

**How it works:**
The strategy tracks changes based on the time values in the `incremental_key` column:
1. When a record has a newer timestamp than existing records, it creates a new version
2. Previous versions are marked as historical (`_is_current: false`) with their `_valid_until` updated
3. Records no longer present in the source are marked as historical

**Automatically added columns:**
- `_valid_from`: TIMESTAMP when the record version became active (derived from the `incremental_key`)
- `_valid_until`: TIMESTAMP when the record version became inactive (set to `TIMESTAMP('9999-12-31')` for current records)
- `_is_current`: BOOLEAN indicating if this is the current version of the record

Here's an example of an asset with `scd2_by_time` materialization:

```bruin-sql
/* @bruin
name: test.products
type: bq.sql

materialization:
  type: table
  strategy: scd2_by_time
  incremental_key: dt

columns:
  - name: product_id
    type: INTEGER
    description: "Unique identifier for the product"
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: "Name of the product"
  - name: stock
    type: INTEGER
    description: "Number of units in stock"
  - name: dt
    type: DATE
    description: "Date when the product was last updated"
@bruin */

SELECT
    1 AS product_id,
    'Laptop' AS product_name,
    100 AS stock,
    DATE '2025-04-02' AS dt
UNION ALL
SELECT
    2 AS product_id,
    'Smartphone' AS product_name,
    150 AS stock,
    DATE '2025-04-02' AS dt
```

**Example behavior:**

Let's say you want to create a new table to track product inventory with SCD2 based on time. If the table doesn't exist yet, you'll need an initial run with the `--full-refresh` flag:

```bash
bruin run --full-refresh path/to/your/products.sql
```

This initial run creates:
```
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
3          | Headphones   | 175   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
4          | Monitor      | 25    | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
```

Now lets say you have new incoming data with updates: Headphones stock changed from 175 to 900 with a new date (2025-06-02), Monitor is no longer available, and a new product PS5 is added. When you run the asset again:

```bash
bruin run path/to/your/products.sql
```

The table becomes:
```
product_id | product_name | stock | _is_current | _valid_from         | _valid_until
1          | Laptop       | 100   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
2          | Smartphone   | 150   | true        | 2025-04-02 00:00:00| 9999-12-31 23:59:59
3          | Headphones   | 175   | false       | 2025-04-02 00:00:00| 2025-06-02 00:00:00
3          | Headphones   | 900   | true        | 2025-06-02 00:00:00| 9999-12-31 23:59:59
4          | Monitor      | 25    | false       | 2025-04-02 00:00:00| 2025-06-02 00:00:00
5          | PS5          | 25    | true        | 2025-06-02 00:00:00| 9999-12-31 23:59:59
```

Notice how:
- Laptop (ID=1) and Smartphone (ID=2) remain unchanged with their original records still current
- Headphones (ID=3) now has two records: the old stock level (marked as historical) and the new stock level (current) based on the newer date
- Monitor (ID=4) is marked as historical since it's no longer in the source data
- PS5 (ID=5) is added as a new current record with the latest date

**Key differences between scd2_by_column and scd2_by_time:**

| Aspect | scd2_by_column | scd2_by_time |
|--------|----------------|--------------|
| **Change Detection** | Automatically detects changes in any non-primary key column | Based on time values in the incremental_key column |
| **_valid_from Value** | Set to `CURRENT_TIMESTAMP()` when change is processed | Derived from the incremental_key column value |
| **Use Case** | When you want to track any column changes regardless of when they occurred | When your source data has reliable timestamps indicating when changes happened |
| **Configuration** | Only requires primary_key columns | Requires both primary_key columns and incremental_key |

> [!WARNING]
> SCD2 materializations are currently only supported for bigquery.


### Python

# Python Assets

Bruin takes the Python data development experience to the next level:
- Bruin runs assets in isolated environments: mix and match Python versions & dependencies
- It installs & manages Python versions automatically, so you don't have to have anything installed
- You can return dataframes and it uploads them to your destination
- You can run quality checks on it just as a regular asset

Bruin uses the amazing [`uv`](https://astral.sh/uv) under the hood to abstract away all the complexity.

Python assets are built to be as flexible as possible. You can use any Python package you want, as long as it is installable with `pip`.

```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.13
connection: bigquery

materialization:
  type: table
  strategy: merge

columns:
  - name: col1
    type: integer
    checks:
      - name: unique
      - name: not_null
@bruin"""

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

## Dependency resolution
Python assets are searching for the closest `requirements.txt` file in the file tree and creates a virtual environment for that file.

For example, assume you have a file tree such as:
```
* folder1/
    * folder2/
        * test.py
        * requirements.txt
    * folder3/
        * test2.py
    * requirements.txt
* folder4/
    * folder5/
        * folder6/
            * test3.py
* requirements.txt
```

* When Bruin runs `test.py`, it will use the `folder1/folder2/requirements.txt` in `folder2`, since they are in the same folder.  
* For `test2.py`, since there is no `requirements.txt` in the same folder, Bruin goes up one level in the tree and finds `folder1/requirements.txt`.  
* Similarly, `requirements.txt` in the main folder used for `test3.py` since none of `folder6`, `folder5` and `folder4` have any `requirements.txt` files.

## Python versions
Bruin supports various Python versions in the same pipeline, all running in isolated environments. The resolved dependencies will be installed correctly for the corresponding Python version without impacting each other.

You can define Python versions using the `image` key:
```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.11
@bruin"""

print('hello world')
```

## Environment Variables
Bruin introduces a set of environment variables by default to every Python asset.

### Builtin

The following environment variables are available in every Python asset execution:

| Environment Variable  | Description                                                                                                |
| :-------------------- | :--------------------------------------------------------------------------------------------------------- |
| `BRUIN_START_DATE`    | The start date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)                             |
| `BRUIN_START_DATETIME`| The start date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`)      |
| `BRUIN_START_TIMESTAMP`| The start timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`) |
| `BRUIN_END_DATE`      | The end date of the pipeline run in `YYYY-MM-DD` format (e.g. `2024-01-15`)                               |
| `BRUIN_END_DATETIME`  | The end date and time of the pipeline run in `YYYY-MM-DDThh:mm:ss` format (e.g. `2024-01-15T13:45:30`)        |
| `BRUIN_END_TIMESTAMP` | The end timestamp of the pipeline run in RFC3339 format with timezone (e.g. `2024-01-15T13:45:30.000000Z07:00`)   |
| `BRUIN_RUN_ID`        | The unique identifier for the pipeline run                                                                 |
| `BRUIN_PIPELINE`      | The name of the pipeline being executed                                                                    |
| `BRUIN_FULL_REFRESH`  | Set to `1` when the pipeline is running with the `--full-refresh` flag, empty otherwise                    |

### Pipeline

<!--TODO: update variables reference to point to dedicated docs section, once it's ready -->
Bruin supports user-defined variables at a pipeline level. These become available as a JSON document in your python asset as `BRUIN_VARS`. See [variables](/assets/templating/templating#adding-variables) for more information on how to define variables for your assets.

Here's a short example:
::: code-group
```yaml [pipeline.yml]
name: pipeline-with-variables
variables:
  env:
    type: string
    default: dev
  users:
    type: array
    items:
      type: string
    default: ["jhon", "nick"]
```
:::

::: code-group
```bruin-python [asset.py]
""" @bruin
name: inspect_env
@bruin """

import os
import json

vars = json.loads(os.environ.get("BRUIN_VARS"))

print("env:", vars["env"])      # env: dev
print("users:", vars["users"])  # users: ['jhon', 'nick']
```
:::

::: tip
You can override the value of variables at runtime using `--var` [flag](/assets/templating/templating#overriding-variables).
:::


## Materialization

Bruin runs regular Python scripts by default; however, quite often teams need to load data into a destination from their Python scripts. Bruin supports materializing the data returned by a Python script into a data warehouse.

The requirements to get this working is:
- define a `materialization` config in the asset definition
- have a function called `materialize` in your Python script that returns a pandas/polars dataframe or a list of dicts.

> [!WARNING]
> This feature has been very recently introduced, and is not battle-tested yet. Please create an issue if you encounter any bugs.

```bruin-python
"""@bruin
name: tier1.my_custom_api
image: python:3.13
connection: bigquery

materialization:
  type: table
  strategy: merge
 
columns:
    - name: col1
      primary_key: true
@bruin"""

import pandas as pd

def materialize(**kwargs):
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

### Under the hood

Bruin uses Apache Arrow under the hood to keep the returned data efficiently, and uses [ingestr](https://github.com/bruin-data/ingestr) to upload the data to the destination. The workflow goes like this:
- install the asset dependencies using `uv`
- run the `materialize` function of the asset
- save the returned data into a temporary file using Arrow memory-mapped files
- run ingestr to load the Arrow memory-mapped file into the destination
- delete the memory-mapped file

This flow ensures that the typing information gathered from the dataframe will be preserved when loading to the destination, and it supports incremental loads, deduplication, and all the other features of ingestr.

## Column-level lineage

Bruin supports column-level lineage for Python assets as well as SQL assets. In order to get column-level lineage, you need to annotate the columns that are exposed by the Bruin asset.

```bruin-python
""" @bruin
name: myschema.my_mat_asset 
materialization:
  type: table
  strategy: merge

columns:
    - name: col1
      type: int
      upstreams:       # [!code ++]
        - table: xyz   # [!code ++]
          column: col1 # [!code ++]

@bruin """

import pandas as pd

def materialize():
    items = 100000
    df = pd.DataFrame({
        'col1': range(items),
        'col2': [f'value_new_{i}' for i in range(items)],
        'col3': [i * 6.0 for i in range(items)]
    })

    return df
```

Bruin will use the annotations to build the column-lineage dependency across all of your assets, including those extracted from SQL automatically.


## Examples
### Print hello world
```bruin-python
""" @bruin
name: hello_world
@bruin """

print("Hello World!")
```

### Ingest data to BigQuery via an API manually
```bruin-python
""" @bruin
name: raw_data.currency_rates
type: python
parameters:
    loader_file_format: jsonl
secrets:
    - key: bigquery_conn
@bruin """

import os
import currency_rates
import pandas as pd
import json
from google.cloud import bigquery

# Bruin injects secrets as a JSON string.
# This function takes a connection name and returns a BigQuery client
def get_bq_client(conn_name: str) -> bigquery.Client:
    serv_acc = json.loads(os.environ[conn_name])
    return bigquery.Client.from_service_account_info(
        json.loads(serv_acc["service_account_json"]), 
        project=serv_acc["project_id"]
    )

START_DATE = os.environ["BRUIN_START_DATE"]
END_DATE = os.environ["BRUIN_END_DATE"]

bq_client = get_bq_client("bigquery_conn")
df = currency_rates.get_rates(start=START_DATE, end=END_DATE)

df.to_gbq("raw_data.currency_rates", if_exists="replace", credentials=bq_client._credentials)
```



### Seed

# Seed Assets
Seeds are CSV-files that contain data that is prepared outside of your pipeline that will be loaded into your data platform. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the destination platform accurately.

You can define seed assets in a file ending with `.asset.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

The `type` key in the configuration defines what platform to run the query against. 

You can see the "Data Platforms" on the left sidebar to see supported types.

## Parameters

The `parameters` key in the configuration defines the parameters for the seed asset. The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.

##  Examples
The examples below show how to load a CSV into a DuckDB & BigQuery database.

### Simplest: Load csv into a Duckdb
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv
```

Example CSV:
```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```

This operation will load the CSV into a table called `seed.raw` in the DuckDB database.

### Adding quality checks
You can attach quality checks to seed assets the same way you do for other assets.

```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: hello.csv

columns:
  - name: name
    type: string
    checks:
      - name: not_null
      - name: unique
```

The example above ensures that the `name` column contains unique and non-null values after the CSV is loaded.



### Sensor

# Sensor Assets

Sensor assets are a special type of asset that allows you to wait until a certain external event has occurred.

The most common use case of a sensor is when a piece of data is being produced by external systems, and you want to wait until that data is available before you start processing it.

Sensors are implemented on a per-platform basis, therefore some platforms may have more sensors than others.

Bruin CLI will run sensors once by default, you can change this behavior by setting the `--sensor-mode` flag to `skip` or `wait`.

## Why?

Sensors allow you to do a couple of things:

- Sensors are useful when you want to wait for an external event to occur before you start processing data.
  - For example, you may want to wait until a file is available in a certain location, or until a certain database table has been populated with data.
- They allow you to enforce quality checks on data that is produced outside of Bruin.
- They allow you to represent external assets within your data lineage and your data catalog.

> [!NOTE]
> Sensors are useful when your pipelines run on a schedule, rather than manual `bruin run` executions. You can control how sensors are executed via the `--sensor-mode` flag in `bruin run` command.

> [!WARNING]
> Bruin CLI supports working sensor implementations for some platforms such as BigQuery or Snowflake, but not all. [Bruin Cloud](https://getbruin.com) supports all sensors, and we are working on adding support for more platforms in Bruin CLI.

Sensors are a crucial part of integrating Bruin with external systems, and we are always happy to improve our sensor coverage.


### Poking Interval

Sensors work as a poking mechanism, they will check the status of the external event every few minutes until the event has occurred.

You can control how often the poking occurs via the `poke_interval` parameter, value being in seconds.

```yaml
name: my_sensor
type: bq.sensor.query
parameters:
    query: select count(*) > 0 from `your-project-id.raw.external_asset`
    poke_interval: 10 # seconds // [!code focus]
```

## Definition Schema

Sensors are defined as YAML files, with the naming schema `<name>.asset.yml`.

They support all the common properties of a Bruin asset, such as documentation, quality checks, and more.

```yaml
name: string
type: string
parameters:
    ... # custom parameters here
```

Here's an example BigQuery SQL sensor:

```yaml
name: raw.external_asset
type: bq.sensor.query
parameters:
    query: select count(*) > 0 from `your-project-id.raw.external_asset`
```

This asset will wait until the query returns any result.

### Adding quality checks
Sensors also support quality checks. You can use them to validate the external data once the sensor succeeds.

```yaml
name: raw.external_asset
type: bq.sensor.query
parameters:
    query: select count(*) > 0 from `your-project-id.raw.external_asset`

columns:
  - name: count
    type: integer
    checks:
      - name: positive
```

The snippet above ensures the `raw.external_asset` table exposes a `count` column and that the values are positive. The sensor waits until the query returns true, and then the quality check validates the data before downstream assets run.








### Sql

# SQL Assets
Bruin supports running SQL assets against a variety of data platforms natively.

You can define SQL assets in a file ending with `.sql`:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

The `type` key in the configuration defines what platform to run the query against. 

You can see the "Data Platforms" on the left sidebar to see supported types.

##  Examples
The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

### Simplest: run `SELECT 1`
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

@bruin */

select 1
```

This operation does not save the result anywhere, it simply runs the query on BigQuery.

### Materialize the data
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
@bruin */

select 1 as one
union all
select 2 as one
```

This example will save the result of this query into a table called `dashboard.hello_bq`.

### Incremental processing
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: dt
@bruin */

select * from my_upstream
where dt between '{{ start_datetime }}' and '{{ end_datetime }}'
```

This example will incrementally update the data in the destination table using this query. Read more about [materialization here](./materialization.md). 

This example also uses Jinja templates, you can read more about [Jinja here](./templating/templating.md).

### Adding quality checks
SQL assets can define quality checks on the columns produced by the query.

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

columns:
  - name: one
    type: integer
    checks:
      - name: positive
      - name: unique
@bruin */

select 1 as one
union all
select -1 as one
```

In this example the `one` column is validated to be positive and unique after the query runs.



## Assets > Templating


### Filters

# Filters

Bruin supports Jinja filters to modify any given variable before they are converted to string. Filters are separated from the variable by a pipe symbol (`|`) and may have optional arguments in parentheses. Multiple filters can be chained. The output of one filter is applied to the next.

```sql
SELECT * 
FROM my_table 
WHERE dt BETWEEN '{{ start_date |  date_format('%Y-%m-%d') }}' 
             AND '{{ end_date | date_format('%Y-%m-%d') }}'
```

## Default Filters
Bruin CLI supports various default filters supported by Jinja, you can see the [list here](https://github.com/NikolaLohinski/gonja/blob/master/docs/filters.md).

## Date Filters
### `add_years`

Adds the given number of years to the date.

```jinja
{{ end_datetime | add_years(2) }}
```

Bruin supports various date-related filters to format dates that can be chained together.

### `add_months`

Adds the given number of months to the date.

```jinja
{{ end_datetime | add_months(3) }}
```


### `add_days`

Adds the given number of days to the date.

```jinja
{{ end_datetime | add_days(3) }}
```

### `add_hours`

Adds the given number of hours to the date.

```jinja
{{ end_datetime | add_hours(3) }}
```

### `add_minutes`

Adds the given number of minutes to the date.

```jinja
{{ end_datetime | add_minutes(3) }}
```

### `add_seconds`

Adds the given number of seconds to the date.

```jinja
{{ end_datetime | add_seconds(3) }}
```

### `add_milliseconds`

Adds the given number of milliseconds to the date.

```jinja
{{ end_datetime | add_milliseconds(3) }}
```

### `truncate_year`

Truncates the given date to the first day of the year at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_year }}  
# Returns: "2024-01-01 00:00:00"
```

### `truncate_month`

Truncates the given date to the first day of the month at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_month }}  
# Returns: "2024-03-01 00:00:00"
```

### `truncate_day`

Truncates the given date to the start of the day at midnight (00:00:00).

```jinja
{{ "2024-03-20 15:30:00" | truncate_day }}  
# Returns: "2024-03-20 00:00:00"
```

### `truncate_hour`

Truncates the given date to the start of the hour (XX:00:00).

```jinja
{{ "2024-03-20 15:30:45" | truncate_hour }}  
# Returns: "2024-03-20 15:00:00"
```

### `date_add`

> [!CAUTION]
> The `date_add` filter is deprecated. Please use the `add_days` filter instead for better compatibility and consistency.


Adds given number of days to the date.

```jinja
{{ end_datetime | date_add(3) }}
```

For a given datetime `2022-02-07T04:00:00.948740Z`, this results in the following string:

```
2022-02-10T04:00:00.948740Z
```

You can also give negative numbers to subtract days.

### `date_format`

Formats the date according to the given format string.

```jinja
{{ end_datetime | date_format('%Y-%m-%d') }}
```

For a given datetime `2022-02-07T04:00:00.948740Z`, this results in the following string:

```
2022-02-07
```

The format given here follows Python date formatting rules, you can see the [list here](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).




### Templating

# Templating

Bruin supports [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) as its templating language for SQL assets. This allows you to write dynamic SQL queries that can be parameterized with variables. This is useful when you want to write a query that is parameterized by a date, a user ID, or any other variable.

The following is an example SQL asset that uses Jinja templating for different `start_date` and `end_date` parameters:

```sql
SELECT * FROM my_table WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

Since `start_date` and `end_date` parameters are automatically passed to your assets by Bruin, this allows the same SQL asset definition to be used both as your regular execution, e.g. daily or hourly, as well as backfilling a longer period of time.

You can do more complex stuff such as looping over a list of values, or using conditional logic. Here's an example of a SQL asset that loops over a list of days and dynamically generates column names.
::: tip Example
`pipeline.yaml`
```yaml 
name: sql-pipeline
variables:
  days:
    type: array
    default: [1, 3, 7, 15, 30, 90]
```

`asset.sql`
```sql 
SELECT
    conversion_date,
    cohort_id,
    {% for day_n in var.days %}
    SUM(IFF(days_since_install < {{ day_n }}, revenue, 0)) 
    AS revenue_{{ day_n }}_days
    {% if not loop.last %},{% endif %}
    {% endfor %}
FROM user_cohorts
GROUP BY 1,2
```
:::

This will render into the following SQL query:

```sql
SELECT
    conversion_date,
    cohort_id,
    SUM(IFF(days_since_install < 1, revenue, 0)) AS revenue_1_days,
    SUM(IFF(days_since_install < 3, revenue, 0)) AS revenue_3_days,
    SUM(IFF(days_since_install < 7, revenue, 0)) AS revenue_7_days,
    SUM(IFF(days_since_install < 15, revenue, 0)) AS revenue_15_days,
    SUM(IFF(days_since_install < 30, revenue, 0)) AS revenue_30_days,
    SUM(IFF(days_since_install < 90, revenue, 0)) AS revenue_90_days
FROM user_cohorts
GROUP BY 1,2
```
You can read more about [Jinja here](https://jinja.palletsprojects.com/en/3.1.x/).

## Adding variables

You can add variables in your `pipeline.yml` file. We support all YAML data types to give you the
maximum flexibility in your variable configuration. `variables` are declared as [JSON Schema object](https://json-schema.org/draft-07/draft-handrews-json-schema-01#rfc.section.4.2.1). Here's a comprehensive example:

::: code-group 
```yaml [pipeline.yml]
name: var-pipeline
variables:
  users:
    type: array
    items:
      type: string
    default: ["jhon", "nick"]
  env:
    type: string
    default: dev
  tags:
    type: object
    properties:
      team:
        type: string
      tenant:
        type: string
    default:
      team: data
      tenant: acme
```
:::
All user-defined variables are accessible via the `var` namespace. For example, if you define a variable called `src`, it will be available as <code>&lbrace;&lbrace; var.src &rbrace;&rbrace;</code> in your assets.

Additionally all top level variables must define a `default` value. This will be used to render your assets in absence of values supplied on the commandline.

## Overriding Variables

You can override variables defined in your `pipeline.yml` file during a `bruin run` execution by using the `--var` flag. This allows you to dynamically adjust variable values without modifying the pipeline configuration file.

Bruin supports two different syntax for overriding variables:
```sh
# key value form; value must be valid JSON
$ bruin run --var key=value

# json form
$ bruin run --var '{"key": "value"}'
```

The `--var` flag can be specified multiple times. However, beware that if you specify the same key multiple times, the latter one will overwrite the previous one.

## Builtin variables

Bruin injects various variables by default:
| Variable | Description | Example |
|----------|-------------|---------|
| `start_date` | The start date in YYYY-MM-DD format | "2023-12-01" |
| `start_datetime` | The start date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-01T15:30:00" |
| `start_timestamp` | The start timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-01T15:30:00.000000+07:00" |
| `end_date` | The end date in YYYY-MM-DD format | "2023-12-02" |
| `end_datetime` | The end date and time in YYYY-MM-DDThh:mm:ss format | "2023-12-02T15:30:00" |
| `end_timestamp` | The end timestamp in [RFC3339](https://datatracker.ietf.org/doc/html/rfc3339) format | "2023-12-02T15:30:00.000000Z07:00" |
| `pipeline` | The name of the currently executing pipeline | `my_pipeline` |
| `run_id` | The unique identifier for the current [pipeline run](../../getting-started/concepts.md#pipeline-run) | `run_1234567890` |

You can use these variables in your SQL queries by referencing them with the `{{ }}` syntax:
```sql
SELECT * FROM my_table 
WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'
```

> [!NOTE]
> Date-related variables are passed in as strings, and they will be driven by the flags given to the `bruin run` command, read more on that [here](../../commands/run.md).

You can modify these variables with the use of [filters](./filters.md).



## Commands


### Clean

# `clean`  Command

The `clean` command is used to remove temporary artifacts related to Bruin.

It currently:
- cleans the cached virtualenv directories inside `~/.bruin` directory
- removes the `logs` directory from the project root

## Usage

```bash
bruin clean [path-to-project-root]
```
**path-to-project-root** (optional): Specifies the path to the root of the project. If omitted, the current directory (`.`) is used as the default.

## Example

### Cleaning the current directory

``` bash
bruin clean 
```
#### Output:


<img alt="Bruin - clean" src="/clean.gif" style="margin: 10px;" />



### Connections

# Connections

Bruin has various commands to handle connections via its CLI. 

The connections are stored in a gitignored file called `.bruin.yml`, and while it is possible to edit this file directly, Bruin CLI offers convenience methods to manage connections.

## List Connections

To list all the connections in the `.bruin.yml` file, run the following command:

```bash
bruin connections list
```

The output will look like this:
```
Environment: default
+---------+-----------+
| TYPE    | NAME      |
+---------+-----------+
| generic | MY_SECRET |
| gorgias | my_conn   |
+---------+-----------+

Environment: someother
+---------+-----------+
| TYPE    | NAME      |
+---------+-----------+
| generic | MY_SECRET |
+---------+-----------+
```


### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |

## Add Connection

To add a connection to a specific environment, run the following command:

```bash
bruin connections add --env someother --type generic --name MY_SECRET --credentials '{"value": "someothersecret"}'
```

This will add the connection to the `.bruin.yml` file and the connection will be available in the given environment.

The parameter after `--credentials` is the value of the connection in JSON format, as you would write it in the `.bruin.yml` file. For further reference, you can check the [Connections section](../getting-started/concepts.md#connection) of the documentation.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |



> [!INFO]
> This command is meant to be used programmatically rather than human beings, since the `credentials` parameter is in JSON format.

### Example: a GCP connection
```bash
bruin connections add \
    --env default \
    --type google_cloud_platform \
    --name MY_GCP_CONNECTION \
    --credentials '{"project_id": "my-gcp-project", "service_account_file": "path/to/service/account/file.json"}'
```

### Example: a generic secret
```bash
bruin connections add \
    --env staging \
    --type generic \
    --name MY_SECRET \
    --credentials '{"value": "secret-password"}'
```


## Delete Connection

To delete a connection from a specific environment, use the following command:

```bash
bruin connections delete --env staging --name MY_SECRET
```

You can define a different path for the repo with an extra argument if you'd like:

```bash
bruin connections delete --env staging --name MY_SECRET <path-to-repo>
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |



### Example

Delete a connection named "my-connection" from the "production" environment:
```
bruin connections delete -e staging -n test-connection -o json
```

## Test Connection
To test if a connection is valid, you can use the test command. 
This command runs a simple validation check for the connection.

```bash
bruin connections test --name <connection-name> [--env <environment>]
```

If no environment flag (`--env`) is provided, the default environment from your `.bruin.yml` will be used.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the `.bruin.yml` file. |


### Examples

Test a connection in the default environment:
```bash
bruin connections test --name my-bigquery-conn
```

Test a connection in a specific environment:
```bash
bruin connections test --name my-snowflake-conn --env production
```

You can also get the output in JSON format:
```bash
bruin connections test --name my-postgres-conn --env staging --output json
```



### Data Diff

# `data-diff` Command

The `data-diff` command compares data between two tables from the same or different data sources. It provides comprehensive schema comparison, statistical analysis, and data profiling to help identify differences between datasets across environments or sources.

This command is particularly useful for:
- Comparing tables between development, staging, and production environments
- Validating data migrations and ETL processes
- Monitoring data drift between different data sources
- Ensuring data consistency across different systems

## Usage

```bash
bruin data-diff [FLAGS] <table1> <table2>
```

**Arguments:**

- **table1:** The first table to compare. Can be specified as `connection:table` or just `table` if using a default connection.
- **table2:** The second table to compare. Can be specified as `connection:table` or just `table` if using a default connection.

<style>
table {
  width: 100%;
}
table th:first-child,
table td:first-child {
  white-space: nowrap;
}
</style>

## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--connection`, `-c` | str | - | Name of the default connection to use when connection is not specified in table arguments |
| `--tolerance`, `-t` | float | `0.001` | Tolerance percentage for considering values equal. Values with percentage difference below this threshold are considered equal |
| `--config-file` | str | `.bruin.yml` | The path to the .bruin.yml configuration file |

## Table Identifier Format

Tables can be specified in two formats:

1. **With connection prefix:** `connection_name:table_name`
   - Example: `prod_db:users`, `staging_bq:events`

2. **Without connection prefix:** `table_name` 
   - Requires the `--connection` flag to specify the default connection
   - Example: `users` (when using `--connection prod_db`)

## What Gets Compared

The `data-diff` command performs a comprehensive comparison that includes:

### Schema Comparison
- **Column names and types:** Identifies missing, extra, and mismatched columns
- **Data type compatibility:** Checks if different types are comparable (e.g., `VARCHAR` vs `STRING`)
- **Nullability constraints:** Compares nullable/not-null settings
- **Uniqueness constraints:** Compares unique/non-unique settings

### Statistical Analysis
For each column that exists in both tables, the command provides detailed statistics based on the column's data type:

#### Numerical Columns
- Row count and null count
- Fill rate (percentage of non-null values)
- Min, Max, Average, Sum values
- Standard deviation

#### String Columns  
- Row count and null count
- Fill rate (percentage of non-null values)
- Distinct value count
- Empty string count
- Min, Max, and Average string lengths

#### Boolean Columns
- Row count and null count
- Fill rate (percentage of non-null values)  
- True and False counts

#### DateTime Columns
- Row count and null count
- Fill rate (percentage of non-null values)
- Distinct value count
- Earliest and Latest dates

#### JSON Columns
- Row count and null count
- Fill rate (percentage of non-null values)

### Difference Calculation
- **Absolute differences:** Raw numeric differences between values
- **Percentage differences:** Relative changes as percentages
- **Tolerance handling:** Values within the specified tolerance are considered equal
- **Color-coded output:** Green for matches/small differences, red for significant differences

## Output Format

The command generates several detailed tables:

1. **Summary Table:** High-level overview of row counts and schema differences
2. **Column Types Comparison:** Side-by-side comparison of column types
3. **Schema Comparison:** Detailed breakdown of matching vs different columns
4. **Column Differences:** Specific differences for columns that exist in both tables
5. **Missing Columns:** Columns that exist in one table but not the other
6. **Statistical Comparison:** Detailed statistics for each common column

## Examples

### Basic Usage

Compare two tables using explicit connection names:
```bash
bruin data-diff prod_db:users staging_db:users
```

Compare tables using a default connection:
```bash
bruin data-diff --connection my_db users_v1 users_v2
```

### Cross-Environment Comparison

Compare the same table across different environments:
```bash
bruin data-diff prod_bq:analytics.users dev_bq:analytics.users
```

### Custom Tolerance

Use a higher tolerance for considering values equal (useful for floating-point comparisons):
```bash
bruin data-diff --tolerance 0.1 prod_db:metrics staging_db:metrics
```

### Custom Config File

Specify a different configuration file:
```bash
bruin data-diff --config-file /path/to/custom/.bruin.yml prod:table1 staging:table2
```

## Supported Data Platforms

The `data-diff` command includes specialized type mapping support for the following data platforms:

- **DuckDB** - Full support for DuckDB data types including `HUGEINT`, `UHUGEINT`, and specialized time types
- **BigQuery** - Native support for BigQuery types including `INT64`, `FLOAT64`, `BIGNUMERIC`, and BigQuery-specific formatting
- **PostgreSQL & AWS Redshift** - Complete support for PostgreSQL types including `SERIAL` types, `MONEY`, network types (`CIDR`, `INET`), and `JSONB`
- **Snowflake** - Full support for Snowflake types including `NUMBER`, `VARIANT`, and timezone-aware timestamp types

### Type Mapping Features

Each supported platform includes intelligent type mapping that:

- **Normalizes data types** across platforms (e.g., PostgreSQL `INTEGER` and BigQuery `INT64` both map to `numeric`)
- **Handles parametrized types** automatically (e.g., `VARCHAR(255)` and `VARCHAR(100)` are both treated as `string`)
- **Supports case-insensitive matching** for type names
- **Maps platform-specific types** to common categories for cross-platform comparisons

### Cross-Platform Comparisons

When comparing tables between different platforms, the command intelligently maps data types to common categories:

- **Numeric types:** All integer, float, decimal, and monetary types
- **String types:** VARCHAR, CHAR, TEXT, and similar text types  
- **Boolean types:** BOOL, BOOLEAN, and logical types
- **DateTime types:** DATE, TIME, TIMESTAMP, and interval types
- **Binary types:** BLOB, BYTEA, BINARY, and similar binary types
- **JSON types:** JSON, JSONB, VARIANT, and structured data types

## Connection Requirements

To use the `data-diff` command, your connections must support table summarization. This means the connection type must implement the `TableSummarizer` interface, which includes the ability to:

- Retrieve table schema information
- Calculate statistical summaries for different column types
- Handle various data types (numerical, string, boolean, datetime, JSON)

For optimal results, use one of the fully supported data platforms listed above. Other database connections may work but may have limited type mapping capabilities.

## Use Cases

### Data Migration Validation
```bash
# Compare source and target after migration
bruin data-diff source_db:customer_data target_db:customer_data
```

### Environment Consistency Checks
If you are comparing different environments, you can use the `--schema-only` flag to only compare the schema of the tables and not the data.
```bash
# Ensure staging matches production structure
bruin data-diff --schema-only prod:important_table staging:important_table --tolerance 0.01
```

### ETL Process Monitoring  
```bash
# Compare before and after transformation
bruin data-diff raw_data:events processed_data:events_cleaned
```

### Data Quality Monitoring
```bash
# Check for unexpected changes in data distribution
bruin data-diff yesterday_snapshot:metrics today_snapshot:metrics
```

## Troubleshooting

### Error: "connection type does not support table summarization"
The specified connection type doesn't support the required table analysis features.

If you'd like to see another platform supported, feel free to open an issue on our [GitHub repository](https://github.com/bruin-data/bruin/issues).

### Error: "incorrect number of arguments"
The command requires exactly two table arguments.

Verify you've provided both table identifiers.

### Error: "connection not specified for table"
Table identifier doesn't include a connection prefix and no default connection was set.

Either use the format `connection:table` or add the `--connection` flag.

### Error: "failed to get connection"
The specified connection name doesn't exist in your configuration.

Check your `.bruin.yml` file for available connections.


### Environments

# `environments` Command

The `environments` command allows you to manage environments defined in the `.bruin.yml` configuration file. 
It currently supports listing all available environments in the current Git repository.

### Usage
```bash
bruin environments [subcommand]
```

## `list` Subcommand

Displays the environments defined in the `.bruin.yml` configuration file in the current Git repository. The environments can be displayed in plain text or JSON format.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the `.bruin.yml` file. |

## Usage

```bash
bruin environments list [flags]
```



### Format

# `format` Command

The `format` command is used to process and format asset definition files in a project. It can handle a single asset file or process all asset files in a given path. 
The command supports two output types: plain text and JSON.

## Usage

```bash
bruin format [path-to-asset-or-project-root] [flags]
```

### Arguments

**path-to-asset-or-project-root** (optional):
- If the argument is a path to an asset definition file, the command processes and formats that single asset.
- If the argument is a path to a project root, it finds and formats all asset files within that path.
- Defaults to the current directory (`.`) if no argument is provided.

### Flags

**--output / -o** (optional):  
Specifies the output format for the command.  
Possible values:
- `plain` (default): Prints human-readable messages.
- `json`: Prints errors (if any) in JSON format.  
- `fail-if-changed`: fail the command if any of the assets need reformatting


### Init

# Init Command

The `init` command initializes a new Bruin pipeline project. It creates a new Bruin pipeline project based on a specified template. If no template is specified, it uses the default template. The command creates a new folder with the project structure and necessary files.


```
bruin init [template name] [folder name]
```

## Arguments

1. `template name` (optional): The name of the template to use. Available templates: 
   - If not provided, the default template is used.

2. `folder name` (optional): The name of the folder where the pipeline will be created.
   - If not provided and using the default template, it will create a folder named "bruin-pipeline".
   - If not provided and using a custom template, it will create a folder with the same name as the template.

> [!INFO]
> If the specified folder already exists, the command will fail.

## Available Templates
In order to see the list of available templates, you can run:
```
bruin init --help
```

The output displays the list of available templates.


## Example

### Initializing the Shopify-Bigquery template

``` bash
bruin init shopify-bigquery
```
#### Output:


<img alt="Bruin - clean" src="/init.gif" style="margin: 10px;" />



### Lineage

# `lineage` Command

The `lineage` command helps you understand how a specific asset fits into your pipeline by showing its dependencies. It tells you:

- What the asset relies on (upstream dependencies).
- What relies on the asset (downstream dependencies).

This is useful for understanding your pipeline’s structure, tracking relationships between components, and troubleshooting issues with asset connections.

```bash
 bruin lineage [flags] <path to the asset definition>
```
### Flags

- `--full`  
  Display all upstream and downstream dependencies, including indirect dependencies.

- `--output`, `-o`  
  Specify the output format. Possible values:
    - `plain` (default): Outputs a human-readable text summary.
    - `json`: Outputs the lineage as structured JSON.

  
## Example

### Understanding the dependencies of Chess template 

``` bash
bruin lineage chess/assets/player_summary.sql 
```
#### Output:


<img alt="Bruin - clean" src="/lineage2.gif" style="margin: 10px;" />




### Query

# Query Command 

The `query` command executes and retrieves the results of a query on a specified connection and 
returns the results on the terminal in either table format or as a JSON object.

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--connection`       | `-c`  |  The name of the connection to use (required).                            |
| `--query`            | `-q`  | The SQL query to execute (required).     |
| `--output [format]`  | `-o`  | Specifies the output type, possible values: `plain`, `json`.                |


### Example

```bash
query --connection my_connection --query "SELECT * FROM table"
```
**Example output:**
```plaintext
+-------------+-------------+----------------+
|   Column1   |   Column2   |    Column3     |
+-------------+-------------+----------------+
| Value1      | Value2      | Value3         |
| Value4      | Value5      | Value6         |
| Value7      | Value8      | Value9         |
+-------------+-------------+----------------+
```



### Render

# `render` Command

The `render` command processes a Bruin SQL asset and generates a SQL query or materialized output for execution. 
It supports multiple databases and output formats, making it a flexible tool for rendering assets in Bruin pipelines.

## Usage

```bash
bruin render [path to asset definition] [flags]
```
<img alt="Bruin - clean" src="/render.gif" style="margin: 10px;" />
### Arguments

**path-to-asset-definition* (required):
- The file path to the Bruin SQL asset you want to render.


### Flags

| Flag               | Alias | Description                                                           |
|--------------------|-------|-----------------------------------------------------------------------|
| `--full-refresh`   | `-r`  | Truncate the table before running the query.                          |
| `--start-date`     |       | Specify the start date in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` format.|
| `--end-date`       |       | Specify the end date in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` format. |
| `--output [format]`| `-o`  | Specify the output format (e.g., `json`). Defaults to console output.  |
| `--config-file`    |       | The path to the `.bruin.yml` file. |


### Examples

**Render an Asset with Default Settings :**

```bash
bruin render path/to/asset.yml
```
**Render an Asset with a Date Range:**
```bash
bruin render path/to/asset.yml --start-date 2024-01-01 --end-date 2024-01-31
```
**Render an Asset in JSON Format:**
```bash
bruin render path/to/asset.yml --output json
```



### Run

# `run` Command

This command is used to execute a Bruin pipeline or a specific asset within a pipeline.

- You can run the pipeline from the current directory or a specific path to the pipeline/task definition.
- If you don't specify a path, Bruin will run the pipeline from the current directory.
- If you specify a path, Bruin will run the pipeline/asset from the directory of the file.
  - Bruin will try to infer if the given path is a pipeline or an asset and will run accordingly.
- You can give specific start and end dates to run the pipeline/asset for a specific range.
- You can limit the types of tasks to run by using the `--only` flag.
  - E.g. only run the quality checks: `bruin run --only checks`

```bash
bruin run [FLAGS] [optional path to the pipeline/asset]
```
<img alt="Bruin - init" src="/chesspipeline.gif" style="margin: 10px;" />

<style>
table {
  width: 100%;
}
table th:first-child,
table td:first-child {
  white-space: nowrap;
}
</style>

## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--downstream` | bool | `false` | Run all downstream tasks as well. |
| `--workers` | int | `16` | Number of workers to run tasks in parallel. |
| `--start-date` | str | Beginning of yesterday | The start date of the range the pipeline will run for. Format: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` |
| `--end-date` | str | End of yesterday | The end date of the range the pipeline will run for. Format: `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DD HH:MM:SS.ffffff` |
| `--environment` | str | - | The environment to use. |
| `--push-metadata` | bool | `false` | Push metadata to the destination database if supported (currently BigQuery). |
| `--force` | bool | `false` | Do not ask for confirmation in a production environment. |
| `--no-log-file` | bool | `false` | Do not create a log file for this run. |
| `--sensor-mode` | str | `'once'` | Set sensor mode: `skip`, `once`, or `wait`. |
| `--full-refresh` | bool | `false` | Truncate the table before running. |
| `--apply-interval-modifiers` | bool | `false` | Apply interval modifiers. |
| `--use-pip` | bool | `false` | Use pip for managing Python dependencies. |
| `--continue` | bool | `false` | Continue from the last failed asset. |
| `--tag` | str | - | Pick assets with the given tag. |
| `--single-check` | str | - | Run a single column or custom check by ID. |
| `--exclude-tag` | str | - | Exclude assets with the given tag. |
| `--only` | []str | `'main', 'checks', 'push-metadata'` | Limit the types of tasks to run. By default it runs `main` and `checks`, while `push-metadata` is optional if defined in the pipeline definition. |
| `--exp-use-winget-for-uv` | bool | `false` | Use PowerShell to manage and install `uv` on Windows. Has no effect on non-Windows systems. |
| `--debug-ingestr-src` | str | - | Use ingestr from the given path instead of the builtin version. |
| `--config-file` | str | - | The path to the `.bruin.yml` file. |
| `--no-validation` | bool | `false` | Skip validation for this run. |
| `--no-timestamp` | bool | `false` | Skip logging timestamps for this run. |
| `--no-color` | bool | `false` | Plain log output for this run. |
| `--minimal-logs` | bool | `false` | Skip initial pipeline analysis logs for this run. |
| `--var` | []str | - | Override pipeline variables with custom values. |


### Continue from the last failed asset

If you want to continue from the last failed task, you can use the `--continue` flag. This will run the pipeline/asset from the last failed task. Bruin will automatically retrieve all the flags used in the last run. 

```bash
bruin run --continue 
```

> [!NOTE]
> This will only work if the pipeline structure is not changed. If the pipeline structure has changed in any way, including asset dependencies, you will need to run the pipeline/asset from the beginning. This is to ensure that the pipeline/asset is run in the correct order.

### Focused Runs: Filtering by Tags and Task Types
As detailed in the flag section above, the  `--tag`, `--downstream`, `--exclude-tag`, and `--only` flags provide powerful ways to filter and control which tasks in your pipeline are executed. These flags can also be combined to fine-tune pipeline runs, allowing you to execute specific subsets of tasks based on tags, include their downstream dependencies, and restrict execution to certain task types.

Let's explore how combining these flags enables highly targeted pipeline execution scenarios:


### Combining Tags and Task Types
Using `--tag` with `--only` restricts the tasks to specific types for the assets filtered by the given tag. For example:
```bash
bruin run --tag quality_tag --only checks
```
This runs only the `checks` tasks for the assets tagged with `quality_tag` while excluding other task types.

### Combining Exclude Tag and Task Types
Using `--exclude-tag` with `--only` allows you to run specific task types while excluding assets with certain tags. For example:
```bash
bruin run --exclude-tag quality_tag --only checks
```
This runs the `checks` tasks for all assets EXCEPT those tagged with `quality_tag`. This is useful when you want to skip certain assets while running specific task types.

### Combining Tag and Exclude-Tag
Using `--tag` with `--exclude-tag` allows you to include specific assets and then exclude certain ones based on another tag. For example:
```bash
bruin run --tag important_tag --exclude-tag quality_tag
```
This command will run tasks for assets tagged with `important_tag` but will exclude those that also have the `quality_tag`. This is useful for focusing on a subset of assets while excluding others that meet certain criteria.


### Combining Downstream and Other Filtering Flags
The `--downstream` flag can be used when running a single asset. You can combine it with other flags like `--exclude-tag` and `--only` to refine your task execution. For example:

- **Using `--downstream` with `--exclude-tag`:**
  ```bash
  bruin run --downstream --exclude-tag quality_tag
  ```
  This command will run tasks for a single asset and exclude any tasks for assets tagged with `quality_tag`.

- **Using `--downstream` with `--only`:**
  ```bash
  bruin run --downstream --only checks
  ```
  This command will run only the `checks` tasks for a single asset, allowing you to focus on specific task types.

These combinations provide flexibility in managing task execution by allowing you to exclude certain assets or focus on specific task types while using the `--downstream` flag.



## Examples
Run the pipeline from the current directory:
```bash
bruin run
```

Run the pipeline from a file:
```bash
bruin run ./pipelines/project1/pipeline.yml
```

Run a specific asset:
```bash
bruin run ./pipelines/project1/assets/my_asset.sql
```

Run the pipeline with a specific environment:
```bash
bruin run --environment dev
```

Run the pipeline with a specific start and end date:
```bash
bruin run --start-date 2024-01-01 --end-date 2024-01-31
```

Run the assets in the pipeline that contain a specific tag:
```bash
bruin run --tag my_tag
```

Run only the quality checks:
```bash
bruin run --only checks
```

Run only the main tasks and not the quality checks:
```bash
bruin run --only main
```


## Metadata Push

Metadata push is a feature that allows you to push metadata to the destination database/data catalog if supported. Currently, we support BigQuery as the catalog.

There are two ways to push metadata:
1. You can set the `--push-metadata` flag to `true` when running the pipeline/asset.
2. You can fill out the `metadata_push` dictionary in the pipeline/asset definition.

```yaml
# pipeline.yml
name: bruin-init
schedule: daily
start_date: "2024-09-01"

default_connections:
   google_cloud_platform: "my-gcp-connection"

metadata_push:
  bigquery: true 
```

When pushing the metadata, Bruin will detect the right connection to use, same way as it happens with running the asset.











### Validate

# `validate` Command

The `validate` command checks the Bruin pipeline configurations for all pipelines in a specified directory or validates a single asset. 
It ensures that the configurations meet specific requirements and are properly configured for the selected environment.

## Usage

```bash
bruin validate [path to pipelines] [flags]
```
<img alt="Bruin - validate" src="/validate.gif" style="margin: 10px;" />

**Arguments:**

- **[path to pipelines]:** Path to the pipeline directory or a specific asset.
Defaults to the current directory (".") if not provided.

**Flags:**

| Flag                     | Alias     | Description                                                                 |
|--------------------------|-----------|-----------------------------------------------------------------------------|
| `--environment`          | `-e, --env` | Specifies the environment to use for validation.                          |
| `--force`                | `-f`       | Forces validation even if the environment is a production environment.     |
| `--output [format]`      | `-o`       | Specifies the output type, possible values: `plain`, `json`.               |
| `--exclude-warnings`     |            | Excludes warnings from the validation output.                              |
| `--config-file`          |            | The path to the `.bruin.yml` file.                                           |
| `--exclude-tag`          |            | Excludes assets with the given tag from validation.                          |
| `--fast`                 |            | Runs only fast validation rules, excludes some important rules such as query validation. |



### Dry-run Validation
One of the beneficial features of the `validate` command is the ability to perform a dry-run validation on the destination data platform. This means, effectively Bruin runs a dry-run version of the query to ensure that the query is valid and can be executed on the destination data platform. This gives a very strong peace of mind in terms of the accuracy of the queries from a syntactical and semantical perspective, and also ensures that the query can be executed on the destination data platform.

Dry-run is automatically enabled for BigQuery and Snowflake.

However, there are also scenarios where dry-run is not the best suited tool:
- Dry-run requires all the tables/views to be there, which means if you are running validation on a table that you haven't created yet, it'll fail.
- Due to the same reason, dry-run will also fail if you are adding a new column to a table and its upstream, but you haven't created them in the destination yet.

In the end, it is better to treat dry-run as an extra check, and accept that it might give false negatives from time to time.

## Examples

**1. Validate all pipelines in the current directory:**

```bash
bruin validate
```

**2. Validate with JSON output:**

```bash
bruin validate --output json

```


**3. Validate a specific asset:**

```bash
bruin validate path/to/specific-asset

```


## Platforms


### Athena

# AWS Athena

Bruin supports AWS Athena as a query engine, which means you can use Bruin to build tables and views in your data lake with Athena.

> [!WARNING]
> Bruin materializations will always create Iceberg tables on Athena. You can still write SQL scripts for legacy tables and not use [materialization](../assets/materialization.md) features.


## Connection
In order to have set up an Athena connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    athena:
        - name: "connection_name"
          region: "us-west-2"
          database: "some_database"
          access_key_id: "XXXXXXXX"
          secret_access_key: "YYYYYYYY"
          query_results_path: "s3://some-bucket/some-path"
          session_token: "ZZZZZZZ" # optional
          profile: "some_profile" # optional
```

You have two ways to set credentials:
- You can put your `access_key_id` and `secret_access_key`, as well as an optional `session_token` here directly.
- Alternatively, you can put your `profile` here, and if you have your local AWS credentials in `~/.aws` configured, Bruin will use them.

The field `database` is optional, if not provided, it will default to `default`.

> [!WARNING]
> The results of the materialization as well as any temporary tables Bruin needs to create will be stored at the location defined by `query_results_path`. This location must be writable and might be required to be empty at the beginning. 


## Athena Assets

### `athena.sql`
Runs a materialized Athena asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to aggregate website traffic data
```bruin-sql
/* @bruin
name: website_traffic.view
type: athena.sql
materialization:
    type: view
@bruin */

select
    date,
    count(distinct user_id) as unique_visitors,
    sum(page_views) as total_page_views,
    avg(session_duration) as avg_session_duration
from raw_web_traffic
group by date;
```

Create a table to analyze daily sales performance:
```bruin-sql
/* @bruin
name: daily_sales_analysis.view
type: athena.sql
materialization:
    type: table
@bruin */

select
    order_date,
    sum(total_amount) as total_sales,
    count(distinct order_id) as total_orders,
    avg(total_amount) as avg_order_value
from sales_data
group by order_date;
```

Bruin Athena assets support partitioning by one column only
```bruin-sql
/* @bruin
name: daily_sales_analysis.view
type: athena.sql
materialization:
    type: table
    partition_by: order_date # <----------
@bruin */

select
    order_date,
    sum(total_amount) as total_sales,
    count(distinct order_id) as total_orders,
    avg(total_amount) as avg_order_value
from sales_data
group by order_date;
```


### `athena.seed`
`athena.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Athena database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Athena database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: athena.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Athena database

The examples below show how to load a CSV into an Athena database.
```yaml
name: dashboard.hello
type: athena.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Bigquery

# Google BigQuery

Google BigQuery is a fully-managed, serverless data platform that enables super-fast SQL queries using the processing power of Google's infrastructure.

Bruin supports BigQuery as a data platform.

## Connection

Google BigQuery requires a Google Cloud Platform connection, which can be added as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      google_cloud_platform:
        - name: "connection_name"
          project_id: "project-id"
          
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```

## BigQuery Assets

### `bq.sql`
Runs a materialized BigQuery asset or a BigQuery script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a BigQuery script
```bruin-sql
/* @bruin
name: events.install
type: bq.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```


### `bq.sensor.table`

Sensors are a special type of assets that are used to wait on certain external signals.


Checks if a table exists in BigQuery, runs every 5 minutes until this table is available.

```yaml
name: string
type: string
parameters:
    table: string
```
**Parameters**:
- `table`: `project-id.dataset_id.table_id` format, requires all of the identifiers as a full name.


#### Examples
```yaml
# Google Analytics Events that checks if the recent date table is available
name: analytics_123456789.events
type: bq.sensor.table
parameters:
    table: "your-project-id.analytics_123456789.events_{{ end_date | date_format('%Y%m%d') }}"
```

### `bq.sensor.query`

Checks if a query returns any results in BigQuery, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: bq.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `bq.seed`
`bq.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your BigQuery database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the BigQuery database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a BigQuery database

The examples below show how to load a CSV into a BigQuery database.
```yaml
name: dashboard.hello
type: bq.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Clickhouse

# Clickhouse
[Clickhouse](https://clickhouse.com/) is a high-performance, column-oriented SQL database management system for online analytical processing.
Bruin supports Clickhouse as both a source and a destination.

## Connection
In order to set up a Clickhouse connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
connections:
    clickhouse:
        - name: "connection_name"
          username: "clickhouse"
          password: "XXXXXXXXXX"
          host: "some-clickhouse-host.somedomain.com"   
          port: 9000
          database: "dev" #Optional for other assets, uneffective when using ClickHouse as an ingestr destination/source, as ingestr takes the database name from the asset file. 
          http_port: 8443 #Only specify if you are using clickhouse as ingestr destination, by default it is 8443
          secure: 1 #Only specify if you are using clickhouse as ingestr destination/source, by default, it is set to 1 (secure). Use 0 for a non-secure connection and 1 for a secure connection.
```
## Ingestr Assets:
After adding connection in `bruin.yml`. To ingest data to clickhouse, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) inside the assets folder and add the following content:
### 
```yaml
name: publicDB.stripe
type: ingestr

parameters:
  source_connection: stripe-default
  source_table: 'events'
  destination: clickhouse
```
In this case, the Clickhouse database is `publicDB`. Please ensure that the necessary permissions are granted to the user. For more details on obtaining credentials and setting up permissions, you can refer to this [guide](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse#2-setup-clickhouse-database)


## Clickhouse Assets

### `clickhouse.sql`
Runs a materialized clickhouse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

### Examples
Create a view to determine the top 10 earning drivers in a taxi company:
```bruin-sql
/* @bruin
name: highest_earning_drivers
type: clickhouse.sql
materialization:
    type: view
@bruin */

SELECT 
    driver_id, 
    SUM(fare_amount) AS total_earnings 
FROM trips 
GROUP BY driver_id 
ORDER BY total_earnings DESC 
LIMIT 10;
```

View Top 5 Customers by Spending:
```bruin-sql
/* @bruin
name: top_five_customers
type: clickhouse.sql
materialization:
    type: view
@bruin */

SELECT 
    customer_id, 
    SUM(fare_amount) AS total_spent 
FROM trips 
GROUP BY customer_id 
ORDER BY total_spent DESC 
LIMIT 5;
```

Table with average driver rating:
```bruin-sql
/* @bruin
name: average_Rating
type: clickhouse.sql
materialization:
    type: table
@bruin */

SELECT 
    driver_id, 
    AVG(rating) AS average_rating 
FROM trips 
GROUP BY driver_id 
ORDER BY average_rating DESC;
```

### `clickhouse.sensor.query`

Checks if a query returns any results in Clickhouse, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: clickhouse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `clickhouse.seed`
`clickhouse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Clickhouse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Clickhouse database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Clickhouse database

The examples below show how to load a CSV into a Clickhouse database:
```yaml
name: dashboard.hello
type: clickhouse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Databricks

# Databricks

Databricks is a unified data analytics platform that provides a collaborative environment for data scientists, data engineers, and business analysts. It is built on top of Apache Spark, which makes it easy to scale and process big data workloads.

Bruin supports Databricks as a data platform.

## Connection

In order to work with Databricks you can add as a configuration item to `connections` in the `.bruin.yml` file complying with the following schema:

```yaml
    connections:
      databricks:
        - name: "connection_name"
          token: "your-databricks-token"
          path: "your-databricks-endpoint-path"
          host: "your-databricks-host"
          port: "your-databricks-port"
          catalog: "your-databricks-catalog"
          schema: "your-databricks-schema"
```

## Databricks Assets

### `databricks.sql`
Runs a materialized Databricks asset or a Databricks SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a script
```bruin-sql
/* @bruin
name: events.install
type: databricks.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `databricks.sensor.query`

Checks if a query returns any results in Databricks, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: databricks.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `databricks.seed`
`databricks.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Databricks database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Databricks database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Databricks database

The examples below show how to load a CSV into a Databricks database.
```yaml
name: dashboard.hello
type: databricks.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Duckdb

# Duck DB

DuckDB is an in-memory database designed to be fast and reliable.

Bruin supports using a local DuckDB database.

## Connection

```yaml
    connections:
      duckdb:
        - name: "connection_name"
          path: "/path/to/your/duckdb/database.db"
```

The field `path` is the only one you need and it can point to an existing database or the full path of the database that you want to create and where your queries would be materialized.

> [!WARNING]
> DuckDB does not allow concurrency between different processes, which means other clients should not be connected to the database while Bruin is running.


## Assets

DuckDB assets should use the type `duckdb.sql` and if you specify a connection it must be of the `duckdb` type.
For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


### Examples

Create a view with orders per country
```bruin-sql
/* @bruin
name: orders_per_country
type: duckdb.sql
materialization:
    type: view
@bruin */

SELECT COUNT(*) as orders, country
FROM events.orders
WHERE status = "paid"
GROUP BY country
```

Materialize new customers per region and append them to an existing table
```bruin-sql
/* @bruin
name: new_customers_per_region
type: duckdb.sql
materialization:
    type: table
    strategy: append
@bruin */

SELECT COUNT(*) as customers, region 
    WHERE created_at >= {{ start_date }} 
      AND created_at < {{ end_date }}
FROM events.customers
```

### `duckdb.sensor.query`

Checks if a query returns any results in DuckDB, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: duckdb.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```


### `duckdb.seed`
`duckdb.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your DuckDB database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the DuckDB database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Duckdb database

The examples below show how to load a CSV into a DuckDB database.
```yaml
name: dashboard.hello
type: duckdb.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Emr_Serverless

# AWS EMR Serverless Spark
Amazon EMR (Elastic MapReduce) Serverless is a deployment option for Amazon EMR that provides a serverless runtime environment. This simplifies the operation of analytics applications that use the latest open-source frameworks, such as Apache Spark and Apache Hive. With EMR Serverless, you don’t have to configure, optimize, secure, or operate clusters to run applications with these frameworks.

Bruin supports EMR Serverless as a data platform. You can use Bruin to integrate your Spark workloads into complex pipelines that use different data technologies, all without leaving your terminal. 

## Connection

In order to use Bruin to run Spark jobs in EMR Serverless, you need to define an `emr_serverless` connection in your `.bruin.yml` file. Here's a sample `.bruin.yml` with the required fields defined.

```yaml 
environments:
  default:
    connections:
      emr_serverless:

        # name of your connection
      - name: emr_serverless-default

        # AWS credentials
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1

        # name of your EMR application
        application_id: EMR_APPLICATION_ID

        # role assumed by your job. This determines
        # what AWS resources your spark job can access.
        execution_role: IAM_ROLE_ARN

        # (Python assets only)
        # declares working area used by pyspark jobs.
        workspace: s3://your-bucket/optional-prefix/

```


## Logs
Bruin supports log streaming for spark jobs. This is only supported for spark logs stored in [S3](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/logging.html#jobs-log-storage-s3-buckets). Both `DRIVER` and `EXECUTOR` logs are streamed by default.

In order to stream logs, one of the following conditions must be met:
* Your EMR Serverless Application is pre-configured with an S3 Log Storage Location. 
* `parameters.logs` must be defined

> [!NOTE]
> Python assets stream logs out-of-the-box. You don't need to specify `parameters.logs` for them.

## EMR Serverless Assets

Bruin supports two different ways of defining a Spark asset:
- what we call a "managed" PySpark asset where Bruin takes care of delivering the code to the cluster as well
- as an external asset defined with YAML where Bruin simply orchestrates

### `emr_serverless.pyspark`
A fully managed option where Bruin takes care of job setup, configuration, and execution. You only need to define the workload logic.

* Supports only PySpark scripts.
* Handles artifact deployment.
* Automatic log configuration.
* Concurrent-safe by default.
* Bundles internal dependencies and configures your job to use them.

#### Example: Standalone script
```bruin-python
""" @bruin
name: pyspark_job
type: emr_serverless.pyspark
connection: app_staging
@bruin """

from pyspark.sql import SparkSession

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("jobName").getOrCreate()
  run_workload(spark_session)
  spark_session.stop()

def run_workload(session):
  """
    crunch some numbers
  """
  ...

```

This defines a pyspark asset that will be executed by the EMR Serverless Application defined by the connection named `app_staging`.

The `run_workload` function is there for demonstration. You can structure your pyspark scripts however you like.

#### Example: Multi-module script

Advanced Spark users often package core logic into reusable libraries to improve consistency, reduce duplication, and streamline development across jobs. This approach ensures that shared transformations, validations, and business rules are implemented once and reused reliably.

Bruin has seamless support for pyspark modules.

For this example, let's assume this is how your Bruin pipeline is structured:
```
acme_pipeline/
├── assets
│   └── main.py
├── lib
│   └── core.py
└── pipeline.yml
```

Let's say that `acme_pipeline/lib/core.py` stores some common routines used throughout your jobs. For this example, we'll create a function called `sanitize` that takes in a Spark DataFrame and sanitize its columns (A common operation in Data Analytics).

::: code-group
```python [acme_pipeline/lib/core.py]
from pyspark.sql import DataFrame

def sanitize(df: DataFrame):
  """
  sanitize a dataframe
  """
  ...
```
:::

You can now import this package in your PySpark assets.
::: code-group
```bruin-python [acme_pipeline/assets/main.py]
""" @bruin
name: raw.transaction
type: emr_serverless.pyspark
connection: app_staging
@bruin """

from acme_pipeline.lib.core import sanitize
from pyspark.sql import SparkSession

if __name__ == "__main__":
  session = SparkSession.builder.appName("raw.transaction_std").getOrCreate()
  src = session.sparkContext.textFile("s3://acme/data/transactions").toDF()
  sanitize(src)
  session.stop()

```
:::

Bruin internally sets the [`PYTHONPATH`](https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH) to the root of your pipeline. So you'll always have to use the fully qualified package name to import any internal packages. 

#### Workspace
Python assets require `workspace` to be configured in your `emr_serverless` connection. Workspace is a S3 path that is used by Bruin as working storage for jobs that run on `emr_serverless`.

Bruin uses this S3 path for:
* Storing Logs.
* Staging your entrypoint file.
* Uploading bundled dependencies.

![workspace diagram](media/pyspark-workspace.svg)

### `emr_serverless.spark`
A lightweight option that only supports triggering a job. 

* Supports both PySpark scripts and JARs.
* Users are responsible for:
  * deploying their artifacts
  * managing internal dependencies

Choose the format that best fits your use case—use YAML when you want to integrate with pre-existing infrastructure, or use Python for a streamlined, fully-managed experience.


#### Example

```yaml
name: spark_example_job
type: emr_serverless.spark
connection: app_staging
parameters:
  entrypoint: s3://amzn-test-bucket/src/script.py
  config: --conf spark.executor.cores=1
```

This defines an asset that runs a spark job on an EMR Serverless Application defined by the connection named `app_staging`. The script at `s3://amzn-test-bucket/src/script.py` is configured as the [entrypoint](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html#spark-params) of the job.

> [!note]
> YAML and Python assets require different `type` parameter.
> * YAML-style assets: `emr_serverless.spark` 
> * Python assets:  `emr_serverless.pyspark`.

## Quality Checks
[Quality checks](/quality/overview.md) for EMR Serverless are powered via [AWS Athena](/platforms/athena.md). 

> [!WARNING]
> Bruin currently requires a few extra steps in order to be able to run quality checks for your EMR Serverless Assets.
> Future versions of Bruin will automate this process for you. 

### Prerequisites
* Configure an [athena connection](/platforms/athena.html#connection) in your `bruin.yml`.
* Set `parameters.athena_connection` to the name of your Athena connection.
* Create an `athena` table on top of your data with the same name as your Asset's name.

### Example

To demonstrate quality checks, we're going to write a simple pyspark script that writes static data as CSV to a bucket in S3.

#### Initial Setup

We're going to start by creating a pipeline called `quality-checks-example`. We first run `bruin init` to create the skeleton structure.
```sh
bruin init default quality-checks-example
```

Now we'll add the pyspark asset.
::: code-group
```bruin-python [quality-checks-example/assets/users.py]
""" @bruin
name: users
type: emr_serverless.pyspark
@bruin """

from pyspark.sql import SparkSession

SCHEMA = ["id", "name", "age"]
USERS = [
  (1, "Alice", 29),
  (2, "Bob", 31),
  (3, "Cathy", 25),
  (4, "David", 35),
  (5, "Eva", 28),
]

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("users").getOrCreate()
  df = spark.createDataFrame(USERS, SCHEMA)
  df.write.csv("s3://acme/user/list", mode="overwrite")
  spark.stop()
```
:::

Next let's setup the `bruin.yml` file with the credentials necessary to run our job.
::: code-group
```yaml [bruin.yml]
environments:
  default:
    connections:
      emr_serverless:
      - name: emr_serverless-default
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1
        application_id: EMR_APPLICATION_ID
        execution_role: IAM_ROLE_ARN
        workspace: s3://acme/bruin-pyspark-workspace/
```
:::

We can now run the pipeline to verify that it works
```sh
bruin run ./quality-checks-example
```

::: info Output
<pre><b>Analyzed the pipeline &apos;quality-checks-example&apos; with 1 assets.</b>

<span style="color:#3465A4"><b>Pipeline: quality-checks-example</b></span>
<span style="color:#4E9A06">  No issues found</span>

<span style="color:#4E9A06"><b>✓ Successfully validated 1 assets across 1 pipeline, all good.</b></span>

<b>Starting the pipeline execution...</b>
<span style="color:#8D8F8A">[2025-05-11 18:10:00]</span> <span style="color:#C4A000">Running:  users</span>
<span style="color:#8D8F8A">[2025-05-11 18:10:00]</span> <span style="color:#C4A000">... output omitted for brevity ...</span>
<span style="color:#8D8F8A">[2025-05-11 18:10:05]</span> <span style="color:#C4A000">Finished: users</span>

<span style="color:#4E9A06"><b>Executed 1 tasks in 5s</b></span>
</pre>
:::

#### Enabling quality checks

In order to run quality checks, we need to:
1. Create an Athena table in our AWS account.
2. Configure an `athena` connection in our `bruin.yml` file.

To start, let's first create our Athena table. Go to your AWS Athena console and run the following DDL Query to create a table over the data our pyspark job created.
```sql
CREATE EXTERNAL TABLE users (id int, name string, age int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://acme/user/list' 
```

> [!TIP]
> For more information on creating Athena tables, see [Create tables in Athena](https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html) and [Use SerDe](https://docs.aws.amazon.com/athena/latest/ug/serde-reference.html) in the AWS Athena Documentation.

Next, update your `bruin.yml` file with an athena connection.

```yaml [bruin.yml]
environments:
  default:
    connections:
      emr_serverless:
      - name: emr_serverless-default
        access_key: AWS_ACCESS_KEY_ID
        secret_key: AWS_SECRET_ACCESS_KEY
        region: eu-north-1
        application_id: EMR_APPLICATION_ID
        execution_role: IAM_ROLE_ARN
        workspace: s3://acme/bruin-pyspark-workspace/
      athena:                                           # [!code ++]
      - name: quality-tests                             # [!code ++]
        access_key_id: AWS_ACCESS_KEY_ID                # [!code ++]
        secret_access_key: AWS_SECRET_ACCESS_KEY        # [!code ++]
        region: eu-north-1                              # [!code ++]
        query_results_path: "s3://acme/athena-output/"  # [!code ++]

```

Now we can update our assets to define some quality checks. For this example, we're going to add one column and one custom check.

::: code-group
```bruin-python [quality-checks-example/assets/users.py]
""" @bruin
name: users
type: emr_serverless.pyspark
columns:              # [!code ++]
  - name: id          # [!code ++]
    type: integer     # [!code ++]
    checks:           # [!code ++]
      - non_negative  # [!code ++]
custom_checks: # [!code ++] 
  - name: users are adults # [!code ++]
    query: SELECT count(*) from users where age < 18 # [!code ++]
    value: 0 # [!code ++]
parameters: # [!code ++]
  athena_connection: quality-tests # [!code ++]
@bruin """

from pyspark.sql import SparkSession

SCHEMA = ["id", "name", "age"]
USERS = [
  (1, "Alice", 29),
  (2, "Bob", 31),
  (3, "Cathy", 25),
  (4, "David", 35),
  (5, "Eva", 28),
]

if __name__ == "__main__":
  spark_session = SparkSession.builder.appName("users").getOrCreate()
  df = spark.createDataFrame(USERS, SCHEMA)
  df.write.csv("s3://acme/user/list", mode="overwrite")
  spark.stop()
```
:::

::: tip
If all your assets share the same `type` and `parameters.athena_connection`, you can set them as [defaults](/getting-started/concepts.html#defaults) in your `pipeline.yml` to avoid repeating them for each asset.


```yaml 
name: my-pipeline
default:
  type: emr_serverless.pyspark
  parameters:
    athena_connection: quality-checks
```
:::
Now when we run our Bruin pipeline again, our quality checks should run after our Asset run finishes.
```sh
bruin run ./quality-checks-example
```

::: info Output
<pre><b>Analyzed the pipeline &apos;quality-checks-example&apos; with 1 assets.</b>

<span style="color:#3465A4"><b>Pipeline: quality-checks-example</b></span>
<span style="color:#4E9A06">  No issues found</span>

<span style="color:#4E9A06"><b>✓ Successfully validated 1 assets across 1 pipeline, all good.</b></span>

<b>Starting the pipeline execution...</b>
<span style="color:#8D8F8A">[2025-05-11 18:20:36]</span> <span style="color:#3465A4">Running:  users</span>
<span style="color:#8D8F8A">[2025-05-11 18:20:36]</span> <span style="color:#3465A4">... output omitted for brevity ...</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:01]</span> <span style="color:#3465A4">Finished: users</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:02]</span> <span style="color:#75507B">Running:  users:id:non_negative</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:02]</span> <span style="color:#C4A000">Running:  users:custom-check:users_are_adults</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:06]</span> <span style="color:#75507B">Finished: users:id:non_negative</span>
<span style="color:#8D8F8A">[2025-05-11 18:21:06]</span> <span style="color:#C4A000">Finished: users:custom-check:users_are_adults</span>

<span style="color:#4E9A06"><b>Executed 3 tasks in 30s</b></span>
</pre>
:::

## Variables

Both built-in variables (e.g., `BRUIN_START_DATE`, `BRUIN_RUN_ID`) and any user-defined variables (from your `pipeline.yml`) are accessible directly as environment variables within the execution environment of your PySpark jobs.

For `emr_serverless` assets, these environment variables can be accessed using `os.environ` in your PySpark scripts, similar to regular Python assets.

Refer to the [Python assets documentation](/assets/python.md#environment-variables) for more information.

::: tip
These variables are available in both `pyspark` and `spark` assets. So you can leverage the power of variables regardless of which asset kind you utilize.
:::
## Asset Schema

Here's the full schema of the `emr_serverless.spark` asset along with a brief explanation:

```yaml
# required
name: spark_submit_test

# required, should be one of 
#   - emr_serverless.spark    (yaml)
#   - emr_serverless.pyspark  (python)
type: emr_serverless.spark 

# optional, defaults to emr_serverless-default
connection: connection-name-example  

# job specific configuration
parameters:

  # path of the pyspark script or jar to run (required) [yaml only]
  entrypoint: s3://amzn-test-bucket/src/script.py   

  # path where logs are stored or should be stored (optional)
  logs: s3://amzn-test-bucket/logs

  # name of your athena connection (optional, defaults to "athena-default")
  # used for quality checks
  athena_connection: athena-conn

  # args to pass to the entrypoint (optional)
  args: arg1 arg2

  # spark configuration (optional)
  config: --conf spark.executor.cores=1

  # timeout for the job, defaults to 0 which means no time limit (optional)
  timeout: 10m
```

For more details on EMR Serverless applications, see [AWS Documentation][emr-app]


[emr-app]: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html


### Ingestr

# Ingestr Assets
[Ingestr](https://github.com/bruin-data/ingestr) is a Python package that allows you to easily move data between platforms. Bruin supports `ingestr` natively as an asset type.

Using Ingestr, you can move data from:
* your production databases like:
    * MSSQL
    * MySQL
    * Oracle
* your daily tools like:
    * Notion
    * Google Sheets
    * Airtable
* from other platforms such as:
    * Hubspot
    * Salesforce
    * Google Analytics
    * Facebook Ads
    * Google Ads

to your data platforms:
* Google BigQuery 
* Snowflake 
* AWS Redshift 
* Azure Synapse 
* Postgres 

> [!INFO]
> You can read more about the capabilities of ingestr [in its documentation](https://bruin-data.github.io/ingestr/).



## Template
```yaml
name: string
type: ingestr
connection: string # optional, by default uses the default connection for destination platform in pipeline.yml
parameters:
  source: string # optional, used when inferring the source from connection is not enough, e.g. GCP connection + GSheets source
  source_connection: string
  source_table: string
  destination: bigquery | snowflake | redshift | synapse
  
  # optional
  incremental_strategy: replace | append | merge | delete+insert
  incremental_key: string
  sql_backend: pyarrow | sqlalchemy
  loader_file_format: jsonl | csv | parquet
```


> [!INFO]
> Ingestr assets require Docker to be installed in your machine. If you are using [Bruin Cloud](https://getbruin.com), you don't need to worry about this.


##  Examples
The examples below show how to use the `ingestr` asset type in your pipeline. Feel free to change them as you wish according to your needs.

### Copy a table from MySQL to BigQuery
```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: public.transactions
  destination: bigquery
```

### Copy a table from Microsoft SQL Server to Snowflake incrementally
This example shows how to use `updated_at` column to incrementally load the data from Microsoft SQL Server to Snowflake.
```yaml
name: raw.transactions
type: ingestr
parameters:
  source_connection: mysql_prod
  source_table: dbo.transactions
  destination: snowflake
  incremental_strategy: append
  incremental_key: updated_at
```

### Copy data from Google Sheets to Snowflake
This example shows how to copy data from Google Sheets into your Snowflake database

```yaml
name: raw.manual_orders
type: ingestr
parameters:
  source: gsheets
  source_connection: gcp-default
  source_table: <mysheetid>.<sheetname>
  destination: snowflake
```



### Materialization

# Materialization

Materialization is the idea of taking a simple `SELECT` query, and applying the necessary logic to materialize the results into a table or view. This is a common pattern in data engineering, where you have a query that is expensive to run, and you want to store the results in a table for faster access.

Bruin supports various materialization strategies catered to different use cases.

Here's a sample asset with materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

## Default: no materialization

By default, Bruin does not apply any materialization to the assets. This means that the query will be executed every time the asset is run, and you are responsible for storing the results in a table via a `CREATE TABLE` or a similar statement in your SQL asset.

## `create+replace`

This materialization strategy is useful when you want to create a table if it does not exist, and replace the contents of the table with the results of the query. This is useful when you want to ensure that the table is always up-to-date with the query results.

`create+replace` strategy does not do any incremental logic, which means it's a full refresh every time the asset is run. This can be expensive for large tables.

Here's an example of an asset with `create+replace` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table

@bruin */

select 1 as one
union all
select 2 as one
```

The result will be a table `dashboard.hello_bq` with the result of the query.

## `delete+insert`
`delete+insert` strategy is useful for incremental updates. It deletes the rows that are no longer present in the query results and inserts the new rows. This is useful when you have a large table and you want to minimize the amount of data that needs to be written.

This strategy requires an `incremental_key` to be specified. This key is used to determine which rows to delete and which rows to insert.

Bruin implements `delete+insert` strategy in the following way:
- run the asset query, put the results in a temp table
- run a `SELECT DISTINCT` query on the temp table to get the unique values of the `incremental_key`
- run a `DELETE` query on the target table to delete all the rows that match the `incremental_key` values determined above
- run an `INSERT` query to insert the new rows from the temp table

Here's an example of an asset with `delete+insert` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: UserId

@bruin */

select 1 as UserId, 'Alice' as Name
union all
select 2 as UserId, 'Bob' as Name
```

## `append`
`append` strategy is useful when you want to add new rows to the table without overwriting the existing rows. This is useful when you have a table that is constantly being updated and you want to keep the history of the data.

Bruin will simply run the query, and insert the results into the destination table.

```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: append

@bruin */

select 1 as one
union all
select 2 as one
```

## `merge`
`merge` strategy is useful when you want to merge the existing rows with the new rows. This is useful when you have a table with a primary key and you want to update the existing rows and insert the new rows, helping you avoid duplication while keeping the most up-to-date version of the data in the table incrementally.

Merge strategy requires columns to be defined and marked with `primary_key` or `update_on_merge`.
- `primary_key` is used to determine which rows to update and which rows to insert.
- `update_on_merge` is used to determine which columns to update when a row already exists. By default, this is considered to be `false`.

> [!INFO]
> An important difference between `merge` and `delete+insert` is that `merge` will update the existing rows, while `delete+insert` will delete the existing rows and insert the new rows. This means if your source has deleted rows, `merge` will not delete them from the destination, whereas `delete+insert` will if their `incremental_key` matches.

Here's a sample asset with `merge` materialization:
```bruin-sql
/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
    type: table
    strategy: merge

columns:
  - name: UserId
    type: integer
    primary_key: true
  - name: UserName
    type: string
    update_on_merge: true

@bruin */

select 1 as UserId, 'Alice' as UserName
union all
select 2 as UserId, 'Bob' as UserName
```


### Mssql

# Microsoft SQL Server

Bruin supports Microsoft SQL Server as a data platform.

> [!NOTE]
> We tend to use "MS SQL" interchangeably to refer to Microsoft SQL Server, apologies for any confusion.


## Connection
In order to set up a SQL Server connection in Bruin, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mssql:
        - name: "connection_name"
          username: "mssql_user"
          password: "XXXXXXXXXX"
          host: "mssql_host.somedomain.com"
          port: 1433
          database: "dev"
```


## SQL Server Assets

### `ms.sql`
Runs a materialized SQL Server asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Examples
Run an MS SQL script to generate sales report
```bruin-sql
/* @bruin
name: sales_report
type: ms.sql
@bruin */

with monthly_sales as (
    select
        product_id,
    year(order_date) as order_year,
    month(order_date) as order_month,
    sum(quantity) as total_quantity,
    sum(price) as total_sales
from sales.orders
group by product_id, year(order_date), month(order_date)
    )
select
    product_id,
    order_year,
    order_month,
    total_quantity,
    total_sales
from monthly_sales
order by order_year, order_month;
```

### `ms.sensor.query`

Checks if a query returns any results in SQL Server, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: ms.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: ms.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `ms.seed`
`ms.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your MSSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the MSSQL database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: ms.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a MSSQL database

The examples below show how to load a CSV into an MSSQL database.
```yaml
name: dashboard.hello
type: ms.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Postgres

# PostgreSQL

Bruin supports PostgreSQL as a data platform.

## Connection
In order to set up a PostgreSQL connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      postgres:
        - name: "connection_name"
          username: "pguser"
          password: "XXXXXXXXXX"
          host: "pghost.somedomain.com"
          port: 5432
          database: "dev"
          ssl_mode: "allow" # optional
          schema: "schema_name" # optional
          pool_max_conns: 5 # optional
```

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).

## PostgreSQL Assets

### `pg.sql`
Runs a materialized Postgres asset or an sql script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: pg.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Postgres script
```bruin-sql
/* @bruin
name: events.install
type: pg.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```
### `pg.sensor.query`

Checks if a query returns any results in Postgres, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: pg.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `pg.seed`
`pg.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your PostgreSQL database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the PostgreSQL database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Postgres database

The examples below show how to load a CSV into a PostgreSQL database.
```yaml
name: dashboard.hello
type: pg.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Redshift

# AWS Redshift

Bruin supports AWS Redshift as a data platform, which means you can use Bruin to build tables and views in your Redshift data warehouse.

## Connection
In order to set up a Redshift connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema
Mind that, despite the connection being at all effects a Postgres connection, the default `port` field of Amazon Redshift is `5439`.

```yaml
    connections:
      redshift:
        - name: "connection_name"
          username: "awsuser"
          password: "XXXXXXXXXX"
          host: "redshift-cluster-1.xxxxxxxxx.eu-north-1.redshift.amazonaws.com"
          port: 5439
          database: "dev"
          ssl_mode: "allow" # optional
```

> [!NOTE]
> `ssl_mode` should be one of the modes describe in the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION).


### Making Redshift publicly accessible

Before the connection works properly, you need to ensure that the Redshift cluster can be accessed from the outside. In order to do that you must mark the configuration option in your Redshift cluster

![Make publicly available](/publicly-accessible.png)

In addition to this, you must configure the inbound rules of the security group your Redshift cluster belongs to, to accept inbound connections. In the example below we enabled access for all origins but you can set more restrictive rules for this.

![Inbound Rules](/inbound-rules.png)

If you have trouble setting this up you can check [AWS documentation](https://repost.aws/knowledge-center/redshift-cluster-private-public) on the topic


## AWS Redshift Assets

### `rs.sql`
Runs a materialized AWS Redshift asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a table for product reviews
```bruin-sql
/* @bruin
name: product_reviews.table
type: rs.sql
materialization:
    type: table
@bruin */

create table product_reviews (
    review_id bigint identity(1,1),
    product_id bigint,
    user_id bigint,
    rating int,
    review_text varchar(500),
    review_date timestamp
);
```

#### Example: Run an AWS Redshift script to clean up old data
```bruin-sql
/* @bruin
name: clean_old_data
type: rs.sql
@bruin */

begin transaction;

delete from user_activity
where activity_date < dateadd(year, -2, current_date);

delete from order_history
where order_date < dateadd(year, -5, current_date);

commit transaction;
```

### `rs.sensor.query`

Checks if a query returns any results in Redshift, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: rs.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: rs.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `rs.seed`
`rs.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Redshift database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Redshift database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: rs.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Redshift database

The examples below show how to load a CSV into a Redshift database.
```yaml
name: dashboard.hello
type: rs.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Snowflake

# Snowflake Assets
Bruin supports Snowflake as a data platform.

## Connection
In order to set up a Snowflake connection, you need to add a configuration item to `connections` in the `.bruin.yml` file.

There's 2 different ways to fill it in

```yaml
    connections:
      snowflake:
        - name: "connection_name"
          username: "sfuser"
          password: "XXXXXXXXXX"
          account: "AAAAAAA-AA00000"
          database: "dev"
          schema: "schema_name" # optional
          warehouse: "warehouse_name" # optional
          role: "data_analyst" # optional
          region: "eu-west1" # optional
          private_key_path: "path/to/private_key" # optional
```

Where account is the identifier that you can copy here:

![Snowflake Account](/snowflake.png)

### Key-based Authentication

Snowflake currently supports both password-based authentication as well as key-based authentication. In order to set up key-based authentication, follow the following steps.


#### Step 1: Generate a key-pair

Open your terminal and run the following command to create a key pair. If you’re using a mac, OpenSSL should be installed by default, so no additional setup is required. For Linux or Windows, you may need to [install OpenSSL first](https://docs.openssl.org/3.4/man7/ossl-guide-introduction/).

```terminal
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

#### Step 2: Set public key for Snowflake user

Log into Snowflake as an admin, create a new worksheet and run the following command (don't forget the single quotes around the key):

```sql
ALTER USER your_snowflake_username
SET RSA_PUBLIC_KEY='your_public_key_here';
```

#### Step 3: Verify
```sql
DESC USER your_snowflake_username;
```

This will show a column named `RSA_PUBLIC_KEY`. You should see your actual key there.

#### Step 4: Update Bruin configuration

In your `.bruin.yml` file, update the Snowflake connection configuration to include the `private_key_path` parameter pointing to your private key file. For example:

```yaml
            snowflake:
                - name: snowflake-default
                  username: JOHN_DOE
                  account: EXAMPLE-ACCOUNT
                  database: dev
                  schema: schema_name
                  warehouse: warehouse_name
                  role: data_analyst
                  region: eu-west1
                  private_key_path: /Users/johndoe/rsa_key.pem
```


For more details on how to set up key-based authentication, see [this guide](https://select.dev/docs/snowflake-developer-guide/snowflake-key-pair).


## Snowflake Assets

### `sf.sql`
Runs a materialized Snowflake asset or a Snowflake script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.


#### Example: Create a table using table materialization
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
materialization:
    type: table
@bruin */

select user_id, ts, platform, country
from analytics.events
where event_name = "install"
```

#### Example: Run a Snowflake script
```bruin-sql
/* @bruin
name: events.install
type: sf.sql
@bruin */

create temp table first_installs as
select 
    user_id, 
    min(ts) as install_ts,
    min_by(platform, ts) as platform,
    min_by(country, ts) as country
from analytics.events
where event_name = "install"
group by 1;

create or replace table events.install
select
    user_id, 
    i.install_ts,
    i.platform, 
    i.country,
    a.channel,
from first_installs as i
join marketing.attribution as a
    using(user_id)
```

### `sf.sensor.query`


Checks if a query returns any results in Snowflake, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters:**
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table
Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table
Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: sf.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `sf.seed`
`sf.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Snowflake database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Snowflake database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Snowflake database

The examples below show how to load a CSV into a Snowflake database.
```yaml
name: dashboard.hello
type: sf.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



### Synapse

# Microsoft Azure Synapse

Bruin supports Azure Synapse as a data platform, which means you can use it to build data pipelines on Synapse.

> [!WARNING]
> I'll be honest with you: Synapse is the least used platform in the list, so there might be rough edges. If you run into any issues, please let us know by opening an issue on [GitHub](https://github.com/bruin-data/bruin/issues).

## Connection
Synapse connection is configured the same way as Microsoft SQL Server connection, check [SQL Server connection](mssql.md#connection) for more details.

## Synapse Assets
### `synapse.sql`
Runs a materialized Synapse asset or an SQL script. For detailed parameters, you can check [Definition Schema](../assets/definition-schema.md) page.

#### Example: Create a view using view materialization
```bruin-sql
/* @bruin
name: customer_data.view
type: synapse.sql
materialization:
    type: view
@bruin */

select customer_id, first_name, last_name, email, country
from sales.customers
where active = 1
```

#### Example: Run a Synapse SQL script
```bruin-sql
/* @bruin
name: orders_summary
type: synapse.sql
@bruin */

create table temp_orders as
select
    order_id,
    order_date,
    customer_id,
    sum(quantity) as total_quantity,
    sum(price) as total_price
from sales.orders
group by order_id, order_date, customer_id;

create or replace view orders_summary as
select
    customer_id,
    count(order_id) as total_orders,
    sum(total_quantity) as total_quantity,
    sum(total_price) as total_price
from temp_orders
group by customer_id;
```

### `synapse.sensor.query`

Checks if a query returns any results in SQL Server, runs every 5 minutes until this query returns any results.

```yaml
name: string
type: string
parameters:
    query: string
```

**Parameters**:
- `query`: Query you expect to return any results

#### Example: Partitioned upstream table

Checks if the data available in upstream table for end date of the run.
```yaml
name: analytics_123456789.events
type: synapse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where dt = "{{ end_date }}"
```

#### Example: Streaming upstream table

Checks if there is any data after end timestamp, by assuming that older data is not appended to the table.
```yaml
name: analytics_123456789.events
type: synapse.sensor.query
parameters:
    query: select exists(select 1 from upstream_table where inserted_at > "{{ end_timestamp }}"
```

### `synapse.seed`
`synapse.seed` is a special type of asset used to represent CSV files that contain data that is prepared outside of your pipeline that will be loaded into your Synapse database. Bruin supports seed assets natively, allowing you to simply drop a CSV file in your pipeline and ensuring the data is loaded to the Synapse database.

You can define seed assets in a file ending with `.yaml`:
```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

**Parameters**:
- `path`:  The `path` parameter is the path to the CSV file that will be loaded into the data platform. path is relative to the asset definition file.


####  Examples: Load csv into a Synapse database

The examples below show how to load a CSV into a Synapse database.
```yaml
name: dashboard.hello
type: synapse.seed

parameters:
    path: seed.csv
```

Example CSV:

```csv
name,networking_through,position,contact_date
Y,LinkedIn,SDE,2024-01-01
B,LinkedIn,SDE 2,2024-01-01
```



## Platforms > Media


## Ingestion


### Adjust

# Adjust
[Adjust](https://www.adjust.com/) is a mobile marketing analytics platform that provides solutions for measuring and optimizing campaigns, as well as protecting user data.

Bruin supports Adjust as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Adjust into your data warehouse.

In order to set up Adjust connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dev.adjust.com/en/api/rs-api/authentication/).

Follow the steps below to correctly set up Adjust as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Adjust, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      adjust:
        - name: "my_adjust"
          api_key: "abc123"
```
- `api_key`: The API key for the Adjust account.
- `lookback_days`: Optional. The number of days to go back from the given start date for data. Defaults to 30 days. To know more about it, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/adjust.html#lookback-days)

### Step 2: Create an asset file for data ingestion

To ingest data from Adjust, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., adjust_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.adjust
type: ingestr
connection: postgres

parameters:
  source_connection: my_adjust
  source_table: 'creatives'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Adjust connection defined in .bruin.yml.
- `source_table`: The name of the data table in Adjust that you want to ingest. For example, `creatives` is the table of Adjust that you want to ingest. You can find the available source tables in Adjust [here](https://bruin-data.github.io/ingestr/supported-sources/adjust.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/adjust_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Adjust table into your Postgres database.


### Airtable

# Airtable
[Airtable](https://www.airtable.com/) is a cloud-based platform that combines spreadsheet and database functionalities, designed for data management and collaboration.

Bruin supports Airtable as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Airtable into your data warehouse.

In order to set up Airtable connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `base_id` and `access_token`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/airtable#setup-guide).

Follow the steps below to correctly set up Airtable as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Airtable, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      airtable:
        - name: "connection_name"
          base_id: "id123"
          access_token: "key123"
```

- `base_id`: A unique identifier for an Airtable base.
- `access_token`: A personal access token for authentication with the Airtable API.

### Step 2: Create an asset file for data ingestion

To ingest data from Airtable, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., airtable_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.airtable
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'details'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the airtable connection defined in .bruin.yml.
- `source_table`: The name of the data table in airtable that you want to ingest. For example, `details` is the table of airtable that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/airtable_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Airtable table into your Postgres database.

<img width="1108" alt="airtable" src="https://github.com/user-attachments/assets/56db541a-7c3a-41c5-82fe-8db03b2f4b21">




### Applovin

# Applovin
[AppLovin](https://www.applovin.com/) is a mobile technology company that allows developers of all sizes to market, monetize, analyze, and publish their apps through its mobile advertising, marketing, and analytics platforms — MAX, AppDiscovery, and SparkLabs.

Bruin supports AppLovin as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from AppLovin into your data platform.

To set up an AppLovin connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_key` that is report key. You can generate a report key from your [analytics dashboard](https://dash.applovin.com/login#keys).

Follow the steps below to set up AppLovin correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
      applovin:
        - name: "applovin"
          api_key: "api_key_123"
```
- `api_key` (required): It is the `report key` which is used for authenticating the request.

### Step 2: Create an asset file for data ingestion
To ingest data from AppLovin, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., applovin_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.applovin
type: ingestr
connection: postgres

parameters:
  source_connection: applovin
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For AppLovin, it will be always `ingestr`.
- `source_connection`: The name of the AppLovin connection defined in `.bruin.yml`.
- `source_table`: The name of the table in AppLovin to ingest. You can find the available source tables [here](https://bruin-data.github.io/ingestr/supported-sources/applovin.html#tables).
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/applovin_asset.yml
```
As a result of this command, Bruin will ingest data from the given AppLovin table into your Postgres database.








### Applovin_Max

# Applovin Max
[AppLovin Max](https://www.applovin.com/max/) is a tool from AppLovin that helps app developers optimize ad revenue by selecting the highest-paying ads from various ad networks.

Bruin supports AppLovin Max as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from AppLovin Max into your data platform.

To set up an AppLovin Max connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_key` that is report key and `application`. For details on how to obtain these credentials, please refer [here](https://developers.applovin.com/en/max/max-dashboard/account/account-info/#keys)

Follow the steps below to set up AppLovin Max correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up AppLovin Max connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      applovinmax:
        - name: "my-applovinmax"
          api_key: "api_key_123"
```
- `api_key` (required): It is the `report key` which is used for authenticating the request.

### Step 2: Create an asset file for data ingestion
To ingest data from AppLovin Max, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., applovinmax_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: my.applovinmax
type: ingestr

parameters:
  source_connection: "my-applovinmax"
  source_table: 'user_ad_revenue:com.example.app1'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For AppLovin Max, it will be always `ingestr`.
- `source_connection`: The name of the AppLovin Max connection defined in `.bruin.yml`.
- `source_table`: The name of the table in AppLovin Max to ingest. Currently, we only support [user_ad_revenue](https://developers.applovin.com/en/max/reporting-apis/user-level-ad-revenue-api/). The format is `<table_name>:<application_ids>` where `application_ids` is a comma-separated list of application IDs.
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/applovinmax_asset.yml
```
As a result of this command, Bruin will ingest data from the given AppLovin Max table into your Postgres database.


<img alt="applovinmax" src="./media/applovinmax.png">







### Appsflyer

# Appsflyer
[Appsflyer](https://www.appsflyer.com/) is a mobile marketing analytics and attribution platform that helps businesses track, measure, and optimize their app marketing efforts across various channels.

Bruin supports Appsflyer as a source, and you can use it to ingest data from Appsflyer into your data warehouse.

In order to set up an Appsflyer connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema. For more information on how to get these credentials check the Appsflyer section in [Ingestr documentation](https://bruin-data.github.io/ingestr/getting-started/quickstart.html)


```yaml
    connections:
      appsflyer:
        - name: "connection_name"
          api_key: "abc123"
```


### Appstore

# Apple AppStore
The [AppStore](https://appstore.com/) is an app marketplace developed and maintained by Apple, for mobile apps on its iOS and iPadOS operating systems. The store allows users to browse and download approved apps developed within Apple's iOS SDK. Apps can be downloaded on the iPhone, iPod Touch, or iPad, and some can be transferred to the Apple Watch smartwatch or 4th-generation or newer Apple TVs as extensions of iPhone apps.

Bruin supports AppStore as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest analytics, sales and performance data into your data warehouse.

In order to set up AppStore connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up AppStore as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to AppStore, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      appstore:
        - name: "my-appstore"
          key_id: "my-key-id"
          issuer_id: "my-issuer-id"
          key_path: /path/to/private/key

          # alternatively, you can specify the key inline
          key: |
            -----BEGIN PRIVATE KEY-----
            (example key. Your key will have a bunch of encoded data here)
            -----END PRIVATE KEY-----
          
```

* `key_id`: ID of the key.
* `issuer_id`: Issuer ID of the key.
* `key_path`: Path to the private key.
* `key`: encoded contents of the private key. Be sure to use `|` yaml scalar when specifying the key inline.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/appstore.html#setting-up-appstore-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from AppStore, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., appstore_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.appstore
type: ingestr
connection: postgres

parameters:
  source_connection: my-appstore
  source_table: 'app-downloads-detailed:123'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for AppStore.
- connection: This is the destination connection.
- source_connection: The name of the AppStore connection defined in .bruin.yml.
- source_table: The name of the table in AppStore Connect API that you want to ingest. The table is of the form `<kind>:<app_id>[,<app_id>]`. Where `kind` is a report type and `<app_id>` is the ID of your app. Multiple App IDs can be specified by delimiting them with a comma.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/appstore_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given AppStore report into your Postgres database.


### Asana

# Asana
[Asana](https://asana.com/) is a software-as-a-service platform designed for team collaboration and work management. Teams can create projects, assign tasks, set deadlines, and communicate directly within Asana. It also includes reporting tools, file attachments, calendars, and goal tracking.


Bruin supports Asana as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Asana into your data warehouse.

In order to set up Asana connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up Asana as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Asana, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      asana:
        - name: "my-asana"
          workspace: "YOUR_WORKSPACE_ID"
          access_token: "YOUR_ACCESS_TOKEN"
          
```

* `workspace`: is the `gid` of your workspace.
* `access_token`: is a personal access token.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/asana.html#uri-format).

### Step 2: Create an asset file for data ingestion

To ingest data from Asana, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., asana_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.asana
type: ingestr
connection: postgres

parameters:
  source_connection: my-asana
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Asana.
- connection: This is the destination connection.
- source_connection: The name of the Asana connection defined in .bruin.yml.
- source_table: The name of the data table in Asana you want to ingest. 

You can find a list of supported tables [here](https://bruin-data.github.io/ingestr/supported-sources/asana.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/asana_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given Asana table into your Postgres database.


### Athena

# Athena
[Athena](https://aws.amazon.com/athena/) is an interactive query service that allows users to analyze data directly in Amazon S3 using standard SQL.

The Athena destination stores data as Parquet files in S3 buckets and creates external tables in AWS Glue Catalog.

Bruin supports Athena as a destination, and you can use it to ingest data to Athena.

In order to set up Athena connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `query_results_path`, `access_key_id`, `secret_access_key` and `region` to access the S3 bucket. Please follow the guide to obtain [credentials](https://dlthub.com/docs/dlt-ecosystem/destinations/athena#2-setup-bucket-storage-and-athena-credentials). Once you've completed the guide, you should have all the above-mentioned credentials.

Follow the steps below to correctly set up Athena as a destination.
### Step 1: Add a connection to .bruin.yml file

To connect to Athena, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
      connections:
        athena:
          - name: athena
            access_key_id: "access_key_123"
            secret_access_key: "secret_key_123"
            query_results_path: "destbucket"
            region: "eu-central-1"
```
- `access_key_id and secret_access_key`: These are AWS credentials that will be used to authenticate with AWS services like S3 and Athena.
- `region`: The AWS region of the Athena service and S3 buckets, e.g. eu-central-1
- `query_results_path`: The query location path where the results of Athena queries will be saved, e.g. dest_path or s3://dest_path.



### Step 2: Create an asset file

To ingest data to  Athena, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., chess_athena.yml) inside the assets folder and add the following content:

```yaml
name: public.chess
type: ingestr
connection: athena

parameters:
  source_connection: my-chess
  source_table: 'profiles'

  destination: athena
```

- `connection`: This is the destination connection, which defines where the data should be stored.

### Step 3: [Run](/commands/run) asset
```     
bruin run assets/chess_athena.yml
```
As a result of this command, Bruin will copy the data from the Chess source into Athena.



### Attio

# Attio
[Attio](https://attio.com/) is an AI-native CRM platform that helps companies build, scale, and grow their business.

Bruin supports Attio as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Attio into your data warehouse.

In order to set up Attio connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Attio as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Attio, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  Attio:
    - name: "attio"
      api_key: "key_123"
```
- `api_key`: the API key used for authentication with the Attio API

### Step 2: Create an asset file for data ingestion
To ingest data from Attio, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., Attio_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.attio
type: ingestr
connection: postgres

parameters:
  source_connection: attio
  source_table: 'objects'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Attio connection defined in .bruin.yml.
- `source_table`: The name of the data table in Attio that you want to ingest.
  You can find the available source tables in Attio [here](https://bruin-data.github.io/ingestr/supported-sources/attio.html#tables)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/attio_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Attio table into your Postgres database.

<img alt="Pipedrive" src="./media/attio_ingestion.png">




### Chess

# Chess
[chess](https://www.chess.com/) is an online platform offering chess games, tournaments, lessons, and more.

Bruin supports Chess as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Chess into your data warehouse.

In order to set up Chess connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Chess as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Chess, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      chess:
        - name: "my-chess"
          players:
            - "erik"
            - "vadimer2"
```
- `players`: A list of players usernames for which you want to fetch data.

### Step 2: Create an asset file for data ingestion

To ingest data from Chess, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., chess_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.chess
type: ingestr
connection: postgres

parameters:
  source_connection: my-chess
  source_table: 'profiles'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Chess connection defined in .bruin.yml.
- `source_table`: The name of the data table in Chess that you want to ingest. For example, `profiles` is the table of Chess that you want to ingest. You can find the available source tables in Chess [here](https://bruin-data.github.io/ingestr/supported-sources/chess.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/chess_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Chess table into your Postgres database.

<img width="1161" alt="chess" src="https://github.com/user-attachments/assets/12418c5b-5483-46fb-9bb3-998e112d8030">



### Db2

# IBM Db2
[IBM Db2](https://www.ibm.com/db2) is a high-performance, enterprise-grade relational database system designed for reliability, scalability, and transactional integrity.


Bruin supports DB2 as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from DB2 into your data warehouse.

In order to set up DB2 connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up DB2 as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to DB2, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  db2:
    - name: "db2"
      username: "user_123"
      password: "pass_123"
      host: "localhost"
      port: 50000
      database: "testdb"
```
- `username`: The username to connect to the database
- `password`: The password for the user
- `host`: The host address of the database server
- `port`: The port number the database server is listening
- `database`: the name of the database to connect to

### Step 2: Create an asset file for data ingestion
To ingest data from DB2, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., db2_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.db1
type: ingestr
connection: neon

parameters:
  source_connection: db2
  source_table: 'test.user'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the DB2 connection defined in .bruin.yml.
- `source_table`: The name of the data table in DB2 that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/db2_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given DB2 table into your Postgres database.

<img alt="applovinmax" src="./media/db2.png">



### Destinations

# Destinations

Bruin supports all the data platforms mentioned in the Data Platforms section as destinations for your ingested data. This means you can configure Bruin to load data into these platforms using [Ingestr Assets](../assets/ingestr.md).


## Supported Platforms

<script setup>
import { withBase } from 'vitepress'
import { useSidebar } from 'vitepress/theme'

const { sidebarGroups } = useSidebar()

const platformsGroup = sidebarGroups.value.find(group => group.text === 'Data Platforms')
</script>

<div v-if="platformsGroup && platformsGroup.items.length > 0">


<ul>
<li v-for="platform in platformsGroup.items" :key="platform">
    <a :href="withBase(platform.link)">{{ platform.text }}</a>
</li>
</ul>
</div>



### Dynamodb

# DynamoDB
Amazon [DynamoDB](https://aws.amazon.com/dynamodb/) is a managed NoSQL database service provided by Amazon Web Services (AWS). It supports key-value and document data structures and is designed to handle a wide range of applications requiring scalability and performance. 


Bruin supports DynamoDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from DynamoDB into your data warehouse.

In order to set up DynamoDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up DynamoDB as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to DynamoDB, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      dynamodb:
        - name: "my-dynamodb"
          access_key_id: "AWS_ACCESS_KEY_ID"
          secret_access_key: "AWS_SECRET_ACCESS_KEY"
          region: "AWS_REGION"
          
```

* `access_key_id`: Identifies an IAM account.
* `secret_access_key`: Password for the IAM account.
* `region`: AWS region in which your DynamoDB table exists.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/dynamodb.html#setting-up-a-dynamodb-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from DynamoDB, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., dynamodb_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.dynamodb
type: ingestr
connection: postgres

parameters:
  source_connection: my-dynamodb
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for DynamoDB.
- connection: This is the destination connection.
- source_connection: The name of the DynamoDB connection defined in .bruin.yml.
- source_table: The name of the table in DynamoDB you want to ingest. 

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/dynamodb_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given DynamoDB table into your Postgres database.


### Elasticsearch

# Elasticsearch
[Elasticsearch](https://www.elastic.co/elasticsearch) is a distributed, RESTful search and analytics engine designed for fast and scalable data retrieval and storage.

Bruin supports Elasticsearch as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Elasticsearch into your data warehouse.

In order to set up Elasticsearch connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Elasticsearch as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Elasticsearch, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  elasticsearch:
    - name: "elasticsearch"
      username: "username_123"
      password: "pass_123"
      host: "localhost"
      port: 5000
      secure: "false"
      verify_certs: "false"
```
- `username`: The username used to authenticate with Elasticsearch.
- `password`: The password associated with the specified username.
- `host`: The host address of the Elasticsearch server.
- `port`: The port number used by the Elasticsearch server.
-  `secure`: Enables HTTPS when set to true. By default, it is true.
- `verify_certs`: Verifies TLS certificates when set to true. By default, it is true.

### Step 2: Create an asset file for data ingestion
To ingest data from Elasticsearch, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., Elasticsearch_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.elasticsearch
type: ingestr
connection: postgres

parameters:
  source_connection: elasticsearch
  source_table: 'index-name'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Elasticsearch connection defined in .bruin.yml.
- `source_table`: The name of the Elasticsearch index from which you want to ingest data.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/elasticsearch_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Elasticsearch table into your Postgres database.



### Facebook Ads

# Facebook Ads
Facebook Ads is the advertising platform that helps users to create targeted ads on Facebook, Instagram and Messenger.

Bruin supports Facebook Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Facebook Ads into your data warehouse.

In order to set up Facebook Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `access_token` and `account_id`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/facebook_ads#grab-credentials)

Follow the steps below to correctly set up Facebook Ads as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Facebook Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  facebookads:
    - name: "my_facebookads"
      access_token: "YOUR_FACEBOOK_ACCESS_TOKEN"
      account_id: "YOUR_ACCOUNT_ID"
```
- `access_token` is associated with Business Facebook App.
- `account_id`  is associated with Ad manager.

### Step 2: Create an asset file for data ingestion
To ingest data from Facebook Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., facebook_ads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.facebookads
type: ingestr
connection: postgres

parameters:
  source_connection: my_facebookads
  source_table: 'ads'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the  Facebook Ads connection defined in .bruin.yml.
- `source_table`: The name of the data table in Facebook Ads you want to ingest. For example, `ads` would ingest data related to ads. You can find the available source tables in Facebook Ads [here](https://bruin-data.github.io/ingestr/supported-sources/facebook-ads.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/facebook_ads_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Facebook Ads table into your Postgres database.


<img width="962" alt="facebooksads" src="https://github.com/user-attachments/assets/7476fb59-8885-4c76-95d4-f150cac2d423">




### Frankfurter

# Frankfurter
[Frankfurter](https://www.frankfurter.app/) is a free API for current and historical foreign exchange rates.

Bruin supports Frankfurter as a source for ingestr assets, allowing you to ingest exchange rate data directly into your data warehouse.

To set up Frankfurter as a data source and perform data ingestion, follow the instructions provided below. 

### Step 1: Add a connection to .bruin.yml file

Add or modify the `.bruin.yml` file as follows:

```yaml
    connections:
      frankfurter:
        - name: frankfurter
```

### Step 2: Create an asset file for data ingestion

To ingest data from Frankfurter, create an `ingestr` asset with the file `assets/frankfurter.asset.yml`. This file defines the data flow from the source to the destination.

```yaml
name: dataset.frankfurter
type: ingestr
connection: duckdb-default

parameters:
  source_connection: frankfurter
  source_table: 'latest'

  destination: duckdb

```

- `name`: The name of the asset. This must be unique within the pipeline. 
- `type`: Specifies the type of the asset. As Frankfurter is an `ingestr` asset, this should be set to `ingestr`. 
- `connection`: The destination connection where the data will be stored. Here `duckdb-default` refers to the database defined in `.bruin.yml`.
- `source_connection`: The name of the Frankfurter connection defined in `.bruin.yml`.
- `source_table`: The name of the Frankfurter table you want to ingest. Frankfurter provides the following tables:
    - `latest`: Contains the latest exchange rates for all supported currencies.
    - `exchange_rates`: Includes historical exchange rate data, allowing you to query rates across a defined date range.
    - `currencies`: Provides a list of all supported currencies available in the Frankfurter API.

### Step 3: [Run](/commands/run) asset to ingest data

Navigate to your pipeline folder and run the following command to ingest data from Frankfurter into your data warehouse:

```bash
bruin run assets/frankfurter.asset.yml
```

### Step 4: Query Your Data in DuckDB
Now that the data is in your database, you can query it to verify the results. Open a terminal and run the following commands to inspect your data:

```bash
bruin query --c duckdb-default  --q "SELECT DATE, CURRENCY_NAME, RATE FROM dataset.frankfurter LIMIT 10;" 
```
After executing the query, you will see the following results displayed in your terminal in a clear and organized table format:
```plaintext
┌────────────┬───────────────┬─────────┐
│ DATE       │ CURRENCY_NAME │ RATE    │
├────────────┼───────────────┼─────────┤
│ 2025-04-11 │ EUR           │ 1       │
│ 2025-04-11 │ AUD           │ 1.8201  │
│ 2025-04-11 │ BGN           │ 1.9558  │
│ 2025-04-11 │ BRL           │ 6.6159  │
│ 2025-04-11 │ CAD           │ 1.5736  │
│ 2025-04-11 │ CHF           │ 0.9252  │
│ 2025-04-11 │ CNY           │ 8.2819  │
│ 2025-04-11 │ CZK           │ 25.147  │
│ 2025-04-11 │ DKK           │ 7.4675  │
│ 2025-04-11 │ GBP           │ 0.86678 │
└────────────┴───────────────┴─────────┘
```


### Gcs

# Google Cloud Storage 

[Google Cloud Storage](https://cloud.google.com/storage?hl=en) (GCS) is an online file storage web service for storing and accessing data on Google Cloud Platform infrastructure. The service combines the performance and scalability of Google's cloud with advanced security and sharing capabilities. It is an Infrastructure as a Service (IaaS), comparable to Amazon S3. 


Bruin supports GCS as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from GCS into your data warehouse.

In order to set up the GCS connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You will need the `service_account_file` or `service_account_json`. For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#setting-up-a-gcs-integration).

Follow the steps below to correctly set up GCS as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to GCS, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      gcs:
          # name of your connection
        - name: "my-gcs"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"
          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself

### Step 2: Create an asset file for data ingestion

To ingest data from GCS, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gcs_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gcs
type: ingestr
connection: postgres

parameters:
  source_connection: my-gcs
  source_table: 'my-bucket/students_details.csv'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the gcs connection defined in .bruin.yml.
- `source_table`: bucket name and file path (or [file glob](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#file-pattern)) separated by a forward slash (`/`).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/gcs_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given gcs bucket into your Postgres database.



### Github

# GitHub
GitHub is a developer platform that allows developers to create, store, manage and share their code.

Bruin supports GitHub as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from GitHub into your data platform.

In order to set up GitHub connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. If you want to fetch from private repositories, you need to provide `access_token`, `owner` and `repo`.For details on how to obtain these credentials and to grant necessary permissions, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/github#setup-guide). If you want to fetch from public repositories, you only need to provide `owner` and `repo`.

Follow the steps below to correctly set up GitHub as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to GitHub as a source, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  github:
    - name: my-github
      access_token: "token_123"
      owner: "John"
      repo: "repo_123"
```
- `access_token` (optional): Used for authentication to access data through the GitHub API for private repositories. 
- `owner` (required): The owner of the repository.
- `repo` (required): The name of the repository.

### Step 2: Create an asset file for data ingestion
To ingest data from GitHub, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., github_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.github
type: ingestr
connection: postgres

parameters:
  source_connection: my-github
  source_table: 'issues'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the GitHub connection defined in .bruin.yml.
- `source_table`: The name of the table in GitHub you want to ingest. You can find the available source tables in GitHub [here](https://bruin-data.github.io/ingestr/supported-sources/github.html#tables).


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/github_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given GitHub table into your Postgres database.


<img alt="github" src="./media/github.png">






### Google Ads

# Google Ads

[Google Ads](https://ads.google.com/), formerly known as Google Adwords, is an online advertising platform developed by Google, where advertisers bid to display brief advertisements, service offerings, product listings, and videos to web users. It can place ads in the results of search engines like Google Search (the Google Search Network), mobile apps, videos, and on non-search websites.

Bruin supports Google Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Ads into your data warehouse.

In order to set up Google Ads connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up Google Ads as a data source and run ingestion:


### Step 1: Add a connection to .bruin.yml file

To connect to Google Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      googleads:
        - name: "my-googleads"
          customer_id: "YOUR_GOOGLE_ADS_CUSTOMER_ID"
          dev_token: "YOUR_DEVELOPER_TOKEN"
          service_account_file: "/path/to/service-account.json"

          # alternatively, you can specify the service account json directly
          service_account_json: |
          {
            "type": "service_account",
            ...
          }
          
```

- `customer_id`: The account ID of your google ads account.
- `dev_token`: [Developer Token](https://developers.google.com/google-ads/api/docs/get-started/dev-token) for your application. 
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself


For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#setting-up-a-google-ads-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from Google Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., google_ads_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.campaigns
type: ingestr
connection: postgres

parameters:
  source_connection: my-googleads
  source_table: 'campaign_report_daily'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Google Ads.
- connection: This is the destination connection.
- source_connection: The name of the Google Ads connection defined in .bruin.yml.
- source_table: The name of the resource in Google Ads you want to ingest. 

You can find a list of supported tables [here](https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#tables).

You can also request a custom report by specifying the source table as `daily:{resource}:{dimensions}:{metrics}`. See [ingestr googleads docs](https://bruin-data.github.io/ingestr/supported-sources/google-ads.html#custom-reports) for more information.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/google_ads_integration.asset.yml
```
As a result of this command, Bruin will ingest data for the given Google Ads resource into your Postgres database.


### Google_Analytics

# Google Analytics
[Google Analytics](https://marketingplatform.google.com/about/analytics/) is an analytics service that helps you measure traffic and engagement across various platforms, like websites and apps.

ingestr supports Google Analytics as a source.

Bruin supports Google Analytics as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Analytics into your data warehouse.

To set up a Google Analytics connection, you need to add a configuration item in the `.bruin.yml` file and the `asset` file. You will need path to `service_account_file` and `property_id`.

Follow the steps below to correctly set up Google Analytics as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
```yaml
    googleanalytics:
        - name: "googleanalytics"
          service_account_file: "/path/to/service/account.json"
          property_id: "1234"
```
- `service_account_file`: The path to the service account JSON file
- `property_id`: It is a unique number that identifies a particular property on Google Analytics. Follow this guide to know about [property ID](https://developers.google.com/analytics/devguides/reporting/data/v1/property-id#what_is_my_property_id).

### Step 2: Create an asset file for data ingestion
To ingest data from Google Analytics, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gAnalytics_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.googleanalyticss
type: ingestr
connection: postgres

parameters:
  source_connection: googleanalytics
  source_table: 'custom:city,date:activeUsers,newUsers'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Google Analytics, it will be always `ingestr`.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Google Analytics connection defined in .bruin.yml.
- `source_table`: The name of the data table in Google Analytics to ingest data from.
Currently, only one table is supported: custom, which refers to Custom Reports. These allow you to retrieve data based on specific dimensions and metrics.
For more information, please visit [here](https://github.com/bruin-data/ingestr/blob/main/docs/supported-sources/google_analytics)
- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gAnalytics_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Google Analytics table into your Postgres database.

<img alt="GoogleAnalytics" src="./media/google_analytics.png">



### Google_Sheets

# Google Sheets
[Google Sheets](https://www.google.com/sheets/about/) is a web-based spreadsheet program that is part of Google's free, web-based Google Docs Editors suite.

Bruin supports Google Sheets as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Google Sheets into your data warehouse.

To set up a Google Sheets connection, you need to add a configuration item in the `.bruin.yml` file and the `asset` file. You will need either the `service_account_file` or the `service_account_json`. For more information, please follow the [guide](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_sheets#google-service-account-credentials). Once you complete the guide, you should have a `service account JSON` file.

Follow the steps below to correctly set up Google Sheets as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
```yaml
    connections:
      google_sheets:
        - name: "my-gsheets"
          # you can either specify a path to the service account file
          service_account_file: "path/to/file.json"

          # or you can specify the service account json directly
          service_account_json: |
            {
              "type": "service_account",
              ...
            }
```
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself


### Step 2: Create an asset file for data ingestion
To ingest data from Google Sheets, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gsheets_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gsheets
type: ingestr
connection: postgres

parameters:
  source_connection: my-gsheets
  source_table: '16UY6EQ_6jkdUdasdNfUq2CA.Sheet1'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Google Sheets connection defined in .bruin.yml.
- `source_table`: The name of the data table in Google Sheets to ingest. For example, if the `spreadsheet URL` is https\://docs.google.com/spreadsheets/d/1VTtCiw7UM1sadasdfas/edit?usp=sharing, the `spreadsheet ID` is 1VTtCiw7UM1sadasdfas. If the `sheet name` is Sheet1, the `source_table` will be `1VTtCiw7UM1sadasdfas.Sheet1`

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gsheets_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Google Sheets table into your Postgres database.

<img width="1140" alt="google_sheets" src="https://github.com/user-attachments/assets/8ee4e055-15e8-4439-a94c-26e124bfd5a7">



### Gorgias

# Gorgias

[Gorgias](https://gorgias.com) is a helpdesk for e-commerce merchants, providing customer service via email, social media, SMS, and live chat.

Bruin supports Gorgias as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Gorgias into your data warehouse.

In order to set up Gorgias connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Gorgias as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to Gorgias, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      gorgias:
        - name: "my-gorgias"
          domain: "my-shop"
          email: "myemail@domain.com"
          api_key: "abc123"
```
- `domain`: the domain of the Gorgias account without the full gorgias.com, e.g. mycompany
- `api_key`: the integration token used for authentication with the Gorgias API
- `email`: the email address of the user to connect to the Gorgias API

### Step 2: Create an asset file for data ingestion

To ingest data from Gorgias, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gorgias_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.gorgias
type: ingestr
connection: postgres

parameters:
  source_connection: my-gorgias
  source_table: 'customers'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Gorgias connection defined in .bruin.yml.
- `source_table`: The name of the data table in Gorgias that you want to ingest. For example, `customers` is the table of Gorgias that you want to ingest. You can find the available source tables in Gorgias [here](https://bruin-data.github.io/ingestr/supported-sources/gorgias.html#supported-entities)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gorgias_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Gorgias table into your Postgres database.


### Hubspot

# HubSpot
[HubSpot](https://www.hubspot.com/) is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from HubSpot into your data warehouse.

In order to set up HubSpot connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You will need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/hubspot#setup-guide).

Follow the steps below to correctly set up HubSpot as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to HubSpot, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      hubspot:
        - name: "my-hubspot"
          api_key: "pat-123"
```
- `name`: The name of the connection
- `api_key`: The API key is used for authentication with the HubSpot API

### Step 2: Create an asset file for data ingestion

To ingest data from HubSpot, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubspot_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hubspot
type: ingestr
connection: postgres

parameters:
  source_connection: my-hubspot
  source_table: 'companies'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the hubspot connection defined in .bruin.yml.
- `source_table`: The name of the data table in hubspot that you want to ingest. For example, `companies` is a data table in hubspot that you may want to ingest. You can find the available source tables in hubspot [here](https://bruin-data.github.io/ingestr/supported-sources/hubspot.html#tables).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/hubspot_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given HubSpot table into your Postgres database.

<img width="1124" alt="hubspot" src="https://github.com/user-attachments/assets/c88f2781-1e78-4d5b-8cb1-60b7993ea674">







### Kafka

# Kafka
[Kafka](https://kafka.apache.org/) is a distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

Bruin supports Kafka as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Kafka into your data warehouse.

In order to set up a Kafka connection, you need to add a configuration item to connections in the .bruin.yml file, following the schema below. The required fields include `bootstrap_servers` and `group_id`. The SASL-related fields (`security_protocol`, `sasl_mechanisms`, `sasl_username`, `sasl_password`) as well as `batch_size` and `batch_timeout` are optional, depending on your Kafka setup. For more information on Kafka, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/kafka.html)

Follow the steps below to correctly set up Kafka as a data source and run ingestion:
### Step 1: Add a connection to .bruin.yml file

To connect to Kafka, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      kafka:
        - name: "my_kafka"
          bootstrap_servers: "localhost:9093"
          group_id: "test123"
```
- `bootstrap_servers`: The Kafka server or servers to connect to, typically in the form of a host and port.
- `group_id` : The consumer group ID used for identifying the client when consuming messages.


### Step 2: Create an asset file for data ingestion

To ingest data from Kafka, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., kafka_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.kafka
type: ingestr
connection: postgres

parameters:
  source_connection: my_kafka
  source_table: 'kafka.my_topic'
  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Kafka.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Kafka connection defined in .bruin.yml.
- `source_table`: The name of the data table in kafka you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/kafka_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Kafka table into your Postgres database.


### Kinesis

# Amazon Kinesis
[Amazon Kinesis](https://aws.amazon.com/kinesis/) is a cloud-based service for real-time data streaming and analytics, enabling the processing and analysis of large streams of data in real time.

Bruin supports Kinesis as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Kinesis into your data warehouse.

To set up a Kinesis connection, you need to have `access_key_id`, `secret_access_key` and `region`. For more information, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/amazon_kinesis#grab-credentials)

Follow the steps below to correctly set up Kinesis as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Kinesis, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      kinesis:
        - name: "kinesis"
          access_key_id: "access_key_id_123"
          secret_access_key: "secret_access_key_123"
          region: "eu-central-1"
```
- `access_key_id`: The access key ID used for authentication with the Kinesis API.
- `secret_access_key`: The secret access key used for authentication with the Kinesis API.
- `region`: The AWS region name where the stream is located.

### Step 2: Create an asset file for data ingestion

To ingest data from Kinesis, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., kinesis_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.kinesis
type: ingestr
connection: postgres

parameters:
  source_connection: kinesis
  source_table: 'test_stream'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always `ingestr` type for Kinesis.
- `connection`: This is the destination connection name.
- `source_connection`: The name of the Kinesis connection defined in .bruin.yml.
- `source_table`: Specify the `stream name` you want to read from. For example, if you want to read from a Kinesis stream named "customer_events", you would use source_table as customer_events.
-  `destination`: The type of data warehouse where you want to store your data. For example: Postgres, Clickhouse, Snowflake.


### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run ingestr.kinesis.asset.yml
```
As a result of this command, Bruin will ingest data from the given Kinesis table into your Postgres database.

<img alt="kinesis" src="./media/kinesis-ingestion.png">


### Klaviyo

# Klaviyo
[Klaviyo](https://www.Klaviyo.com/) is a marketing automation platform that helps businesses build and manage digital relationships with their customers by connecting through personalized email and enhancing customer loyalty.

Bruin supports Klaviyo as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Klaviyo into your data warehouse.

To set up a Klaviyo connection, you need to have Klaviyo API key and source table. For more information, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html)

Follow the steps below to correctly set up Klaviyo as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Klaviyo, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      klaviyo:
        - name: "my_klaviyo"
          api_key: "YOUR_KLAVIYO_API_KEY"
```
- `api_key`: The API key used for authentication with the Klaviyo API.

### Step 2: Create an asset file for data ingestion

To ingest data from Klaviyo, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., klaviyo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.klaviyo
type: ingestr
connection: postgres

parameters:
  source_connection: my_klaviyo
  source_table: 'events'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Klaviyo.
- `connection`: This is the destination connection. 
- `source_connection`: The name of the Klaviyo connection defined in .bruin.yml.
- `source_table`: The name of the data table in klaviyo you want to ingest. For example, `events` would ingest data related to events. You can find the available source tables in Klaviyo [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html#available-tables).


### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run ingestr.klaviyo.asset.yml
```
As a result of this command, Bruin will ingest data from the given Klaviyo table into your Postgres database.


### Linkedinads

# LinkedIn Ads
LinkedIn Ads is an advertising platform that allows businesses and marketers to create, manage, and analyze advertising campaigns.

Bruin supports LinkedIn Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from LinkedIn Ads into your data platform.

To set up a LinkedIn Ads connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `access_token` and `account_ids`. For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/linkedin_ads.html#create-a-linkedin-developer-application-to-obtain-an-access-token)

Follow the steps below to set up LinkedIn Ads correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up LinkedIn Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      linkedinads:
        - name: "my-linkedinads"
          account_ids: "id_123,id_456"
          access_token: "token_123"
```
- `access_token` (required): The access token is used for authentication and allows your app to access data based on the permissions configured in the Developer App for your LinkedIn account.
- `account_ids` (required): A comma-separated list of LinkedIn Ad Account IDs that identifies the accounts from which you want to retrieve data. 

### Step 2: Create an asset file for data ingestion
To ingest data from LinkedIn Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., linkedinads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.linkedinads
type: ingestr
connection: postgres

parameters:
  source_connection: my-linkedinads
  source_table: 'custom:campaign,month:clicks,impressions,shares'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline.
- `connection`: The name of the destination connection and it must match the name of the connection defined in the .`bruin.yml` file.
- `source_connection`: The name of the LinkedIn Ads connection defined in `.bruin.yml`.
- `source_table`: The name of the table in LinkedIn Ads to ingest. Currently, we only support custom reports based on specified dimensions and metrics.

Custom Table Format:
`custom:<dimensions>:<metrics>`

Parameters:
- `dimensions`(required): A comma-separated list of dimensions to retrieve. It must include at least one of the following: `campaign`, `account`, or `creative`, along with one time-based dimension, either `date` or `month`.

- `metrics`(required): A comma-separated list of [metrics](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting?view=li-lms-2024-11&tabs=http#metrics-available) to retrieve.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/linkedinads_asset.yml
```
As a result of this command, Bruin will ingest data from the given LinkedIn Ads table into your Postgres database.


<img alt="linkedinads" src="./media/linkedinads.png">







### Mongo

# MongoDB
MongoDB is a popular, open source NoSQL database known for its flexibility, scalability, and wide adoption in a variety of applications.

Bruin supports MongoDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from MongoDB into your data warehouse. 

In order to set up MongoDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up MongoDB as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to MongoDB, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:
```yaml
    connections:
      mongo:
        - name: "localMongo"
          username: "testUser"
          password: "testPass123"
          host: "localhost"
          port: 27018
          database: "testDB"
```
- `name`: The name to identify this MongoDB connection
- `username`: The MongoDB username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the MongoDB server (e.g., localhost or an IP address)
- `port`: The port number the database server is listening on (default: 27017)
- `database`:  The name of the database to connect to

### Step 2: Create an asset file for data ingestion

To ingest data from MongoDB, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., mongo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.mongo
type: ingestr
connection: postgres

parameters:
  source_connection: localMongo
  source_table: 'users.details'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the MongoDB connection defined in .bruin.yml.
- `source_table`: The name of the data table in MongoDB that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/mongo_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given MongoDB table into your Postgres database.

<img width="1017" alt="image" src="https://github.com/user-attachments/assets/2bad9131-baa1-4f20-8e3d-eed4329e7f90">





### Mysql

# MySQL
 MySQL is a widely used relational database management system (RDBMS) that is free and open-source, making it ideal for both small and large applications.

Bruin supports MySQL as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from MySQL into your data warehouse.

In order to set up MySQL connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up MySQL as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to MySQL, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
     mysql:
        - name: "new-mysql"
          username: "test_user"
          password: "test_password"
          host: "localhost"
          port: 3306
          database: "mysql"
          ssl_ca_path: "path/to/ca.pem"
          ssl_cert_path: "path/to/cert.pem"
          ssl_key_path: "path/to/key.pem"
```
- `name`: The name to identify this MySQL connection
- `username`: The MySQL username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the MySQL server (e.g., localhost or an IP address)
- `port`: The port number the database server is listening on (default: 3306)
- `database`:  The name of the database to connect to
- `driver`: (Optional) The name of the database driver to use
- `ssl_ca_path`: (Optional) The path to the CA certificate file
- `ssl_cert_path`: (Optional) The path to the client certificate file
- `ssl_key_path`: (Optional) The path to the client key file

### Step 2: Create an asset file for data ingestion
To ingest data from MySQL, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., mysql_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.mysql
type: ingestr
connection: postgres

parameters:
  source_connection: new-mysql
  source_table: 'mysql.user'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the MySQL connection defined in .bruin.yml.
- `source_table`: The name of the data table in MySQL that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/mysql_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given MySQL table into your Postgres database.

<img width="1081" alt="mysql" src="https://github.com/user-attachments/assets/efd0666c-3c9b-40b3-bfa9-bf9ed05620d7">



### Notion

# Notion
[Notion ](https://www.notion.so/) is an all-in-one workspace for note-taking, project management, and database management.

Bruin supports Notion as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Notion into your data warehouse.

In order to set up Notion connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/notion#setup-guide).

Follow the steps below to correctly set up Notion as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Notion, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      notion:
        - name: "my-notion"
          api_key: "YOUR_NOTION_API_KEY"
```

- `api_key`: the API key used for authentication with Notion
### Step 2: Create an asset file for data ingestion

To ingest data from Notion, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., notion_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.notion
type: ingestr
connection: postgres

parameters:
  source_connection: my_notion
  source_table: 'd8ee2d159ac34cfc85827ba5a0a8ae71'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Notion connection defined in .bruin.yml.
- `source_table`: The name of the data table in Notion that you want to ingest. Use the `database ID` as the source_table. For example, if the Notion URL is: https://www.notion.so/d8ee2d159ac34cfc85827ba5a0a8ae71?v=c714dec3742440cc91a8c38914f83b6b, the database ID is the string immediately following notion.so/ and preceding any question marks. In this example, the `database ID` is `d8ee2d159ac34cfc85827ba5a0a8ae71`.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/notion_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Notion table into your Postgres database.

<img width="1178" alt="notion" src="https://github.com/user-attachments/assets/347b9714-e6c2-4e82-ad4a-2cbea37953ab">




### Overview

# Data Ingestion

Bruin has built-in data ingestion capabilities thanks to [ingestr](https://github.com/bruin-data/ingestr). The basic idea is simple:
- you have data sources
- each source may have one or more tables/streams
  - e.g. for Shopify, you have customers, orders, products, each being separate tables.
- you want to load these to a destination data platform

[Ingestr](https://github.com/bruin-data/ingestr) abstracts away all of these in the concept of sources, destinations and tables.

Using Bruin, you can load data from any source into your data platforms as a regular asset.

## Definition Schema

Ingestr assets are defined in a simple YAML file:

```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: <source-connection-name>
  source_table: customers
  destination: bigquery
```

The interesting part is in the `parameters` list:
- `source_connection`: the [connection](../getting-started/concepts.md#connection) that defines the source platform
- `source_table`: the table name for that source on [ingestr](https://bruin-data.github.io/ingestr/supported-sources/shopify.html)
- `destination`: the destination you'd like to load the data on

Effectively, this asset will run `ingestr` in the background and load the data to your data warehouse.

## Examples
There are various combinations of sources and destinations, but below are a few examples for common scenarios.

### Load data from Postgres -> BigQuery
```yaml
name: raw.customers
type: ingestr
parameters:
  source_connection: my-postgres
  source_table: raw.customers
  destination: bigquery
```

### Shopify Orders -> Snowflake
```yaml
name: raw.orders
type: ingestr
parameters:
  source_connection: my-shopify
  source_table: orders
  destination: snowflake
```

### Kafka -> BigQuery
```yaml
name: raw.topic1
type: ingestr
parameters:
  source_connection: my-kafka
  source_table: topic1
  destination: bigquery
```




### Personio

# Personio
[Personio](https://personio.de/) is a human resources management software that helps businesses
streamline HR processes, including recruitment, employee data management, and payroll, in one
platform.

Bruin supports Personio as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Personio into your data platform.

To set up a Personio connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `client_id` and `client_secret`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/personio#grab-credentials)

Follow the steps below to set up Personio correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up Personio connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      personio:
        - name: "personio"
          client_id: "id_123"
          client_secret: "secret_123"    
```
- `client_id` (required): It is the `client_id` which is used for authenticating the request.
- `client_secret` (required): It is the `client_secret` which is used for authenticating the request.

### Step 2: Create an asset file for data ingestion
To ingest data from Personio, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., personio_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.personio
type: ingestr

parameters:
  source_connection: personio
  source_table: 'employees'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Personio, it will be always `ingestr`.
- `source_connection`: The name of the Personio connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Personio to ingest. Available tables are:
  - `employees` : Retrieves company employees details
  - `absences` : Retrieves absence periods for absences tracked in days
  - `absence_types` : Retrieves list of various types of employee absences
  - `attendances` : Retrieves attendance records for each employee
  - `projects` : Retrieves a list of all company projects
  - `document_categories` : Retrieves all document categories of the company
  - `employees_absences_balance` : Retrieves the absence balance for a specific employee
  - `custom_reports_list` : Retrieves metadata about existing custom reports in your Personio account, such as report name, report type, report date / timeframe.
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/personio_asset.yml
```
As a result of this command, Bruin will ingest data from the given Personio table into your Postgres database.


<img alt="personio" src="./media/personio.png">







### Phantombuster

# PhantomBuster
[PhantomBuster](https://phantombuster.com/) is a cloud-based data automation and web scraping platform that allows users to extract data from websites, automate actions.

Bruin supports PhantomBuster as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from PhantomBuster into your data warehouse.

In order to set up PhantomBuster connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up PhantomBuster as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to PhantomBuster, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  PhantomBuster:
    - name: "phantombuster"
      api_key: "key_123"
```
- `api_key`: the API key used for authentication with the PhantomBuster API

### Step 2: Create an asset file for data ingestion
To ingest data from PhantomBuster, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., PhantomBuster_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.phantombuster
type: ingestr
connection: postgres

parameters:
  source_connection: phantombuster
  source_table: 'completed_phantoms:<agent_id>'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the phantombuster connection defined in .bruin.yml.
- `source_table`: The name of the data table in PhantomBuster that you want to ingest.
  For now, we only support `completed_phantoms` tables followed by an agent ID. For example:
  `completed_phantoms:<agentid>`. An Agent ID is a unique identifier for a specific Phantom.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/phantombuster_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given PhantomBuster table into your Postgres database.




### Pipedrive

# Pipedrive
[Pipedrive](https://www.pipedrive.com/) is a cloud-based sales Customer Relationship Management (CRM) tool designed to help businesses manage leads and deals, track communication, and automate sales processes.

Bruin supports Pipedrive as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Pipedrive into your data platform.

To set up a Pipedrive connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `api_token`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/Pipedrive#grab-api-token).

Follow the steps below to set up Pipedrive correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
In order to set up Pipedrive connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. This configuration must comply with the following schema:

```yaml
connections:
      pipedrive:
        - name: "pipedrive"
          api_token: "token-123"   
```
- `api_token`: token used for authentication with the Pipedrive API

### Step 2: Create an asset file for data ingestion
To ingest data from Pipedrive, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., Pipedrive_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.pipedrive
type: ingestr

parameters:
  source_connection: pipedrive
  source_table: 'users'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Pipedrive, it will be always `ingestr`
- `source_connection`: The name of the Pipedrive connection defined in `.bruin.yml`
- `source_table`: The name of the table in Pipedrive to ingest. Available tables are:
  - `activities`: Refers to scheduled events or tasks associated with deals, contacts, or organizations
  - `organizations`: Refers to company or entity with which you have potential or existing business dealings.
  - `products`: Refers to items or services offered for sale that can be associated with deals
  - `deals`: Refers to potential sale or transaction that you can track through various stages
  - `users`: Refers to Individual with a unique login credential who can access and use the platform
  - `persons`: Refers individual contacts or leads that can be linked to sales deals
- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/Pipedrive_asset.yml
```
As a result of this command, Bruin will ingest data from the given Pipedrive table into your Postgres database.

********
<img alt="Pipedrive" src="./media/Pipedrive.png">







### S3

# S3
Amazon Simple Storage Service [S3](https://aws.amazon.com/s3/) is a service offered by Amazon Web Services (AWS) that provides object storage through a web service interface.Amazon S3 uses the same scalable storage infrastructure that Amazon.com uses to run its e-commerce network. Amazon S3 can store any type of object, which allows use cases like storage for Internet applications, backups, disaster recovery, data archives, data lakes for analytics, and hybrid cloud storage.

Bruin supports S3 via [Ingestr assets](/assets/ingestr), and you can use it to move data to and from your data warehouse.

## Reading data from S3

In order to set up the S3 connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file. You will need the `access_key_id` and `secret_access_key`. For details on how to obtain these credentials, please refer [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html).

Follow the steps below to correctly set up S3 as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to S3, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      s3:
        - name: "my-s3"
          access_key_id: "AKI_123"
          secret_access_key: "L6L_123"
```

- `access_key_id` and `secret_access_key`: Used for accessing S3 bucket.

### Step 2: Create an asset file for data ingestion

To ingest data from S3, you need to create an [asset configuration](/assets/ingestr.html#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., s3_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.s3
type: ingestr
connection: postgres

parameters:
  source_connection: my-s3
  source_table: 'mybucket/students/students_details.csv'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the S3 connection defined in .bruin.yml.
- `source_table`: the bucket name and file path (or [file glob](https://bruin-data.github.io/ingestr/supported-sources/gcs.html#file-pattern)) separated by a forward slash (`/`).

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/s3_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given S3 table into your Postgres database.

<img width="986" alt="S3" src="https://github.com/user-attachments/assets/eef6dbdd-0a59-4767-b1ea-c1de28273037">

## Writing data to S3
Bruin also allows you to move data from any supported source to S3 using Ingestr assets. This is useful for exporting processed data, creating backups, or sharing data.

Follow the steps below to correctly set up S3 as a data destination and run data exports.

### Step 1: Add a connection to .bruin.yml file

To write data to S3, you first need to configure an S3 connection in your `.bruin.yml` file. This connection will specify the destination bucket and credentials.

```yaml
    connections:
      s3: 
        - name: "my-s3-destination" 
          access_key_id: "YOUR_AWS_ACCESS_KEY_ID"
          secret_access_key: "YOUR_AWS_SECRET_ACCESS_KEY"
          bucket_name: "your-s3-bucket-name"
          path_to_file: "your/destination/prefix" 
```

- `name`: A unique name for this S3 connection.
- `access_key_id` and `secret_access_key`: AWS credentials for accessing the S3 bucket.
- `bucket_name`: The name of the S3 bucket where data will be written.
- `path_to_file`: A base path or prefix within the bucket where files will be stored. Files specified in the asset will be relative to this path. For example, if `path_to_file` is `exports/` and your asset writes `report.csv`, the full path will be `exports/report.csv` within the bucket.

### Step 2: Create an asset file for data export

Next, create an asset configuration file (e.g., `s3_export.asset.yml`) in your `assets` folder. This file defines the data flow from your source (e.g., a database table) to S3.

```yaml
name: public.results 
type: ingestr
connection: my-s3-destination

parameters:
  source_connection: postgres 
  source_table: 'public.students' 

  destination: s3 
```

- `name`: The name of the asset .
- `type`: Specifies the type of the asset. Set this to `ingestr`.
- `connection`: The name of the S3 connection (defined in `.bruin.yml`) to which data will be written. This is your destination connection.
- `source_connection`: The name of the Bruin connection for your source database.
- `source_table`: The fully qualified name of the table in your source database that you want to export.
- `destination`: Set to `s3` to indicate S3 as the destination type.

### Step 3: [Run](/commands/run) asset to export data
Finally, run the asset to export data from your database to S3:
```bash
bruin run assets/s3_export.yml
```
As a result of this command, Bruin will execute the Ingestr pipeline, reading data from the specified source table and writing it to the designated S3 location. The format of the output file will be parquet.

::: info OUTPUT
<div style="overflow:scroll">
<pre><b>Analyzed the pipeline &apos;pg-to-s3&apos; with 2 assets.</b>

<span style="color:#2A7BDE"><b>Pipeline: pg-to-s3 (.)</b></span>
<span style="color:#26A269">  No issues found</span>

<span style="color:#33D17A"><b>✓ Successfully validated 1 assets across 1 pipeline, all good.</b></span>

<b>Starting the pipeline execution...</b>
<span style="color:#A347BA">Running:  public.results</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt; Initiated the pipeline with the following:</span>
<span style="color:#A347BA">[public.results] &gt;&gt;   Source: postgresql / public.students</span>
<span style="color:#A347BA">[public.results] &gt;&gt;   Destination: s3 / public.results</span>
<span style="color:#A347BA">[public.results] &gt;&gt;   Incremental Strategy: replace</span>
<span style="color:#A347BA">[public.results] &gt;&gt;   Incremental Key: None</span>
<span style="color:#A347BA">[public.results] &gt;&gt;   Primary Key: None</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt; Starting the ingestion...</span>
<span style="color:#A347BA">[public.results] &gt;&gt; --- Extract ---</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Resources: 0/1 (0.0%) | Time: 0.00s | Rate: 0.00/s</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Memory usage: 236.23 MB (54.80%) | CPU usage: 0.00%</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt; --- Extract ---</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Files: 0/1 (0.0%) | Time: 0.00s | Rate: 0.00/s</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Items: 0  | Time: 0.00s | Rate: 0.00/s</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Memory usage: 239.30 MB (54.90%) | CPU usage: 0.00%</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt;  Normalize </span>
<span style="color:#A347BA">[public.results] &gt;&gt; Jobs: 0/1 (0.0%) | Time: 2.48s | Rate: 0.00/s</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Memory usage: 263.02 MB (55.00%) | CPU usage: 0.00%</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt;  Load </span>
<span style="color:#A347BA">[public.results] &gt;&gt; Jobs: 1/1 (100.0%) | Time: 2.99s | Rate: 0.33/s</span>
<span style="color:#A347BA">[public.results] &gt;&gt; Memory usage: 266.11 MB (55.00%) | CPU usage: 0.00%</span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">[public.results] &gt;&gt; Successfully finished loading data from &apos;postgresql&apos; to &apos;s3&apos; in 5.69 seconds </span>
<span style="color:#A347BA">[public.results] &gt;&gt; </span>
<span style="color:#A347BA">Finished: public.results (5.69s)</span>
</pre>
</div>
:::

## Writing to a S3-Compatible storage
Bruin supports writing data to any S3 Compatible storage like [minio](https://min.io/), Digital Ocean [spaces](https://www.digitalocean.com/products/spaces) or Cloudflare [R2](https://developers.cloudflare.com/r2/). 

To use an S3-compatible storage service, you need to configure the `endpoint_url` in your S3 connection settings within the `.bruin.yml` file. This URL should point to the API server of the S3-compatible storage service you are using.

For example, if you are using MinIO, your connection configuration might look like this:

```yaml
    connections:
      s3:
        - name: "my-minio-destination"
          access_key_id: "YOUR_MINIO_ACCESS_KEY"
          secret_access_key: "YOUR_MINIO_SECRET_KEY"
          bucket_name: "your-minio-bucket-name"
          path_to_file: "your/destination/prefix"
          endpoint_url: "http://your-minio-server:9000" 
```

- `endpoint_url`: The API endpoint of your S3-compatible storage service.

:::info NOTE
`endpoint_url` is used to enable using an S3-compatible service, such as GCS or Minio, as a destination.
:::

The rest of the setup, including creating asset files and running the export, remains the same as described in the "Writing data to S3" section. By specifying the `endpoint_url`, Bruin will direct Ingestr to interact with your chosen S3-compatible provider instead of AWS S3.

## Controlling the layout
When writing data to S3 or an S3-compatible storage, Bruin allows you to control the naming and structure of the output files using the `layout` parameter in your S3 connection configuration within `.bruin.yml`. This parameter provides a way to customize the output path and filename based on variables like the table name and extension.

If the `layout` parameter is not specified, the output structure follows the default behavior of [`ingestr`](https://github.com/bruin-data/ingestr). Typically, `ingestr` creates a folder named after the source table and places the data file (e.g., a Parquet file) within it. For instance, exporting a `public.users` table to `s3://my-bucket/exports/` would result in an output file path like `s3://my-bucket/exports/public.users/<load_id>.<file_id>.parquet`.

To customize this, you can add the `layout` field to your S3 connection.

For example, to save the output as a Parquet file named directly after the table specified by your asset (for example `sales`)

```yaml
    connections:
      s3:
        - name: "my-s3-custom-layout"
          access_key_id: "YOUR_ACCESS_KEY_ID"
          secret_access_key: "YOUR_SECRET_ACCESS_KEY"
          bucket_name: "your-s3-bucket"
          path_to_file: "exports/" 
          layout: "{table_name}.{ext}"
```

Will result in the output file being written to 
- `s3://your-s3-bucket/exports/sales.parquet`.

You can find a list of available variables [here](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem#available-layout-placeholders)


### Salesforce

# Salesforce
[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform.

To set up a Salesforce connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `username`, `password` and `token`. You can obtain your security token by logging into your Salesforce account and navigating to the user settings under "Reset My Security Token."

Follow the steps below to set up Salesforce correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
      salesforce:
            - name: "salesforce"
              username: "user_123"
              password: "pass_123"
              token: "token_123"
```
- `username` is your Salesforce account username.
- `password` is your Salesforce account password.
- `token` is your Salesforce security token.

### Step 2: Create an asset file for data ingestion
To ingest data from Salesforce, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., salesforce_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.salesforce
type: ingestr
connection: postgres

parameters:
  source_connection: salesforce
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Salesforce, it will be always `ingestr`.
- `source_connection`: The name of the Salesforce connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Salesforce to ingest. You can find the available source tables [here](https://bruin-data.github.io/ingestr/supported-sources/salesforce.html#tables).
- `destination`: The name of the destination connection.


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/salesforce_asset.yml
```
As a result of this command, Bruin will ingest data from the given salesforce table into your Postgres database.



### Sap_Hana

# SAP HANA
SAP HANA is an in-memory, column-oriented, relational database management system.

Bruin supports SAP HANA  as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SAP HANA into your data warehouse.

In order to set up SAP HANA connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.
Follow the steps below to correctly set up SAP HANA as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to SAP HANA, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      hana:
        - name: "connection_name"
          username: "hana_user"
          password: "hana123"
          host: "hana-xyz.sap.com"
          port: 39013
          database: "systemdb"
```
- `name`: The name to identify this SAP HANA connection
- `username`: The SAP HANA username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the SAP HANA server
- `port`: The port number the database server is listening on ( default is 30015)
- `database`:  The name of the database to connect to

### Step 2: Create an asset file for data ingestion
To ingest data from SAP HANA, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hana_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hana
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'users.details'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the SAP HANA connection defined in .bruin.yml.
- `source_table`: The name of the data table in SAP HANA that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/hana_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given SAP HANA table into your Postgres database.



### Sftp

# sftp
SFTP (SSH File Transfer Protocol) is a secure file transfer protocol that runs over the SSH protocol. It provides a secure way to transfer files between a local and a remote computer.


Bruin supports sftp as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from sftp into your data warehouse.

In order to set up sftp connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `username`, `password`, `host`, `port` of sftp server.

Follow the steps below to correctly set up sftp as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to sftp, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    sftp:
        - name: sftp
          username: user_1
          password: pass-1234
          host: localhost
          port: 22
```

- `username`: The username for the SFTP server.
- `password`: The password for the SFTP server.
- `host`: The hostname or IP address of the SFTP server.
- `port`: The port number of the SFTP server.

### Step 2: Create an asset file for data ingestion

To ingest data from sftp, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sftp_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sftp
type: ingestr
connection: neon

parameters:
  source_connection: sftp
  source_table: 'users.csv'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the sftp connection defined in .bruin.yml.
- `source_table`: The source-table specifies /path/to/directory. The base directory on the server where bruin should start looking for files.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/sftp_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given sftp table into your Postgres database.



<img alt="sftp" src="./media/sftp_ingestion.png">


### Shopify

# Shopify
[Shopify](https://www.Shopify.com/) is a comprehensive e-commerce platform that enables individuals and businesses to create online stores.

Bruin supports Shopify as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Shopify into your data warehouse.

In order to set up Shopify connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `url` of your Shopify store and the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/shopify#setup-guide).

Follow the steps below to correctly set up Shopify as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file
To ingest data from Shopify, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., shopify_ingestion.yml) inside the assets folder and add the following content:

```yaml
   connections:
    shopify:
        - name: my-shopify
          url: test.myshopify.com
          api_key: abckey
```
- `api_key`: the API key used for authentication with Shopify

### Step 2: Create an asset file for data ingestion

```yaml
name: public.shopify
type: ingestr
connection: postgres

parameters:
  source_connection: my-shopify
  source_table: 'orders'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Shopify.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Shopify connection defined in .bruin.yml.
- `source_table`: The name of the data table in Shopify you want to ingest. For example, "order" would ingest data related to order.You can find the available source tables in Shopify [here](https://bruin-data.github.io/ingestr/supported-sources/shopify.html#available-tables)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/shopify_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Shopify table into your Postgres database.

<img width="1217" alt="shopify" src="https://github.com/user-attachments/assets/0fe4b3e9-e9b8-4967-b892-4dc539683155">



### Slack

# Slack
[slack](https://slack.com/) is a messaging platform for teams and organizations where they can collaborate, share ideas and information.

Bruin supports Slack as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Slack into your data warehouse.

In order to set up Slack connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need the `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/slack#setup-guide).

Follow the steps below to correctly set up Slack as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Slack, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      slack:
        - name: "my-slack"
          api_key: "YOUR_SLACK_API_KEY"
          
```

### Step 2: Create an asset file for data ingestion

To ingest data from Slack, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., slack_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.slack
type: ingestr
connection: postgres

parameters:
  source_connection: my-slack
  source_table: 'users'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Slack.
- connection: This is the destination connection.
- source_connection: The name of the Slack connection defined in .bruin.yml.
- source_table: The name of the data table in Slack you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/slack_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Slack table into your Postgres database.


### Smartsheet

# Smartsheet
[Smartsheet](https://www.smartsheet.com/) is a software as a service (SaaS) offering for collaboration and work management.

ingestr supports Smartsheet as a source.

Bruin supports Smartsheet as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Smartsheet into your data warehouse.

In order to set up Smartsheet connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `access_token` . For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/smartsheets.html#setting-up-a-smartsheet-integration).

Follow the steps below to correctly set up Smartsheet as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Smartsheet, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    smartsheet:
        - name: "smartsheet"
          access_token: "access_token"
```
- `access_token`: Your Smartsheet API access token.

### Step 2: Create an asset file for data ingestion

To ingest data from Smartsheet, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., smartsheet_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.smartsheet
type: ingestr
connection: postgres

parameters:
  source_connection: smartsheet
  source_table: '1234567890123456'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Smartsheet connection defined in .bruin.yml.
- `source_table`: The source table will be the `sheet_id` of the Smartsheet you want to ingest. You can find the sheet_id by opening the sheet in Smartsheet and going to File > Properties. The Sheet ID will be listed there.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/Smartsheet_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Smartsheet table into your Postgres database.



### Solidgate

# Solidgate
[Solidgate](https://solidgate.com/) is a one-stop payment processing platform that ensures seamless online purchases and streamlined payment infrastructure.

ingestr supports Solidgate as a source.

Bruin supports Solidgate as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Solidgate into your data warehouse.

In order to set up Solidgate connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `public_key` and `secret_key` . For details on how to obtain these credentials, please refer [here](https://docs.solidgate.com/payments/integrate/access-to-api/#retrieve-your-credentials).

Follow the steps below to correctly set up Solidgate as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Solidgate, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    solidgate:
        - name: "solidgate"
          secret_key: "api_sk_test"
          public_key: "api_pk_test"
```
- `public_key`: The public API key used to identify the account.
- `secret_key`: The secret API key used to authenticate requests to the Solidgate API.

### Step 2: Create an asset file for data ingestion

To ingest data from Solidgate, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., solidgate_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.solidgate
type: ingestr
connection: postgres

parameters:
  source_connection: solidgate
  source_table: 'subscriptions'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Solidgate connection defined in .bruin.yml.
- `source_table`: The name of the data table in Solidgate you want to ingest. You can find the available source tables in Solidgate [here](https://bruin-data.github.io/ingestr/supported-sources/solidgate.html#tables)


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/Solidgate_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Solidgate table into your Postgres database.


<img alt="solidgate" src="./media/solidgate_ingestion.png">



### Spanner

# GCP Spanner
GCP Spanner is a fully managed, mission-critical database service that combines the capabilities of relational databases, key-value stores, and search engines.

Bruin supports GCP Spanner as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Spanner into your data warehouse. 

In order to set up a GCP Spanner connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up GCP Spanner as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to GCP Spanner, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:
```yaml
    connections:
      spanner:
        - name: "my-spanner"
          project_id: "project_123"
          instance_id: "instance_123"
          database: "my-db"
          service_account_file: "./service_account.json"
```
URI parameters:
- `project_id`: Your Google Cloud project ID
- `instance_id`: The Spanner instance ID
- `database`: The database name
- `service_account_file`: The path to the service account JSON file
- `service_account_json`: The service account JSON content itself. This is an alternative to using `service_account_file`

### Step 2: Create an asset file for data ingestion

To ingest data from GCP Spanner, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., gcp_spanner_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.spanner
type: ingestr
connection: neon

parameters:
  source_connection: my-spanner
  source_table: 'tbl1'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: The name of the destination connection. For example, "neon" is a connection name.
- `source_connection`: The name of the GCP Spanner connection defined in .bruin.yml.
- `source_table`: The name of the data table in GCP Spanner that you want to ingest.
- `destination`: The name or type of the destination connection, which is Postgres.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/gcp_spanner_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given GCP Spanner table into your Postgres database.

<img alt="Spanner" src="./media/spanner_ingestion.png">


### Sqlite

# SQLite
SQLite is a C-language library that implements a small, fast, self-contained, high-reliability, full-featured, SQL database engine.

Bruin supports SQLite as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SQLite into your data warehouse. 

In order to set up SQLite connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up SQLite as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to SQLite, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:
```yaml
    connections:
      sqlite:
        - name: "sqlite"
          path: "/Users/test/temporary/my_database.db"
```
- `name`: The name to identify this SQLite connection
- `path`: The path to the SQLite database file


### Step 2: Create an asset file for data ingestion

To ingest data from SQLite, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., sqlite_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.sqlite
type: ingestr
connection: neon

parameters:
  source_connection: sqlite
  source_table: 'users'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the SQLite connection defined in .bruin.yml.
- `source_table`: The name of the data table in SQLite that you want to ingest.
- `destination`: The name of the destination connection.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/sqlite_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given SQLite table into your Postgres database.

<img alt="applovinmax" src="./media/sqlite_ingestion.png">



### Stripe

# Stripe
[Stripe](https://stripe.com/) is a technology company that builds economic infrastructure for the internet, providing payment processing software and APIs for e-commerce websites and mobile applications.

Bruin supports Stripe as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Stripe into your data warehouse.

In order to set up Stripe connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You need `api_key`. For details on how to obtain these credentials, please refer [here](https://dlthub.com/docs/dlt-ecosystem/verified-sources/stripe#grab-credentials).

Follow the steps below to correctly set up Stripe as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file

To connect to Stripe, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
    stripe:
        - name: 'my-stripe'
          api_key: 'test_123'
```
- `api_key`: the API key used for authentication with the Stripe API

### Step 2: Create an asset file for data ingestion

To ingest data from Stripe, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., stripe_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.stripe
type: ingestr
connection: postgres

parameters:
  source_connection: my-stripe
  source_table: 'event'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: "postgres" indicates that the ingested data will be stored in a PostgreSQL database.
- `source_connection`: The name of the Stripe connection defined in .bruin.yml.
- `source_table`: The name of the data table in Stripe you want to ingest. For example, "events" would ingest data related to events. You can find the available source tables in Stripe [here](https://bruin-data.github.io/ingestr/supported-sources/stripe.html#tables)


### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/stripe_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Stripe table into your Postgres database.



<img width="1088" alt="stripe" src="https://github.com/user-attachments/assets/7133763d-91cb-4882-bb82-02617024b5dc">



### Tiktokads

# TikTok Ads
TikTok Ads is an advertising platform that enables businesses and marketers to create, manage, and analyze ad campaigns targeting TikTok's user base.

Bruin supports TikTok Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from TikTok Ads into your data warehouse.

In order to set up TikTok Ads connection, you need to add a configuration item in the `.bruin.yml` file and `asset` file. You need `access_token` and `advertiser_ids`. `timezone` is optional. For details on how to obtain these credentials, please refer [here](https://business-api.tiktok.com/portal/docs?id=1738373141733378)

Follow the steps below to correctly set up TikTok Ads as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to TikTok Ads as a source, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
connections:
  <destination_connection_details>
  tiktokads:
     - name: my-tiktokads
       access_token: "access_token123"
       advertiser_ids: "id_123,id_456"
       timezone: "Europe/Madrid"
```
- `access_token` (required): Used for authentication and is necessary to access reports through the TikTok Marketing API.
- `advertiser_ids` (required): The comma-separated list of advertiser IDs to retrieve data for.
- `timezone` (optional): The timezone to use for the data retrieval, you should set this value to the timezone of the advertiser account. Defaults to `UTC`.

### Step 2: Create an asset file for data ingestion
To ingest data from TikTok Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., tiktokads_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.tiktokads
type: ingestr
connection: postgres

parameters:
  source_connection: my-tiktokads
  source_table: 'custom:campaign_id,stat_time_day:clicks,cpc'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the  TikTok Ads connection defined in .bruin.yml.
- `source_table`: The custom report in TikTok Ads you want to ingest based on the dimension and metrics.
Custom Table Format:

`custom:<dimensions>:<metrics>[:<filter_name,filter_values>]`
Parameters:
`dimensions`(required): A comma-separated list of dimensions to retrieve.
`metrics` (required): A comma-separated list of metrics to retrieve.
`filters (optional): Filters are specified in the format <filter_name=filter_values>.
`filter_name`: The name of the filter (e.g. campaign_ids).
`filter_values`: A comma-separated list of one or more values associated with the filter name (e.g., camp_id123,camp_id456). Only the IN filter type is supported. Learn more about filters [here](https://business-api.tiktok.com/portal/docs?id=1751443975608321).

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/tiktok_ads_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given TikTok Ads table into your Postgres database.


<img alt="tiktokads" src="./media/tiktok_ads.png">






### Zendesk

# Zendesk
[Zendesk](https://www.zendesk.com/) is a cloud-based customer service and support platform. It offers a range of features including ticket management, self-service options, knowledgebase management, live chat, customer analytics, and conversations.

Bruin supports Zendesk as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Zendesk into your data warehouse.

In order to set up Zendesk connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in `asset` file. Depending on the data you are ingesting (source_table), you will need to use either `API Token authentication` or `OAuth Token authentication`. Choose the appropriate method based on your source table. For more details, please refer to the [Ingestr documentation](https://bruin-data.github.io/ingestr/supported-sources/zendesk.html)

Follow the steps below to correctly set up Zendesk as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to Zendesk, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

API Token Authentication:
```yaml
      connections:
          zendesk:
            - name: "my_zendesk",
              api_token: "xyzKey",
              email: "example.zendesk@gmail.com",
              sub_domain: "myCompany",
```

OAuth Token Authentication:
```yaml
  connections:
        zendesk:
          - name: "my_zendesk",
            oauth_token: "abcToken",
            sub_domain: "myCompany",
```

- `sub_domain`: the unique Zendesk subdomain that can be found in the account URL. For example, if your account URL is https://my_company.zendesk.com/, then `my_company` is your subdomain
- `email`: the email address of the user
- `api_token`: the API token used for authentication with Zendesk
- `oauth_token`: the OAuth token used for authentication with Zendesk

### Step 2: Create an asset file for data ingestion
To ingest data from Zendesk, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., zendesk_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.zendesk
type: ingestr
connection: postgres

parameters:
  source_connection: my_zendesk
  source_table: 'brands'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Zendesk connection defined in .bruin.yml.
- `source_table`: The name of the data table in Zendesk that you want to ingest. You can find the available source tables in Zendesk [here](https://bruin-data.github.io/ingestr/supported-sources/zendesk.html#tables)

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/zendesk_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Zendesk table into your Postgres database.

<img width="1082" alt="zendesk" src="https://github.com/user-attachments/assets/b4cb54eb-dc05-4b6e-a113-e07316be9bff">



## Ingestion > Media


## Quality


### Available_Checks

# Available Checks

Bruin provides the following checks to validate assets, ensuring that asset data meets specified quality standards.

- [**Accepted Values**](#accepted-values)
- [**Negative**](#negative)
- [**Non-Negative**](#non-negative)
- [**Not-Null**](#not-null)
- [**Pattern**](#pattern)
- [**Positive**](#positive)
- [**Unique**](#unique)

You can find a detailed description of each check below.
## Accepted values

This check will verify that the value of the column will be one of the accepted values

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: accepted_values
        value: [1, 3, 5, 7, 9]
```
## Negative
This check will verify that the values of the column are all negative

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: negative
```
## Non-Negative
This check will verify that the values of the column are all non negative (positive or zero)
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: non_negative
```

## Not-Null
This check will verify that none of the values of the checked column are null.
```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: not_null
```
## Pattern
The `pattern` quality check ensures that the values of the column match a specified regular expression.

> [!WARNING]
> For most platforms, POSIX regular expressions are the ones meant to be used with this check, but some platforms that don't support it (Synapse, MsSQL) might have other pattern languages (see [Pattern matching in SQL Server](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms187489(v=sql.105)?redirectedfrom=MSDN))

```yaml
columns:
  - name: name
    type: string
    description: "Just a name"
    checks:
        - name: pattern
          value: "^[A-Z][a-z]*$"
```


## Positive

This check will verify that the values of the column are all positive, i.e. greater than zero.

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: positive
```


## Unique

This check will verify that no value in the specified column appears more than once

```yaml
columns:
  - name: one
    type: integer
    description: "Just a number"
    checks:
      - name: unique
```


### Custom

# Custom Checks

While Bruin supports a [handful of built-in quality checks](./available_checks.md), they have a shortcoming:
- quality checks are not always within the scope of a single column
- some checks are specific to the business, and custom logic is needed

Due to these reasons, Bruin supports defining custom quality checks using SQL. You can define as many of them, they will all run in parallel.

## Definition Schema

You can define custom quality checks under a key called `custom_checks`:
```bruin-sql
/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks: // [!code ++]  
  - name: row count is greater than zero // [!code ++]  
    description: this check ensures that the table is not empty // [!code ++]  
    query: SELECT count(*) > 1 FROM dataset.player_count // [!code ++]  
   
@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

> [!INFO]
> Non-blocking checks are useful for long-running or expensive quality checks. It means the downstream assets will not be waiting for this quality check to finish.

There are a few fields to configure the check behavior:
- `name`: required, give a name to the check.
- `query`: required, the query to run as the quality check
- `description`: optional, add a longer description if needed using Markdown.
- `value`: optional, a value to compare the quality check output
  - if not given, Bruin will try to match the query output to be integer zero.
- `count`: optional, expected number of rows returned by the query. When set,
  Bruin will automatically wrap the query with `SELECT count(*) FROM (<query>)`.
- `blocking`: optional, whether the test should block running downstreams, default `true`.

## Examples:

### Simple check
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:
  - name: row count is greater than zero
    query: SELECT count(*) > 1 FROM dataset.player_count
    value: 1

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

### Run non-blocking checks
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:
  - name: row count is greater than zero
    query: SELECT count(*) > 1 FROM dataset.player_count
    value: 1
    blocking: false

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```
> [!INFO]
> Non-blocking checks are useful for long-running or expensive quality checks. It means the downstream assets will not be waiting for this quality check to finish.

### Specific value to expect
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:
  - name: row count is equal to 15
    query: SELECT count(*)  FROM dataset.player_count
    value: 15

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

### Count-based check
```bruin-sql
/* @bruin
name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table

custom_checks:
  - name: exactly 15 rows returned
    query: SELECT * FROM dataset.player_count
    count: 15

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1
```

> [!INFO]
> Non-blocking checks are useful for long-running or expensive quality checks. It means the downstream assets will not be waiting for this quality check to finish.



### Overview

# Quality checks

Quality checks are tests you can perform on your Bruin assets **after** they run to verify that the produced data satisfies your expectations. They are a great way to ensure your data is accurate, complete and consistent.

Quality checks run after the asset has been executed. If a check fails and its
`blocking` attribute is `true`, the rest of the checks are skipped and the asset
is marked as failed. When `blocking` is `false` the asset still fails but the
remaining checks continue to run. `blocking` defaults to `true`.

Below is a short example of attaching checks to columns:

```yaml
columns:
  - name: id
    type: integer
    description: "Primary key"
    checks:            # run built-in checks
      - name: unique
      - name: not_null
```

If any of those checks fails the asset will be marked as failed and any downstream assets will not be executed.

Quality checks can also be executed on their own without running the asset again:

```bash
bruin run --only checks assets/my_asset.sql
```



## Cloud


### Cross Pipeline

# Cross-pipeline dependencies

Bruin Cloud allows defining dependencies between pipelines. This allows you to run pipelines and refresh assets in parallel, while ensuring assets that need to wait on each other wait until the upstream dependencies are ready.

## URIs

Bruin considers assets unique; however, asset names often do not fulfill the uniqueness criteria across multiple repos, projects and pipelines. While asset names are required to be unique within the same pipeline, there can be assets with the same name across different pipelines. For instance, a mobile gaming company may have a pipeline for each game, and each pipeline may have an asset named `raw.events`. This poses a problem of uniqueness for dependencies that span pipelines and repos. 

However, even if they have assets with the same name, in reality they have separate tables in their data warehouse.

In order to uniquely identify what the individual assets contain or where they are located, Bruin has a concept of a URI. A URI is a unique identifier for an asset, and it is expected to be unique across all pipelines and repos of the customer.


> [!WARNING]
> URI is a required field for cross-pipeline dependencies. If you do not provide a URI, Bruin will not be able to resolve the dependency.

### URI Format
Bruin Cloud does not enforce a structure for the URI yet; however we recommend following the standard URI format with the destination information. These URIs need not be resolvable or accessible by any means, they are meant to convey the location of the asset.

Here are some common examples for major data warehouses:
- BigQuery: `bigquery://project.dataset.table`
- Snowflake: `snowflake://database.schema.table`
- Redshift: `redshift://database.schema.table`
- S3: `s3://bucket/path/to/file`

## Defining dependencies

In order to depend on an upstream that lives elsewhere, Bruin Cloud utilizes URIs, and identifies the upstream asset by the URI.

Let's say you have an upstream asset with a URI:
```yaml
name: asset1

# the URI allows us to identify the asset across pipelines and repos.
uri: external://some_external_asset
```

In order to define a dependency on this upstream asset that might be living anywhere across your repos and pipelines, you can use the new `uri` key in the `depends` array:
```yaml
name: domain.my_asset

depends:
  - some_asset
  - another_one
  
  # dependencies now accept objects with a `uri` key as well.
  - uri: external://some_external_asset
```

The new `uri` key in the `depends` array allows you to define a dependency on an asset that lives in a different pipeline. This allows you to define cross-pipeline dependencies without having to know what pipeline or repo the asset lives in.

> [!INFO]
> Bruin previously required `depends` to be a string array, whereas now it can also accept an object with the `uri` key.


## How it works?

When you define a cross-pipeline dependency, Bruin will wait for the upstream pipeline to finish before starting the downstream pipeline. This ensures that the downstream pipeline has the latest data from the upstream pipeline. The way it works is this:
- Bruin identifies the upstream asset from the URI, looking across all the repos and pipelines of the team.
- Once the upstream is identified, Bruin defines a "sensor" on the upstream asset. 
  - This sensor is a lightweight process that listens to the upstream asset and triggers the downstream pipeline when the upstream asset is refreshed.
  - These sensors are displayed as "prerequisites" in the asset run page for the downstream.
- Once the sensor succeeds, i.e. the upstream asset is passed successfully, the current asset is triggered and the pipeline runs as usual.

This approach has a couple of advantages:
- It allows running pipelines in parallel without any trigger-based approach, enables a more resilient and decoupled architecture.
- It is flexible for upstream failures, meaning that it will keep waiting for the upstream even if it fails, in case it succeeds later.
- It allows for a more granular control over the dependencies, as you can define dependencies on a per-asset basis.

## Limitations
- `bruin validate` CLI command validates the structure of the dependencies, but cannot validate if the URI actually exists.
- The downstream will wait for 12 hours maximum for the upstream to pass, then it will fail. This is to prevent the downstream from waiting indefinitely for the upstream to pass.

## Dependencies with different schemas

When the upstream and downstream both have the same schedule, it's easy to determine when does the downstream need to run. 
E.g., if both run every 5 minutes, then if downstream has a data interval from `2025-10-11 15:30:00`to `2025-10-11 15:35:00`,
then the upstream should have a successful run for the same interval.

In the case of mixed schedules it's a bit more complicated but still possible.
Imagine we have a downstream that runs every 5 minutes and we have a run with the data interval  `2025-10-11 15:30:00`to `2025-10-11 15:35:00`.
This downstream has an external dependency that runs every 2 minutes.

Then, in order to run the downstream, we will wait until the upstream has successful runs with data intervals covering fully the downstream data interval.
For example for the aforementioned `2025-10-11 15:30:00`to `2025-10-11 15:35:00`, we would need the downstream to have, for example:

 * `2025-10-11 15:30:00`to `2025-10-11 15:32:00`
 * `2025-10-11 15:32:00`to `2025-10-11 15:34:00`
 * `2025-10-11 15:34:00`to `2025-10-11 15:36:00`

These intervals fully cover the original downstream interval and thus the downstream can run.






### Dbt

# dbt Projects

Bruin Cloud supports [dbt](https://www.getdbt.com/) projects natively, where your dbt models will be represented as assets in Bruin Cloud.

You can define a schedule to run your dbt pipeline by adding a special variable in your `dbt_project.yml` file:

```yaml
vars:
  bruin_schedule: "*/5 0 0 0 0"
```

You can define any schedule you want using the cron syntax. The above example will run your dbt pipeline every 5 minutes.

> [!NOTE]
> The `bruin_schedule` variable is optional. If you don't define it, your dbt pipeline will not run automatically.




### Instance Types

# Instance Types

Bruin Cloud runs all assets in individual, ephemeral environments, called "instances". These instances are managed by the Bruin Cloud platform, and provide serverless compute for all of your assets.

You can configure the instance type for each asset inside your asset definitions:

```yaml
instance: "b1.small"
```

The following instance types are available in Bruin Cloud:

| Instance Type | CPU  | Memory |
|---------------|------|--------|
| b1.nano       | 250m | 256Mi  |
| b1.small      | 500m | 1Gi    |
| b1.medium     | 750m | 2Gi    |
| b1.large      | 1    | 4Gi    |
| b1.xlarge     | 2    | 6Gi    |
| b1.2xlarge    | 2    | 8Gi    |
| b1.3xlarge    | 3    | 12Gi   |
| b1.4xlarge    | 4    | 16Gi   |

By default, Bruin Cloud will use the `b1.nano` instance type.

Notes:
- CPU is measured in cores (1000m = 1 core)
- Memory is measured in gibibytes (Gi) or mebibytes (Mi)
- Values shown represent guaranteed resources allocated to your instance

## Custom Instance Types
Bruin Cloud supports custom instance types. You can specify a custom instance type by setting the `instance` field to the name of the instance type you want to use. Talk to your account manager to get access to custom instance types.





### Notifications

# Notifications

Bruin Cloud supports various types of notifications, starting with Slack & Microsoft Teams. These notifications allow you to receive updates on your data pipelines, such as when a pipeline has completed successfully, as well as when a pipeline has failed.

Notifications are always defined on a pipeline level, inside the `pipeline.yml` file.

## Slack

> [!INFO]
> You need to create a Slack connection in Bruin Cloud before you can use Slack notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

Adding Slack notifications is just a few lines of code:
```yaml
notifications:
  slack:
    # the only required field is `channel`. By default, this will send both success and failure notifications to this channel.   
    - channel: "#channel1"
    
    # you can have multiple channels, all of them will be notified.
    - channel: "#channel2"
    
    # you can also specify different channels for success and failure notifications
    - channel: "#channel-for-only-success"
      failure: false

    - channel: "#channel-for-only-failure"
      success: false

```

The full spec for Slack notifications is like this:
```yaml
notifications:
  slack:
    - channel: "#your-channel-name"
      success: true
      failure: true
```

## Microsoft Teams

> [!INFO]
> You need to create a Microsoft Teams connection in Bruin Cloud before you can use Teams notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Microsoft Teams webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Microsoft Teams notifications is like this:
```yaml
notifications:
  ms_teams:
    - connection: "the-name-of-the-ms-teams-connection"
      success: true
      failure: true
```

## Discord

> [!INFO]
> You need to create a Discord connection in Bruin Cloud before you can use Discord notifications. You can do this by navigating to the `Connections` tab in the Bruin Cloud UI.

A Discord webhook can be configured per channel, which means you can send notifications to multiple channels by adding separate connections.

The full spec for Discord notifications is like this:
```yaml
notifications:
  discord:
    - connection: "the-name-of-the-discord-connection"
      success: true
      failure: true
```



### Overview

# Overview

[Bruin Cloud](https://github.com/bruin-data/bruin) is a cloud platform that offers a managed environment for Bruin pipelines.

It includes:
- a built-in data catalog
- serverless environment for all transformations
- cost & usage reporting
- advanced data lineage abilities, including column-level lineage
- support for data ingestion, data transformation, data quality and governance
- dedicated support from the founding team

If you are interested, feel free to drop your email [here](https://github.com/bruin-data/bruin).

> [!INFO]
> This part of the documentation is relevant for Bruin Cloud customers.


## Vscode-Extension


### Configuration

## Configuration

You can configure the Bruin VS Code extension to suit your preferences, adjusting settings for folding behavior and the default path separator.

### Folding Behavior

Choose whether Bruin sections are folded or expanded by default when opening an asset. Adjust this in the extension settings under `Bruin > Default Folding State`. The options are:
  - **Folded**: All Bruin sections will be collapsed by default.
  - **Expanded**: All Bruin sections will be open by default.

This feature helps you focus on relevant sections, keeping the rest of the code minimized.

### Path Separator

Set your preferred path separator in the extension settings under `Bruin > Path Separator`. While the extension detects your operating system's default (`\` for Windows and `/` for Unix-based systems), you can override this if you have specific path formatting needs.

### Accessing Configuration Settings

To access and modify the Bruin extension settings:

1. Open VS Code.
2. Click the **Settings** gear icon in the lower-left corner.
3. In the search bar, type "Bruin" to filter the Bruin extension settings.
4. Adjust the settings for **Default Folding State** and **Path Separator** as needed.


![Extension Config](../public/vscode-extension/extension-config.png)



### Getting Started

# Getting Started

Once you’ve installed the Bruin VS Code extension and configured your settings, you’re ready to start using its features. Follow these steps to get started:

## 1. Open a Supported File

The Bruin extension works with SQL, Python, and YAML files. Open a file with one of these extensions in VS Code, or create a new one to get started.

## 2. Create a Bruin Section

To access Bruin features, add a Bruin section in your code. You can do this manually or with the extension’s predefined snippets. You can learn more about Bruin sections in the [Asset Definition](../getting-started/concepts.md) **Concepts** section under **Asset Schema Definition**.

- **Using Snippets**: Quickly create Bruin sections with these snippets:
  - **For SQL Files**: Type `!fullsqlasset` and select the snippet to insert a Bruin section. Or, create it manually like this:
    ```bruin-sql
    /* @bruin
      Your Bruin-specific SQL commands or configurations go here.
    @bruin */
    ```

  - **For Python Files**: Type `!fullpythonasset` and select the snippet to insert a Bruin section. Alternatively, create it manually like this:
    ```bruin-python
    """@bruin
    Your Bruin-specific Python logic or configurations go here.
    @bruin"""
    ```

## 3. Use Syntax Highlighting and Autocompletion

Syntax highlighting is automatic in a Bruin section. If the section is written without formatting errors, the syntax should be highlighted in YAML colors. If it appears as a comment instead, check the structure to ensure there are no errors. Autocompletion is also available in `pipeline.yml` and `.bruin.yml` files, allowing you to access relevant properties and values as you type, with a JSON schema to maintain accuracy and completeness.

![Bruin Autocomplete](../public/vscode-extension/snippets/autocomplete-postgres.gif)

## 4. Run Bruin CLI Commands

Run Bruin CLI commands directly from the extension:

- Use the command palette (`Ctrl + Shift + P`) and type the command you want, like `Bruin: Install Bruin CLI`.
- Or, use the buttons in the UI to run commands without needing the terminal.

![Bruin Action Buttons](../public/vscode-extension/render-asset.gif)

## 5. Explore the Bruin Panels

The Bruin extension provides two main panels :

- **Side Panel**: Displays the current asset’s details with tabs for viewing asset information, columns, and settings.
- **Lineage Panel**: Found at the bottom of VS Code, near the terminal, this panel shows the asset lineage, giving you a visual overview of how the asset connects to other assets.
- **Query Preview Panel**: Found near the `Lineage` panel, this panel allows you to preview query results directly within VS Code. When running a query, results will be displayed in this panel, enabling quick debugging and verification without switching to an external database tool.




### Overview

# Bruin VS Code Extension

The Bruin VS Code extension complements the Bruin CLI by offering a more visual and interactive approach to managing data pipelines. Integrated directly into VS Code, it simplifies tasks like building, managing, and deploying pipelines, making it easier for developers to interact with their projects.

![Bruin Action Buttons](/vscode-extension/render-asset.gif)


## Syntax highlighting

Bruin has its own syntax for defining asset metadata, inside special markers. In order to make the experience of building Bruin pipelines easier, the VS Code extension includes a built-in syntax highlighter, allowing you to easily navigate the configuration & code.

![Syntaxe Coloring](../public/vscode-extension/syntaxe-coloring.png)

## Autocompletion & Snippets

The Bruin VS Code extension includes autocompletion for Bruin [definitions](../assets/definition-schema), and validates the configuration. This allows you to quickly build models & easily identify mistakes in your asset definitions.

In addition, Bruin VS Code extension includes snippets to auto-generate full or partial configurations. Simply type `!fullsqlasset` and get a ready-to-go asset with all the fields filled out.

## Data Lineage

The VS Code extension includes a built-in lineage panel that allows you to get a clear view of how data assets connect and interact.

![Lineage Overview](../public/vscode-extension/panels/lineage-panel/lineage-overview.png)

## Rendered Jinja queries
Bruin supports [Jinja](../assets/templating/templating) out of the box, and while Jinja is powerful, it is often tricky to understand the resulting query. The VS Code extension includes a built-in query renderer, which shows you the rendered version of the query, not just for Jinja but also [materialized](../assets/materialization.md) versions of any query.

![Bruin Action Buttons](/vscode-extension/render-asset.gif)



## Local data catalog

The VS Code extension visualizes Bruin definitions, including asset & column documentation. This means that you can simply view an asset file, and on the right side you can see all the relevant details about an asset, its columns, quality checks, and lineage.


## Bruin CLI integration

The VS Code extension gives a visual way of working with Bruin pipelines. It integrates seamlessly with Bruin CLI, and allows you to validate, run and manage your assets from the interface.

![CLI Integration](../public/vscode-extension/action-buttons.gif)

## Real-time updates and feedback
The extension automatically runs things in the background and shows you feedback immediately as you type or change things.

![Real-Time Feedback](../public/vscode-extension/real-time-feedback.gif)



### Panel Overview

# Panels Overview  

The Bruin VS Code extension provides three main panels that help streamline your workflow: the Side Panel, the Lineage Panel, and the Query Preview Panel. 
Each panel offers specific functionality to enhance asset management, data lineage tracking, and query result visualization.

![Bruin Panels](../public/vscode-extension/panels/bruin-panels.png)



## Vscode-Extension > Panels


### Lineage Panel

# Lineage Panel

The Lineage Panel is located at the bottom of the VS Code interface, near the terminal tab. It provides a visual representation of the current asset's lineage.

## Functionality
- **Display Asset Lineage**
    - Shows how the current asset is connected to others in the data pipeline.
    - The view updates automatically when changes are made or when switching to another asset.

- **Expand Dependencies**: 
    - The lineage panel includes an options menu, collapsed by default. When expanded, you can choose which part of the lineage to display: `All (downstream, upstream)`, only one of them, or `Direct Only` (with `Direct Only` selected by default).
    - Each downstream or upstream node that has further dependencies displays a plus button. Clicking this button expands the node to show the dependencies in the same direction.

- **Navigate to Node Asset**  
    - When you click on a particular node, a link appears that allows you to navigate directly to the corresponding asset file by clicking it.

- **Control panel**: 
    - A control panel allows you to zoom in and out, fit the view, or lock the nodes in place to prevent displacement.

![Bruin Lineage Panel](../../public/vscode-extension/panels/lineage-panel/lineage-panel-with-options.gif)

- **Pipeline Lineage View** (New)  
    - A new **PipelineLineage** component has been added.
    - Navigation controls are now available in the expanded panel at the top right:
        - A **radio button** allows you to switch between `Asset View` and `Pipeline View`.
        - Selecting **Pipeline View** displays the data pipeline flow for the current asset.
        - In the **Pipeline View**, a button labeled **Asset View** appears in the top right. Clicking it switches back to showing the asset lineage for the current file.
    - This allows users to easily toggle between seeing an individual asset's lineage and its broader pipeline context.

![Bruin Lineage Panel](../../public/vscode-extension/panels/lineage-panel/pipeline-lineage-view.gif)


 



### Query Preview

# Query Preview Panel
The Query Preview Panel is located at the bottom of the VS Code interface, near the `Lineage` Panel. It offers a visual representation of the query output, displayed as a table.

## Functionality
- **Display Query Output**  
    - A **Run** button is available, along with the keybindings `Ctrl + Enter` (Windows) or `Cmd + Enter` (Mac). Clicking the button will execute the entire query for the asset and display the result.  
    - The query must be located in a valid Bruin asset, random files are not supported at this time.  
    - You can also select a part of the query to run. When you highlight a subquery and click **Run**, only the selected query will be executed, and the result will be shown.  
    - **Limit**: The maximum number of rows displayed in the query output is limited to `1000`. This limit is editable, but it cannot exceed `1000` rows.  
    - **Clear Icon Button**: Clears the results of the current query output.  
    - **Search**: Allows you to search for specific words within the query output. The number of matches found is displayed along with the total number of rows.

- **Multi-Tab Functionality**  
    - Multiple queries can be run in separate tabs, allowing you to add or delete tabs as needed. When you run a query, the output is displayed in the current tab, while the other tabs retain their individual query outputs.  
    - **Search** and **Clear** work independently within each tab, just as the **Run** function does.

    ![Bruin Query Preview Panel](../../public/vscode-extension/panels/query-preview-panel/query-preview-options.gif)



## Vscode-Extension > Panels > Side-Panel


### Editor Experience

# Editor Experience
These in-editor features appear automatically based on file context or environment setup, helping you take quick action without leaving the editor.

## CLI Installation Prompt
If the Bruin CLI is not installed, the extension helps you get set up with minimal friction.

* **Trigger Condition**:
  * Triggered when you click the Render button and the CLI is missing.

* **How It Works**:
  * An alert is shown in the side panel.
  * A **“Install CLI”** button allows you to install the CLI immediately.

![Install Bruin CLI](../../../public/vscode-extension/panels/side-panel/install-cli.gif)

## File Conversion
When you open a file that isn’t yet a Bruin asset but is eligible for conversion, the extension displays an alert offering a conversion option.

* **Eligible File Types**:
  * `.sql` and `.py`: Converted using the `patch-asset` command.
  * `.yml`: Converted by renaming to `*.asset.yml`.

* **How It Works**:
  * An alert appears when an eligible file is opened.
  * A **“Convert to Asset”** button is provided to initiate the conversion directly.

  ![Convert file to Bruin asset](../../../public/vscode-extension/panels/side-panel/convert-file-to-asset.gif)



### Side Panel

# Side Panel

The Side Panel in the Bruin VSCode extension enhances your workflow in two main ways:
- **Tabs** – for managing asset-specific data.
- **Editor Experience** – for contextual, in-editor prompts like file conversion and installation alerts.



### Tabs



# Tabs Overview
These tabs give you direct access to view and manage details of the currently open Bruin asset.

## 1. General
This tab provides a comprehensive view of your asset, allowing you to manage key information and execute actions directly from the UI.

- **Pipeline & Asset Name**: Displays the current pipeline and asset names. The asset name can be updated from the UI.
- **Pipeline Schedule & Asset Type**: Displays the current pipeline schedule and asset type as tags.
- **Description**: View or edit the asset’s description to document its purpose.
- **Environment Dropdown**: Seamlessly switch between environments (e.g., `development`, `production`).
- **Date Inputs**: 
    - Customize the date/time for runs directly within the UI. 
    - A refresh button allows synchronization with the pipeline schedule.
    - *Note*: The time is specified in `UTC`.
- **SQL Preview**: Displays a preview of the SQL query related to the asset for quick inspection.

![Bruin Side Panel](../../../public/vscode-extension/panels/side-panel/asset-details-tab-new.gif)

## 2. Columns
- Lists the columns associated with the asset.
- *Each column shows*:
  - **Name**: The name of the column.
  - **Type**: The data type of the column (e.g., string, number).
  - **Quality Checks**: Any quality checks linked to the column.
  - **Description**: A brief description of the column's purpose.
  - **Glossary Integration**:
    - Columns sourced from the Glossary are marked with a link icon.
    - Clicking the link opens the corresponding glossary for more details.
- *Edit Capability*:
  - **Edit Name, Type, and Description**: Modify column properties directly within the UI.
  - **Add or Remove Columns**: Easily insert new columns or delete existing ones.
  - **Manage Quality Checks**: Add or remove quality checks.

![Bruin Columns Tab](../../../public/vscode-extension/panels/side-panel/manage-columns.gif)

## 3. Details

The Details tab is your central hub for configuring crucial metadata and operational aspects of your asset within the database. It allows you to manage everything from how your asset is stored to who owns it and how its data intervals are defined.

### Features:
- **Owner and Tags**:
  - **Owner**: Set or edit the owner of the asset. This can be useful for identifying who is responsible for the asset.
  - **Tags**: Add or remove tags to categorize and filter assets.

- **Partitioning**:
  - Select a column or manually enter an expression to partition the table by. Supports expressions such as date(col_name) or timestamp_trunc(col2, hour).

- **Clustering**:
  - Select multiple columns to cluster the table by. Clustering organizes data storage based on the specified columns, which can improve query performance.

- **Interval Modifiers**: Define the time window or validity period for your asset's data. See [Interval Modifiers](../../../assets/interval-modifiers.md) for more details.

  - **Start**: Set the beginning of the interval using a numerical value and select a specific unit from the dropdown (e.g., -2 days).
  - **End**: Set the end of the interval using a numerical value and select a specific unit from the dropdown (e.g., 1 month).

- **Materialization**: Control how your asset is materialized in the database.
  - **None**: No materialization.
  - **Table**: Materialize the asset as a table.
  - **View**: Materialize the asset as a view.

- **Strategy** (for Tables):
  - **Create + Replace**: Drop and recreate the table completely.
  - **Delete + Insert**: Delete existing data using an incremental key and insert new records.
  - **Append**: Add new rows without modifying existing data.
  - **Merge**: Update existing rows and insert new ones using primary keys.
  - **Time Interval**: Process time-based data using an incremental key.
  - **DDL**: Use DDL to create a new table using the information provided in the embedded Bruin section.

### How to Use:

1. **Set Owner and Tags**:
   - Edit the owner by clicking on the edit icon next to the owner field. Changes are saved immediately.
   - Add or remove tags by clicking on the add or remove icons next to the tags field. Changes are saved immediately.

2. **Set Partitioning**:
   - Click on the partitioning input field and select a column from the dropdown list to partition your table.

3. **Set Clustering**:
   - Click on the clustering input field and select one or more columns from the dropdown list to cluster your table.

4. **Set Interval Modifiers**:
   - Edit the interval modifiers by clicking on the edit icon next to the interval modifiers field. Changes are saved immediately.

5. **Select Materialization Type**:
   - Choose between `None`, `Table`, or `View` using the radio buttons.

6. **Configure Strategy** (if Table is selected):
   - Select a strategy from the dropdown menu. Each strategy has a description to help you understand its use case.

7. **Save Changes**:
   - Click the "Save Changes" button to apply your materialization settings for partitioning, clustering, and strategy.

![Details Tab](../../../public/vscode-extension/panels/side-panel/details-tab.gif)

## 4. Custom Checks
The Custom Checks tab allows you to manage custom checks for your assets directly from the UI.

### How to Use:

1. **Add a Custom Check**:
   - Click the "Add Check" button.
   - Enter the details for your custom check:
     - **Name**: Provide a descriptive name.
     - **Value**: Set the expected value or threshold.
     - **Description**: Add a brief description of the check.
     - **Query**: Input the SQL query for validation.

2. **Edit a Custom Check**:
   - Click the edit icon (pencil) next to the check you wish to modify.
   - Make your changes in the provided fields.
   - Save your changes by clicking the save icon (checkmark), or cancel by clicking the cancel icon (cross).

3. **Delete a Custom Check**:
   - Click the delete icon (trash can) next to the check you want to remove.
   - Confirm the deletion in the dialog that appears.

4. **View Custom Checks**:
   - All custom checks are displayed in a table, showing their name, value, description, and query.
   - SQL queries are syntax-highlighted for clarity.

![Custom Checks Tab](../../../public/vscode-extension/panels/side-panel/custom-checks-tab.gif)

## 5. Settings
The Settings tab has two main sections:

### a. Bruin CLI Management
- **Install & Update**: Easily install or update the Bruin CLI directly from the UI.
- **Quick Documentation Access**: A dedicated button redirects you to the Bruin documentation for reference.
- **Version Details & Debugging**: A chevron down arrow expands to reveal:
  - Bruin CLI version
  - Your operating system version
  - VS Code extension version
  - These details can be copied for debugging purposes.

![Bruin Settings Tab](../../../public/vscode-extension/panels/side-panel/settings-tab.png)

### b. Connection Management
- You can manage your connections, including:
  - **Add Connection**: Add new connections by entering the required credentials. If the connection name already exists in the same environment, an error will be displayed.
  - **Duplicate Connection**:  If some connections share similar credentials, it's easier to duplicate and modify them as needed. This is fully supported.
  - **Update Connection**: Update existing connections.
  - **Delete Connection**: Delete existing connections.  
  - **Test Connection**: This allows you to test your connection directly from the UI. Unsupported connections will display a message indicating they cannot be tested.

![Bruin Connections Manager](../../../public/vscode-extension/panels/side-panel/manage-connections.gif)




## .Vitepress


## .Vitepress > Cache


## .Vitepress > Cache > Deps


## .Vitepress > Theme


## Cicd


### Github Action

# GitHub Actions

The [`bruin-data/setup-bruin@main`](https://github.com/marketplace/actions/setup-bruin) action is used to set up the Bruin environment in your GitHub Actions workflow. This action ensures that all necessary dependencies and configurations are in place for your Bruin tasks to run smoothly.

```yaml
- uses: bruin-data/setup-bruin@main
```

### Parameters
- **version** (optional): Specify the version of Bruin to install. If not provided, the latest version will be used.


## Setting Up GitHub Actions
1. Create a `.github/workflows` directory in your repository.
2. Inside that directory, create a YAML file (e.g., `ci.yml`) to define your workflow.

```yaml
name: Bruin Test

on:
  push:
    branches: [ main ]
jobs:
  linux:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    
    # install Bruin CLI in your pipeline
    - uses: bruin-data/setup-bruin@main
    
    # validate your pipeline
    - run: bruin validate ./bruin-pipeline/
      name: Validate Pipeline
```




## Public


## Public > Getting-Started


## Public > Vscode-Extension


## Public > Vscode-Extension > Panels


## Public > Vscode-Extension > Panels > Lineage-Panel


## Public > Vscode-Extension > Panels > Query-Preview-Panel


## Public > Vscode-Extension > Panels > Side-Panel


## Public > Vscode-Extension > Snippets

