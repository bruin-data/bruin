# Brief: Local DuckDB + DuckLake tutorial

Instructions for writing a Learn/Tutorials doc that walks a reader through building a **local DuckLake lakehouse** with Bruin, from scratch.

## Goal

The reader ends with a working local lakehouse (DuckDB engine + Postgres catalog + MinIO/S3 storage) and a pipeline that **seeds → ingests → transforms** data inside it.

Learning outcomes to state up front:
- How the pieces of a DuckLake lakehouse fit together: engine, catalog, storage.
- Running a Postgres catalog and a MinIO (S3-compatible) store locally with Docker.
- Configuring a DuckDB + DuckLake connection in `.bruin.yml`.
- Building and running a seed → ingest → transform pipeline against the lakehouse.

## Concept to explain first

A lakehouse stores data as open-format files (Parquet) on object storage, while a catalog database tracks schema, table versions, and metadata. DuckLake is the format we use here. See the [DuckLake reference](../../platforms/duckdb.md#ducklake) for the supported catalog/storage combinations. Three moving parts in this setup:

- **Engine** — DuckDB, embedded in Bruin. Runs the SQL, reads/writes the lakehouse.
- **Catalog** — a Postgres database (`ducklake_meta`) holding lakehouse metadata.
- **Storage** — a MinIO bucket holding the Parquet files. MinIO speaks the S3 API, standing in for AWS S3 locally.

(A simple engine → catalog / engine → storage diagram would help here.)

## Prerequisites

- Bruin CLI installed — link [Installation](../introduction/installation.md).
- Docker **or** OrbStack installed and running. Note OrbStack is a drop-in Docker replacement on macOS; all commands below use standard `docker` / `docker compose`, so either works. `docker ps` should succeed.

## Step 1 — Initialize an empty project

```bash
bruin init empty duckdb-ducklake
```

Resulting structure — note that `.bruin.yml` (the connection/config file) is created at the **repo root**, one level above the pipeline folder:

```plaintext
.bruin.yml          # connections & config — repo root
.gitignore
duckdb-ducklake/    # the pipeline
├─ assets/
└─ pipeline.yml
```

Run every `bruin` command from this **repo root** (the directory that holds `.bruin.yml`).

## Step 2 — Start local infrastructure

Create `docker-compose.yml` at the repo root (next to `.bruin.yml`):

```yaml
services:
  # Catalog database: Stores DuckLake metadata
  catalog:
    image: postgres:16
    environment:
      POSTGRES_USER: lakehouse
      POSTGRES_PASSWORD: lakehouse
      POSTGRES_DB: ducklake_meta
    ports:
      - "5434:5432" # host 5434 -> container 5432
    volumes:
      - catalog-data:/var/lib/postgresql/data

  # Object storage: Holds the Parquet data files (S3-compatible)
  storage:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9010:9000" # S3 API -> http://localhost:9010
      - "9011:9001" # Web UI -> http://localhost:9011
    volumes:
      - storage-data:/data

  # One-shot job: create the "ducklake" bucket, then exit
  create-bucket:
    image: minio/mc:latest
    depends_on:
      - storage
    entrypoint: >
      /bin/sh -c "
      until (mc alias set local http://storage:9000 minioadmin minioadmin) do echo 'waiting for minio...' && sleep 1; done;
      mc mb --ignore-existing local/ducklake;
      echo 'bucket ready';
      "

volumes:
  catalog-data:
  storage-data:
```

Commands:

```bash
docker compose up -d
docker compose ps
```

Explain: `catalog` and `storage` should be running; `create-bucket` should be exited (expected — it creates the bucket and stops). MinIO console is at `http://localhost:9011` (`minioadmin` / `minioadmin`); the `ducklake` bucket should exist.

Note to include: the ports (`5434`, `9010`, `9011`) avoid clashes with default Postgres/MinIO installs. If changed here, they must match `.bruin.yml`.

## Step 3 — Configure connections

Edit the root `.bruin.yml`:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        # 1) Plain local DuckDB: The source we seed raw data into
        - name: duckdb-default
          path: source.duckdb

        # 2) The DuckLake lakehouse: DuckDB engine + Postgres catalog + MinIO storage
        - name: ducklake-pg
          path: engine.duckdb
          lakehouse:
            format: ducklake
            catalog:
              type: postgres
              host: localhost
              port: 5434
              database: ducklake_meta
              auth:
                username: lakehouse
                password: lakehouse
            storage:
              type: s3
              path: s3://ducklake/warehouse
              endpoint: localhost:9010
              url_style: path
              use_ssl: false
              auth:
                access_key: minioadmin
                secret_key: minioadmin
```

Points to make:
- `duckdb-default`: An ordinary local DuckDB file used as the *source* holding raw data before it's loaded into the lakehouse.
- `ducklake-pg`: The lakehouse; the `lakehouse` block makes it DuckLake. `catalog` points at the Postgres container (`port: 5434` matches the Docker mapping). `storage` points at MinIO. Because it's S3-compatible you need `endpoint`, `url_style: path`, and `use_ssl: false` (the three fields for any non-AWS S3 backend over plain HTTP).
- Link to [catalog options](../../platforms/duckdb.md#catalog-options) and [storage options](../../platforms/duckdb.md#storage-options) for the full set.

## Step 4 — Configure the pipeline

Edit `duckdb-ducklake/pipeline.yml`:

```yaml
name: ducklake_pipeline
schedule: "@daily"

default_connections:
  duckdb: duckdb-default
```

Explain: the default `duckdb` connection is used by any asset that doesn't name one; lakehouse assets name `ducklake-pg` explicitly.

## Step 5 — Seed raw data

Create `duckdb-ducklake/assets/orders.csv`:

```csv
order_id,country,amount
1,US,120.50
2,US,80.00
3,DE,200.00
4,DE,55.25
5,TR,300.00
6,TR,45.00
7,GB,150.75
8,GB,90.00
```

Create `duckdb-ducklake/assets/raw_orders.asset.yml`:

```yaml
name: raw.orders
type: duckdb.seed

parameters:
  path: orders.csv

columns:
  - name: order_id
    type: integer
  - name: country
    type: string
  - name: amount
    type: float
```

Explain: loads the CSV into `raw.orders` in `duckdb-default` (seed uses the pipeline's default `duckdb` connection). Link [Seed assets](../../assets/seed.md).

Declaring `columns` with types is good practice: CSVs are untyped, so defining types pins each column at the source and keeps downstream SQL and aggregations predictable. It also lets you attach quality checks — see [Seed assets → enforcing column types](../../assets/seed.md#enforcing-column-types).

## Step 6 — Load into the lakehouse (ingestr)

Create `duckdb-ducklake/assets/orders.asset.yml`:

```yaml
name: ducklake.orders
type: ingestr
connection: ducklake-pg

depends:
  - raw.orders

parameters:
  source_connection: duckdb-default
  source_table: raw.orders
  destination: duckdb
```

Explain: `connection: ducklake-pg` = destination is the lakehouse; `source_connection`/`source_table` = where it comes from; `destination: duckdb` = the engine used. Result: `orders` under the `ducklake` schema, stored as Parquet in MinIO with metadata in Postgres. Link [ingestr assets](../../assets/ingestr.md).

Declaring `depends: [raw.orders]` sets the execution order so the seed runs first and this asset loads afterwards. It also keeps the two from running at the same time, which matters because both access the same local DuckDB file and DuckDB allows only one writer per file.

## Step 7 — Transform inside the lakehouse

Create `duckdb-ducklake/assets/orders_by_country.sql`:

```bruin-sql
/* @bruin
name: ducklake.orders_by_country
type: duckdb.sql
connection: ducklake-pg
depends:
  - ducklake.orders
materialization:
  type: table
@bruin */

SELECT
  country,
  COUNT(*) AS order_count,
  SUM(amount) AS total_amount
FROM ducklake.orders
GROUP BY country
ORDER BY total_amount DESC, country;
```

Explain: `duckdb.sql` + `connection: ducklake-pg` runs against the lakehouse; `materialization.type: table` writes the result back as a lakehouse table; `depends` enforces order. Link [SQL assets / materialization](../../assets/sql.md).

Final project layout to show:

```plaintext
duckdb-ducklake/
├─ assets/
│  ├─ orders.csv
│  ├─ raw_orders.asset.yml        # seed:      CSV -> duckdb-default
│  ├─ orders.asset.yml            # ingestr:   duckdb-default -> lakehouse
│  └─ orders_by_country.sql       # transform: lakehouse -> lakehouse
└─ pipeline.yml
.bruin.yml                        # connections & config (repo root)
docker-compose.yml                # local infra (repo root)
```

## Step 8 — Validate and run

```bash
bruin validate duckdb-ducklake
bruin run duckdb-ducklake
```

Note: assets run in dependency order (seed → ingest → transform).

## Step 9 — Query and inspect

```bash
bruin query --connection ducklake-pg --query "SELECT * FROM ducklake.orders_by_country;"
```

Expected output:

```plaintext
┌─────────┬─────────────┬──────────────┐
│ COUNTRY │ ORDER_COUNT │ TOTAL_AMOUNT │
├─────────┼─────────────┼──────────────┤
│ TR      │ 2           │ 345          │
│ DE      │ 2           │ 255.25       │
│ GB      │ 2           │ 240.75       │
│ US      │ 2           │ 200.5        │
└─────────┴─────────────┴──────────────┘
```

Optional "look behind the scenes":
- Storage — MinIO console `http://localhost:9011`, look inside the `ducklake` bucket under `warehouse/` for the Parquet files.
- Catalog — inspect the metadata tables: `docker compose exec catalog psql -U lakehouse -d ducklake_meta -c "\dt"`.

## Cleanup

```bash
docker compose down -v
```

(`-v` also removes the data volumes.)

## Wrap-up / what's next (link internally only)

- Swap MinIO for real cloud storage (S3, GCS, R2) — only the `storage` block changes; see [storage options](../../platforms/duckdb.md#storage-options).
- Try a DuckDB or SQLite catalog for a lighter setup — see [catalog options](../../platforms/duckdb.md#catalog-options).
- Add [quality checks](../../quality/overview.md).
- Read the [Lakehouse Support](../lakehouse.md) overview.
