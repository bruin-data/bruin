# Odoo

[Odoo](https://www.odoo.com/) is an open-source suite of business apps covering CRM, sales, purchasing, inventory, accounting, projects, and HR.

Bruin supports Odoo as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Odoo into your data warehouse. It works with Odoo Online, Odoo.sh, and self-hosted Odoo servers.

In order to set up an Odoo connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Odoo as a data source and run ingestion.

## Configuration

### Step 1: Get an API key

1. Log in to Odoo as the user that Bruin should read as. That user's access rights decide which records and fields can be ingested.
2. Open your avatar menu, then **Preferences** (or **My Profile**) → **Account Security**.
3. Click **New API Key**, give it a name, and copy the key.

The external API is only available on Odoo Online's Custom plan. It is always available on self-hosted servers and Odoo.sh.

### Step 2: Add a connection to .bruin.yml file

To connect to Odoo, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      odoo:
        - name: "odoo"
          host: "mycompany.odoo.com"
          api_key: "key_123"
          database: "mycompany"
          username: "me@example.com"
```

- `name`: The name to identify this Odoo connection.
- `host`: The address of your Odoo server, such as `mycompany.odoo.com` or `erp.example.com:8069`. Do not include `https://`; use `scheme` instead.
- `api_key`: An API key of the Odoo user to read as. On servers without API keys, the user's password works here too.
- `database`: (Optional) The Odoo database name. Required on Odoo 18 and older. On Odoo 19 and newer it is only needed when one server hosts several databases.
- `username`: (Optional) The login of the Odoo user. Required on Odoo 18 and older.
- `scheme`: (Optional) `https` or `http` for servers without TLS. Defaults to `https`.
- `api`: (Optional) The Odoo API to use: `json2` (Odoo 19 and newer) or `jsonrpc` (Odoo 18 and older). Defaults to `auto`, which detects the server version when connecting.
- `page_size`: (Optional) The number of records fetched per request, between 1 and 5000. Defaults to `1000`.

For a self-hosted Odoo 18 server without TLS:

```yaml
    connections:
      odoo:
        - name: "odoo-local"
          host: "localhost:8069"
          api_key: "key_123"
          database: "odoo"
          username: "admin"
          scheme: "http"
          api: "jsonrpc"
```

Bruin URL-encodes every value, so API keys and logins containing special characters such as `@`, `+`, `/`, or `&` can be used as-is.

### Step 3: Create an asset file for data ingestion

To ingest data from Odoo, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., odoo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.odoo_sale_orders
type: ingestr
connection: postgres

parameters:
  source_connection: odoo
  source_table: 'sale_orders'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Odoo connection defined in .bruin.yml.
- `source_table`: The name of the table to read. See [Source tables](#source-tables).

Any other model, including custom and Studio models, can be read by using its technical name as the `source_table`:

```yaml
name: public.odoo_vehicles
type: ingestr
connection: postgres

parameters:
  source_connection: odoo
  source_table: 'fleet.vehicle'

  destination: postgres
```

```yaml
name: public.odoo_custom_vehicles
type: ingestr
connection: postgres

parameters:
  source_connection: odoo
  source_table: 'x_vehicles'

  destination: postgres
```

### Step 4: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/odoo_ingestion.yml
```

As a result of this command, Bruin will ingest the sale orders from Odoo into your Postgres database.

## Source tables

Every table has the primary key `id` and uses the `merge` strategy keyed on `write_date`, Odoo's last-modified timestamp. Archived records are included.

| Source table | Odoo model | Details |
|--------------|------------|---------|
| `partners` | `res.partner` | Customers, vendors, and contacts |
| `partner_categories` | `res.partner.category` | Contact tags |
| `users` | `res.users` | Users |
| `companies` | `res.company` | Companies |
| `currencies` | `res.currency` | Currencies |
| `currency_rates` | `res.currency.rate` | Currency exchange rates |
| `countries` | `res.country` | Countries |
| `product_categories` | `product.category` | Product categories |
| `product_templates` | `product.template` | Product templates |
| `products` | `product.product` | Product variants |
| `uoms` | `uom.uom` | Units of measure |
| `pricelists` | `product.pricelist` | Pricelists |
| `sale_orders` | `sale.order` | Quotations and sales orders |
| `sale_order_lines` | `sale.order.line` | Sales order lines |
| `purchase_orders` | `purchase.order` | Requests for quotation and purchase orders |
| `purchase_order_lines` | `purchase.order.line` | Purchase order lines |
| `crm_leads` | `crm.lead` | Leads and opportunities |
| `crm_stages` | `crm.stage` | CRM pipeline stages |
| `crm_teams` | `crm.team` | Sales teams |
| `account_moves` | `account.move` | Invoices, bills, and journal entries |
| `account_move_lines` | `account.move.line` | Journal items |
| `account_payments` | `account.payment` | Payments |
| `accounts` | `account.account` | Chart of accounts |
| `journals` | `account.journal` | Accounting journals |
| `taxes` | `account.tax` | Taxes |
| `payment_terms` | `account.payment.term` | Payment terms |
| `analytic_lines` | `account.analytic.line` | Analytic lines and timesheets |
| `stock_pickings` | `stock.picking` | Transfers (receipts, deliveries) |
| `stock_moves` | `stock.move` | Stock moves |
| `stock_move_lines` | `stock.move.line` | Detailed stock move lines |
| `stock_quants` | `stock.quant` | On-hand quantities per location |
| `stock_locations` | `stock.location` | Stock locations |
| `warehouses` | `stock.warehouse` | Warehouses |
| `projects` | `project.project` | Projects |
| `project_tasks` | `project.task` | Project tasks |
| `project_task_stages` | `project.task.type` | Task stages |
| `employees` | `hr.employee` | Employees |
| `departments` | `hr.department` | Departments |
| `jobs` | `hr.job` | Job positions |
| `activities` | `mail.activity` | Scheduled activities |
| `messages` | `mail.message` | Chatter messages and notes |
| `<model_name>` | any | Any other model by its technical name, such as `fleet.vehicle` or `x_vehicles` |

A table is only available when the Odoo app that provides it is installed, such as Sales for `sale_orders` or Inventory for `stock_pickings`.

Each table contains the model's stored fields. Many-to-one fields hold the ID of the related record, many-to-many fields hold a JSON array of IDs, and one-to-many and binary (attachment and image) fields are left out. Timestamps are in UTC.

## Incremental loading

Each run fetches only the records whose `write_date` falls between the run's start and end dates, and merges them into the destination on `id`. Use [`--full-refresh`](/commands/run) to load every record again. Models without a `write_date` field are re-read in full on every run.

## Limitations

- Odoo hard-deletes records and leaves no tombstone, so records deleted in Odoo stay in the destination until the table is rebuilt with `--full-refresh`. Archived records come through with `active = false`.
- Requests are capped at 10 per second.
