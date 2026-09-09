# Schema

The tables sit in DuckDB's default schema. Queries read `FROM orders` with no
prefix, and `SHOW TABLES` lists all seven.

| Table | Rows |
|---|---|
| `dates` | 1,096 |
| `stores` | 6 |
| `products` | 60 |
| `customers` | 510 |
| `orders` | 1,212 |
| `order_items` | 2,895 |
| `fx_rates` | 5,480 |

## dates

One row per calendar day, 2023-01-01 to 2025-12-31.

```text
date_day            DATE
date_key            INTEGER
year                INTEGER
quarter             INTEGER
year_month          VARCHAR
month_number        INTEGER
month_name          VARCHAR
iso_week            INTEGER
day_of_week_number  INTEGER
day_of_week_name    VARCHAR
is_working_day      BOOLEAN
```

## stores

One row per store.

```text
store_id      INTEGER
store_code    VARCHAR
country_code  VARCHAR
country_name  VARCHAR
city          VARCHAR
opened_on     DATE
closed_on     DATE
status        VARCHAR
timezone      VARCHAR
```

## products

One row per product.

```text
product_id        INTEGER
product_code      VARCHAR
product_name      VARCHAR
brand             VARCHAR
category_name     VARCHAR
subcategory_name  VARCHAR
color             VARCHAR
unit_cost         DECIMAL
list_price        DECIMAL
```

## customers

One row per customer account.

```text
customer_id   INTEGER
first_name    VARCHAR
last_name     VARCHAR
city          VARCHAR
state         VARCHAR
country       VARCHAR
signed_up_on  DATE
segment       VARCHAR
```

## orders

One row per order, as delivered by the source system.

```text
order_id                INTEGER
customer_id             INTEGER
store_id                INTEGER
ordered_at              TIMESTAMP
_loaded_at              TIMESTAMP
promised_delivery_date  DATE
currency_code           VARCHAR
order_status            VARCHAR
order_total             DECIMAL
```

## order_items

One row per line on an order.

```text
order_id     INTEGER
line_number  INTEGER
product_id   INTEGER
quantity     INTEGER
unit_price   DECIMAL
net_price    DECIMAL
unit_cost    DECIMAL
```

## fx_rates

One row per date per currency pair.

```text
rate_date      DATE
from_currency  VARCHAR
to_currency    VARCHAR
rate           DECIMAL
```

---

This file records the grain of each table and nothing else. The column
descriptions are missing on purpose. Writing them is coursework.
