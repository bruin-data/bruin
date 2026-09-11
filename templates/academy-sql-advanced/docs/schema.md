# Schema

All generated tables are deterministic and live in the DuckDB default schema.

| Table | Rows | Grain |
|---|---:|---|
| `dates` | 1,096 | one calendar day |
| `stores` | 6 | one store |
| `products` | 60 | one product |
| `customers` | 510 | source customer row |
| `customer_snapshots` | 1,080 | one changing customer per month |
| `orders` | 1,212 | source order record |
| `order_items` | 2,895 | source order line |
| `fx_rates` | 5,480 | one date and currency pair |

The six store time zones are `America/New_York`, `America/Los_Angeles`, `Europe/London`,
`Europe/Berlin`, `Australia/Sydney`, and `America/Toronto`. `orders` contains 1,200 distinct order ids and 12 changed-status resends. `order_items` contains
15 exact duplicate rows. The staging and core assets retain the defects needed by the lessons until
the student chooses a fix.

## Timestamps

`ordered_at` is a naive store-local timestamp. `ordered_at_utc` is populated for about 80 percent
of source rows. `_loaded_at` records ingestion time and includes late arrivals.
