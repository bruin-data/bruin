# Schema

Six tables. Read this before you query - it is cheaper than asking the database to
describe itself every time. To see them from the command line:

```sql
SHOW TABLES;
```

## What "grain" means

The **grain** of a table is what one row of it stands for. `orders` has one row per
order, so its grain is "one order". `order_items` has one row per line on an order,
so its grain is "one order line". Getting the grain wrong is the most common way to
produce a number that looks right and is not, which is why the word comes up
constantly below and throughout the course. Before you write or trust any query, say
out loud what one row of the result represents.

## How the tables relate

```text
                         dates
                     (one row per calendar day)
                              ^
                              | ordered_at::date
                              |
   stores ---<  orders  >--- customers
  (one row/store)   (one row per order)     (one row/customer*)
                              |
                              | order_id  (1 order -> ~2.4 lines)
                              v
                       order_items
                     (one row per order line)
                              |
                              | product_id
                              v
                        products
                     (one row per product)

   * customers has 10 duplicated customer_ids on purpose.
```

The two grains to keep straight: `orders` is one row per order,
`order_items` is one row per line. Joining them multiplies each order by
its line count (about 2.4), so an order-header measure like `order_total` must not
be summed across that join.

## dates - 1,096 rows, one row per calendar day

| Column | Type | Notes |
|---|---|---|
| `date_day` | date | The day. Primary key. |
| `date_key` | integer | The day as `YYYYMMDD`. |
| `year` | integer | e.g. 2024. |
| `quarter` | integer | 1-4. |
| `year_month` | varchar | e.g. `2024-07`. |
| `month_number` | integer | 1-12. |
| `month_name` | varchar | e.g. `July`. |
| `iso_week` | integer | ISO week. 1-52 across the three years in this data. |
| `day_of_week_number` | integer | 1 = Monday ... 7 = Sunday. |
| `day_of_week_name` | varchar | e.g. `Monday`. |
| `is_working_day` | boolean | True Mon-Fri. |

## stores - 6 rows, one row per store

| Column | Type | Notes |
|---|---|---|
| `store_id` | integer | Primary key. |
| `store_code` | varchar | e.g. `NYC01`. |
| `country_code` | varchar | Two letters. |
| `country_name` | varchar | Full country name. |
| `city` | varchar | Store city. |
| `opened_on` | date | Opening date. |
| `closed_on` | date | Closing date, or NULL if still open. One store (Paris) is closed, on a date after the last order. |
| `status` | varchar | `open` or `closed`. |
| `timezone` | varchar | IANA timezone. |

## products - 60 rows, one row per product

8 categories, 20 subcategories.

| Column | Type | Notes |
|---|---|---|
| `product_id` | integer | Primary key. |
| `product_code` | varchar | e.g. `ELEC-001`. |
| `product_name` | varchar | Display name. |
| `brand` | varchar | Brand. |
| `category_name` | varchar | One of 8 categories. |
| `subcategory_name` | varchar | One of 20 subcategories. |
| `color` | varchar | Primary colour. |
| `unit_cost` | decimal(5,2) | Cost to the retailer. |
| `list_price` | decimal | Catalogue price. |

## customers - 510 rows, one row per customer (500 distinct)

`customer_id` is meant to be unique but 10 ids are duplicated on purpose, so this
table has 510 rows. 40 of the 500 customers (ids 461-500) have never placed an
order, so a LEFT JOIN from customers to orders returns rows an INNER JOIN drops.

| Column | Type | Notes |
|---|---|---|
| `customer_id` | integer | Meant to be one per customer, but NOT unique here. |
| `first_name` | varchar | |
| `last_name` | varchar | |
| `city` | varchar | |
| `state` | varchar | State or region. |
| `country` | varchar | One of 12 countries. |
| `signed_up_on` | date | Account creation date. |
| `segment` | varchar | `consumer`, `small_business`, or `enterprise`. |

## orders - 1,200 rows, one row per order

| Column | Type | Notes |
|---|---|---|
| `order_id` | integer | Primary key. |
| `customer_id` | integer | Joins to `customers`. |
| `store_id` | integer | Joins to `stores`. |
| `ordered_at` | timestamp | When the order was placed (store-local). Business time. |
| `_loaded_at` | timestamp | When the row was recorded by the source. Ingestion time - not the same as `ordered_at`. |
| `promised_delivery_date` | date | Promised delivery, NULL on a few orders. |
| `currency_code` | varchar | One of USD, EUR, GBP, CAD, AUD. |
| `order_status` | varchar | Lifecycle status. NULL on 24 orders. |
| `order_total` | decimal(10,2) | Order-header total. Do NOT sum across a join to lines. It is a figure the source system supplies, and it does **not** equal the sum of the order's lines - see the note below. |

### `order_total` does not reconcile with the lines

`orders.order_total` and `SUM(quantity * net_price)` from `order_items` are two
different numbers and they will never agree: 604,065.00 against 851,617.69 across the
whole table, and on a single order they can be far apart. That is not a bug in your
query. This is sample data, and `order_total` is generated as its own value rather
than being derived from the lines. Real datasets have this problem too, usually
because the header captures discounts, shipping or tax that the lines do not. Pick
one definition of revenue, say which one you picked, and stay with it.

## order_items - 2,880 rows, one row per order line

Exactly 2.4 lines per order on average. Prices are read from `products`,
so `unit_price` really is that product's catalogue price and `unit_cost` really
is what it cost the retailer.

| Column | Type | Notes |
|---|---|---|
| `order_id` | integer | Joins to `orders`. Several lines share one. |
| `line_number` | integer | Line position within the order, from 1. |
| `product_id` | integer | Joins to `products`. 15 lines have an id that is not in products. |
| `quantity` | integer | Units on the line. |
| `unit_price` | decimal(6,2) | The product's catalogue price at the time of sale, copied from `products.list_price`. Before discount. |
| `net_price` | decimal(10,2) | Price actually charged per unit, after a discount of 0-24%. Use this for revenue. |
| `unit_cost` | decimal(5,2) | What the unit cost the retailer, copied from `products.unit_cost`. NULL on 57 lines. |
