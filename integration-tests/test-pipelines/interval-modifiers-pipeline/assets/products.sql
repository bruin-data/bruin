/* @bruin

type: duckdb.sql

materialization:
  type: table
  strategy: create+replace

columns:
  - name: product_id
    type: INTEGER
    description: Unique identifier for the product
    primary_key: true
  - name: product_name
    type: VARCHAR
    description: Name of the product
  - name: price
    type: FLOAT
    description: Price of the product in USD
    checks:
      - name: positive
  - name: stock
    type: INTEGER
    description: Number of units in stock
interval_modifiers:
  start: -2h
  end: -2h

@bruin */

with t2 as (
    select
        1 as product_id,
        'Laptop' as product_name,
        799 as price,
        50 as stock,
        TIMESTAMP '2025-04-02 08:00:00' as dt
    union all
    select
        2 as product_id,
        'Smartphone' as product_name,
        599 as price,
        40 as stock,
        TIMESTAMP '2025-04-02 09:00:00' as dt
    union all
    select
        3 as product_id,
        'Headphones' as product_name,
        399 as price,
        100 as stock,
        TIMESTAMP '2025-04-02 10:00:00' as dt
    union all
    select
        4 as product_id,
        'Monitor' as product_name,
        199 as price,
        25 as stock,
        TIMESTAMP '2025-04-02 11:00:00' as dt
    union all
    select
        5 as product_id,
        'Keyboard' as product_name,
        35 as price,
        150 as stock,
        TIMESTAMP '2025-04-02 12:00:00' as dt
    union all
    select
        6 as product_id,
        'Mouse' as product_name,
        29 as price,
        120 as stock,
        TIMESTAMP '2025-04-02 13:00:00' as dt
    union all
    select
        7 as product_id,
        'Tablet' as product_name,
        399 as price,
        40 as stock,
        TIMESTAMP '2025-04-02 14:00:00' as dt
    union all
    select
        8 as product_id,
        'Smartwatch' as product_name,
        249 as price,
        70 as stock,
        TIMESTAMP '2025-04-02 15:00:00' as dt
    union all
    select
        9 as product_id,
        'Gaming Console' as product_name,
        499.99 as price,
        20 as stock,
        TIMESTAMP '2025-04-02 16:00:00' as dt

)

select
    product_id,
    product_name,
    stock,
    price,
    dt
from t2
where dt between '{{ start_timestamp }}' and '{{ end_timestamp }}'
