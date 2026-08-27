/* @bruin
name: products
type: duckdb.sql

description: >-
  One row per product, sixty in total, spread across eight categories and twenty
  subcategories. A dimension table written out by hand so the product names read
  like real ones. Generated sample data for the Agentic Data Analysis course.
  Deterministic: the same rows are produced on every run.

materialization:
  type: table
  strategy: create+replace

columns:
  - name: product_id
    type: integer
    description: "Unique identifier for the product. One row per product_id. Primary key."
    primary_key: true
    checks:
      - name: unique
      - name: not_null
  - name: product_code
    type: varchar
    description: "Short human-readable product code, e.g. 'ELEC-001'."
  - name: product_name
    type: varchar
    description: "Display name of the product."
  - name: brand
    type: varchar
    description: "Brand the product is sold under."
  - name: category_name
    type: varchar
    description: "Top-level category. There are eight."
  - name: subcategory_name
    type: varchar
    description: "Subcategory within the category. There are twenty."
  - name: color
    type: varchar
    description: "Primary colour of the product."
  - name: unit_cost
    type: decimal
    description: "What the product costs the retailer. Always less than list_price."
  - name: list_price
    type: decimal
    description: "The catalogue price before any discount."
@bruin */

-- Hand-written dimension. Sixty products. Deterministic by construction.
-- Note: order lines reference products by product_id. A small number of order
-- lines point at a product_id that is NOT in this table on purpose - see
-- docs/known-defects.md.

SELECT * FROM (
    VALUES
        (1,  'ELEC-001', 'Aurora Pico 5 Smartphone',      'Aurora',    'Electronics',     'Phones',              'Black',   320.00,  699.00),
        (2,  'ELEC-002', 'Aurora Pico 5 Pro Smartphone',  'Aurora',    'Electronics',     'Phones',              'Graphite', 430.00,  999.00),
        (3,  'ELEC-003', 'Cobalt One Smartphone',         'Cobalt',    'Electronics',     'Phones',              'Blue',    240.00,  499.00),
        (4,  'ELEC-004', 'Vertex Air 13 Laptop',          'Vertex',    'Electronics',     'Laptops',             'Silver',  620.00, 1199.00),
        (5,  'ELEC-005', 'Vertex Pro 15 Laptop',          'Vertex',    'Electronics',     'Laptops',             'Space Grey', 880.00, 1699.00),
        (6,  'ELEC-006', 'Cobalt Book 14 Laptop',         'Cobalt',    'Electronics',     'Laptops',             'Silver',  510.00,  949.00),
        (7,  'ELEC-007', 'Lumen Buds Wireless',           'Lumen',     'Electronics',     'Headphones',          'White',    45.00,  129.00),
        (8,  'ELEC-008', 'Lumen Over-Ear Headphones',     'Lumen',     'Electronics',     'Headphones',          'Black',    88.00,  219.00),
        (9,  'ELEC-009', 'Aurora Sound Earbuds',          'Aurora',    'Electronics',     'Headphones',          'Navy',     52.00,  139.00),
        (10, 'ELEC-010', 'Vertex Tab 10 Tablet',          'Vertex',    'Electronics',     'Tablets',             'Silver',  190.00,  449.00),
        (11, 'ELEC-011', 'Cobalt Slate Mini Tablet',      'Cobalt',    'Electronics',     'Tablets',             'Grey',    150.00,  329.00),
        (12, 'ELEC-012', 'Aurora Pad 11 Tablet',          'Aurora',    'Electronics',     'Tablets',             'Rose',    260.00,  599.00),

        (13, 'HOME-001', 'Meadow 10-Piece Cookware Set',  'Meadow',    'Home & Kitchen',  'Cookware',            'Steel',   110.00,  249.00),
        (14, 'HOME-002', 'Meadow Cast Iron Skillet',      'Meadow',    'Home & Kitchen',  'Cookware',            'Black',    28.00,   69.00),
        (15, 'HOME-003', 'Harbor Chef Knife 8in',         'Harbor',    'Home & Kitchen',  'Cookware',            'Silver',   34.00,   89.00),
        (16, 'HOME-004', 'Everly Blender Pro',            'Everly',    'Home & Kitchen',  'Small Appliances',    'Black',    62.00,  149.00),
        (17, 'HOME-005', 'Everly Espresso Machine',       'Everly',    'Home & Kitchen',  'Small Appliances',    'Stainless', 180.00,  399.00),
        (18, 'HOME-006', 'Everly Air Fryer 5L',           'Everly',    'Home & Kitchen',  'Small Appliances',    'Black',    58.00,  129.00),
        (19, 'HOME-007', 'Harbor Storage Bins 6-Pack',    'Harbor',    'Home & Kitchen',  'Storage',             'Clear',    18.00,   39.00),
        (20, 'HOME-008', 'Meadow Glass Food Containers',  'Meadow',    'Home & Kitchen',  'Storage',             'Clear',    15.00,   34.00),
        (21, 'HOME-009', 'Harbor Pantry Organizer',       'Harbor',    'Home & Kitchen',  'Storage',             'White',    22.00,   49.00),

        (22, 'APRL-001', 'NorthPeak Men''s Rain Jacket',  'NorthPeak', 'Apparel',         'Menswear',            'Green',    40.00,   99.00),
        (23, 'APRL-002', 'NorthPeak Men''s Hoodie',       'NorthPeak', 'Apparel',         'Menswear',            'Grey',     22.00,   59.00),
        (24, 'APRL-003', 'Everly Men''s Chino Pants',     'Everly',    'Apparel',         'Menswear',            'Khaki',    18.00,   49.00),
        (25, 'APRL-004', 'NorthPeak Women''s Parka',      'NorthPeak', 'Apparel',         'Womenswear',          'Red',      55.00,  139.00),
        (26, 'APRL-005', 'Everly Women''s Cardigan',      'Everly',    'Apparel',         'Womenswear',          'Cream',    20.00,   54.00),
        (27, 'APRL-006', 'Everly Women''s Leggings',      'Everly',    'Apparel',         'Womenswear',          'Black',    12.00,   34.00),
        (28, 'APRL-007', 'NorthPeak Trail Runner Shoes',  'NorthPeak', 'Apparel',         'Footwear',            'Blue',     44.00,  109.00),
        (29, 'APRL-008', 'NorthPeak Hiking Boots',        'NorthPeak', 'Apparel',         'Footwear',            'Brown',    58.00,  149.00),
        (30, 'APRL-009', 'Cobalt Canvas Sneakers',        'Cobalt',    'Apparel',         'Footwear',            'White',    24.00,   64.00),
        (31, 'APRL-010', 'Everly Wool Socks 3-Pack',      'Everly',    'Apparel',         'Footwear',            'Grey',      8.00,   19.00),

        (32, 'SPRT-001', 'Vertex Adjustable Dumbbells',   'Vertex',    'Sports & Outdoors', 'Fitness',           'Black',    90.00,  199.00),
        (33, 'SPRT-002', 'Vertex Yoga Mat',               'Vertex',    'Sports & Outdoors', 'Fitness',           'Purple',   14.00,   39.00),
        (34, 'SPRT-003', 'Vertex Resistance Bands Set',   'Vertex',    'Sports & Outdoors', 'Fitness',           'Assorted', 10.00,   29.00),
        (35, 'SPRT-004', 'NorthPeak 2-Person Tent',       'NorthPeak', 'Sports & Outdoors', 'Camping',           'Orange',   85.00,  199.00),
        (36, 'SPRT-005', 'NorthPeak Sleeping Bag',        'NorthPeak', 'Sports & Outdoors', 'Camping',           'Green',    42.00,   99.00),
        (37, 'SPRT-006', 'NorthPeak Camp Stove',          'NorthPeak', 'Sports & Outdoors', 'Camping',           'Silver',   36.00,   79.00),
        (38, 'SPRT-007', 'NorthPeak Headlamp',            'NorthPeak', 'Sports & Outdoors', 'Camping',           'Black',    12.00,   29.00),

        (39, 'BOOK-001', 'The Glass Harbor',              'Meadow Press', 'Books',         'Fiction',             'Multi',     4.00,   16.00),
        (40, 'BOOK-002', 'Northern Lights',               'Meadow Press', 'Books',         'Fiction',             'Multi',     4.50,   18.00),
        (41, 'BOOK-003', 'The Quiet Tide',                'Meadow Press', 'Books',         'Fiction',             'Multi',     4.00,   15.00),
        (42, 'BOOK-004', 'Data for Everyone',             'Meadow Press', 'Books',         'Non-Fiction',         'Multi',     6.00,   28.00),
        (43, 'BOOK-005', 'The Analyst''s Handbook',       'Meadow Press', 'Books',         'Non-Fiction',         'Multi',     7.00,   34.00),
        (44, 'BOOK-006', 'Cooking Simply',                'Meadow Press', 'Books',         'Non-Fiction',         'Multi',     5.50,   24.00),

        (45, 'TOYS-001', 'Harbor Quest Board Game',       'Harbor',    'Toys & Games',    'Board Games',         'Multi',    16.00,   39.00),
        (46, 'TOYS-002', 'Settlers of Meadow',            'Meadow',    'Toys & Games',    'Board Games',         'Multi',    18.00,   44.00),
        (47, 'TOYS-003', 'Cobalt Chess Set',              'Cobalt',    'Toys & Games',    'Board Games',         'Wood',     20.00,   49.00),
        (48, 'TOYS-004', 'Aurora 1000-Piece Puzzle',      'Aurora',    'Toys & Games',    'Puzzles',             'Multi',     7.00,   19.00),
        (49, 'TOYS-005', 'Meadow Wooden Puzzle',          'Meadow',    'Toys & Games',    'Puzzles',             'Wood',      9.00,   24.00),
        (50, 'TOYS-006', 'Lumen Brain Teaser',            'Lumen',     'Toys & Games',    'Puzzles',             'Multi',     6.00,   16.00),

        (51, 'BTY-001',  'Everly Daily Moisturizer',      'Everly',    'Beauty',          'Skincare',            'White',     8.00,   24.00),
        (52, 'BTY-002',  'Everly Vitamin C Serum',        'Everly',    'Beauty',          'Skincare',            'Amber',    11.00,   34.00),
        (53, 'BTY-003',  'Everly Sunscreen SPF50',        'Everly',    'Beauty',          'Skincare',            'White',     6.00,   18.00),
        (54, 'BTY-004',  'Aurora Eau de Parfum',          'Aurora',    'Beauty',          'Fragrance',           'Gold',     30.00,   79.00),
        (55, 'BTY-005',  'Harbor Cologne',                'Harbor',    'Beauty',          'Fragrance',           'Blue',     26.00,   69.00),

        (56, 'GRDN-001', 'Meadow Pruning Shears',         'Meadow',    'Garden',          'Garden Tools',        'Green',    12.00,   29.00),
        (57, 'GRDN-002', 'Meadow Garden Trowel',          'Meadow',    'Garden',          'Garden Tools',        'Green',     6.00,   16.00),
        (58, 'GRDN-003', 'Meadow Hose 50ft',              'Meadow',    'Garden',          'Garden Tools',        'Green',    18.00,   44.00),
        (59, 'GRDN-004', 'Harbor Patio Chair',            'Harbor',    'Garden',          'Outdoor Furniture',   'Grey',     48.00,  119.00),
        (60, 'GRDN-005', 'Harbor Outdoor Table',          'Harbor',    'Garden',          'Outdoor Furniture',   'Brown',    95.00,  219.00)
) AS p(product_id, product_code, product_name, brand, category_name, subcategory_name, color, unit_cost, list_price)
ORDER BY product_id;
