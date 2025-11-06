/* @bruin
name: test.books
type: duckdb.sql
materialization:
  type: table
  strategy: truncate+insert
columns:
  - name: book_id
    type: INTEGER
    primary_key: true
  - name: title
    type: VARCHAR
  - name: author
    type: VARCHAR
  - name: year
    type: INTEGER
@bruin */

SELECT 1 AS book_id, 'The Great Gatsby' AS title, 'F. Scott Fitzgerald' AS author, 1925 AS year
UNION ALL
SELECT 2 AS book_id, '1984' AS title, 'George Orwell' AS author, 1949 AS year
UNION ALL
SELECT 3 AS book_id, 'To Kill a Mockingbird' AS title, 'Harper Lee' AS author, 1960 AS year
