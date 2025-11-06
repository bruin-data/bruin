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

SELECT 10 AS book_id, 'Harry Potter' AS title, 'J.K. Rowling' AS author, 1997 AS year
UNION ALL
SELECT 20 AS book_id, 'The Hobbit' AS title, 'J.R.R. Tolkien' AS author, 1937 AS year
