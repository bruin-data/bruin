/* @bruin
name: example
type: duckdb.sql

depends:
  - asset1               # this one waits
  - asset: asset2        # this also waits
    mode: symbolic


  - asset: my-other-asset   # this does not wait
    mode: symbolic


   @bruin */

SELECT 1 