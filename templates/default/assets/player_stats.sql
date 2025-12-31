/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players

@bruin */

SELECT name, count(*)
FROM dataset.players
GROUP BY 1