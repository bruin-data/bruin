/* @bruin

name: dataset.player_stats
type: duckdb.sql
materialization:
  type: table
   
depends:
   - dataset.players


# you can define column metadata and quality checks
columns: 
  - name: name 
    type: string
    description: this column contains the player names 
    checks: 
      - name: not_null 
      - name: unique 
  - name: player_count 
    type: int 
    description: the number of players with the given name
    checks: 
      - name: not_null 
      - name: positive 

# you can also define custom checks 
custom_checks:
  - name: row count is greater than zero 
    description: this check ensures that the table is not empty 
    query: SELECT count(*) > 1 FROM dataset.player_stats
    value: 1

@bruin */

SELECT name, count(*) AS player_count
FROM dataset.players
GROUP BY 1