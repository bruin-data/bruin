/* @bruin

name: chess_playground.player_profile_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.profiles
columns:
  - name: total_players
    type: integer
    description: "Total number of players in the profiles table"
    checks:
      - name: positive
  - name: active_players
    type: integer
    description: "Number of players marked as active"
  - name: inactive_players
    type: integer
    description: "Number of players marked as inactive"
tags:
    - exclude
@bruin */

SELECT
    COUNT(*) AS total_players,
    COUNT(CASE WHEN p.status = 'active' THEN 1 END) AS active_players,
    COUNT(CASE WHEN p.status = 'inactive' THEN 1 END) AS inactive_players
FROM chess_playground.profiles p;
