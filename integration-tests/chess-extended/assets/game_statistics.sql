/* @bruin

name: chess_playground.game_statistics
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.game_results

@bruin */

SELECT
    COUNT(*) AS total_games,
    COUNT(CASE WHEN winner_aid IS NOT NULL THEN 1 END) AS games_with_winners,
    ROUND(COUNT(CASE WHEN winner_aid IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS win_rate
FROM chess_playground.game_results;
