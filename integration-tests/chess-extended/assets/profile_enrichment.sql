/* @bruin

name: chess_playground.profile_enrichment
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.player_summary

@bruin */

SELECT
    ps.username,
    ps.total_games,
    ps.white_win_rate,
    ps.black_win_rate,
    CASE
        WHEN ps.white_win_rate + ps.black_win_rate > 75 THEN 'Expert'
        WHEN ps.white_win_rate + ps.black_win_rate > 50 THEN 'Intermediate'
        ELSE 'Beginner'
        END AS performance_level
FROM chess_playground.player_summary ps;
