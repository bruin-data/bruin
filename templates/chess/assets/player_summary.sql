/* @bruin

name: chess_playground.player_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.games
   - chess_playground.profiles

@bruin */

WITH game_results AS (
    SELECT
        CASE
            WHEN g.white__result = 'win' THEN g.white__aid
            WHEN g.black__result = 'win' THEN g.black__aid
            ELSE NULL
            END AS winner_aid,
        g.white__aid AS white_aid,
        g.black__aid AS black_aid
    FROM chess_playground.games g
)

SELECT
    p.username,
    p.aid,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS white_wins,
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS black_wins,
    COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END) AS white_games,
    COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END) AS black_games,
    ROUND(COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END), 0), 2) AS white_win_rate,
    ROUND(COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END), 0), 2) AS black_win_rate
FROM chess_playground.profiles p
LEFT JOIN game_results g
       ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
ORDER BY total_games DESC
