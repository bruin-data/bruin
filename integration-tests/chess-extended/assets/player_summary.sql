/* @bruin

name: chess_playground.player_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.game_results
   - chess_playground.profiles

columns:
  - name: total_games
    type: integer
    description: "the games"
    checks:
      - name: positive
tags:
    - include

@bruin */

SELECT
    p.username,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS white_wins,
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS black_wins,
    COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END) AS white_games,
    COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END) AS black_games,
    ROUND(COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.white_aid = p.aid THEN 1 END), 0), 2) AS white_win_rate,
    ROUND(COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) * 100.0 / NULLIF(COUNT(CASE WHEN g.black_aid = p.aid THEN 1 END), 0), 2) AS black_win_rate
FROM chess_playground.profiles p
         LEFT JOIN chess_playground.game_results g
                   ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
ORDER BY total_games DESC;

