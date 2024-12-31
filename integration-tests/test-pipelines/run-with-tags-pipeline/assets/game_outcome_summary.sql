/* @bruin

name: chess_playground.game_outcome_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.games

columns:
  - name: result_type
    type: string
    description: "Type of game result (win, draw, etc.)"
  - name: total_games
    type: integer
    description: "Total number of games with this result"
    checks:
      - name: positive
tags:
   - include

@bruin */

SELECT
    g.white->>'result' AS result_type,
    COUNT(*) AS total_games
FROM chess_playground.games g
WHERE g.white->>'result' IS NOT NULL
GROUP BY g.white->>'result'
ORDER BY total_games DESC;
