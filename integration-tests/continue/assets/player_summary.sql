/* @bruin

name: chess_playground.player_summary
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.game_outcome_summary
   - chess_playground.player_profile_summary
   - chess_playground.games
   - chess_playground.profiles

columns:
  - name: username
    type: string
    description: "Username of the player"
  - name: total_games
    type: integer
    description: "Total games played by the player"
    checks:
      - name: non_negative
  - name: total_wins
    type: integer
    description: "Total games won by the player"
  - name: win_rate
    type: float
    description: "Win rate of the player"
tags:
    - include
    - exclude
@bruin */


        g.white->>'@id' AS white_aid,
    g.black->>'@id' AS black_aid
FROM chess_playground.games g
    )

SELECT
    p.username,
    p.aid,
    COUNT(*) AS total_games,
    COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) +
    COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) AS total_wins,
    ROUND(
            (COUNT(CASE WHEN g.white_aid = p.aid AND g.winner_aid = p.aid THEN 1 END) +
             COUNT(CASE WHEN g.black_aid = p.aid AND g.winner_aid = p.aid THEN 1 END)) * 100.0 /
            NULLIF(COUNT(*), 0),
            2
    ) AS win_rate
FROM chess_playground.profiles p
         LEFT JOIN game_results g
                   ON p.aid IN (g.white_aid, g.black_aid)
GROUP BY p.username, p.aid
ORDER BY total_games DESC;
