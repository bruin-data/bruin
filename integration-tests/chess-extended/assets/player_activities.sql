/* @bruin

name: chess_playground.player_activities
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.game_results

tags:
   - include
   - exclude

@bruin */

SELECT
    g.white_aid AS player_aid,
    COUNT(*) AS white_games,
    COUNT(CASE WHEN g.winner_aid = g.white_aid THEN 1 END) AS white_wins
FROM chess_playground.game_results g
WHERE g.white_aid IN ('https://api.chess.com/pub/player/hikaru', 'https://api.chess.com/pub/player/magnuscarlsen')
GROUP BY g.white_aid

UNION ALL

SELECT
    g.black_aid AS player_aid,
    COUNT(*) AS black_games,
    COUNT(CASE WHEN g.winner_aid = g.black_aid THEN 1 END) AS black_wins
FROM chess_playground.game_results g
WHERE g.black_aid IN ('https://api.chess.com/pub/player/hikaru', 'https://api.chess.com/pub/player/magnuscarlsen')
GROUP BY g.black_aid;
