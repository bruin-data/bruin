/* @bruin

name: chess_playground.game_results
type: duckdb.sql
materialization:
   type: table

depends:
   - chess_playground.games
tags:
   - include

@bruin */

SELECT
    CASE
        WHEN g.white__result = 'win' THEN g.white__aid
        WHEN g.black__result = 'win' THEN g.black__aid
        ELSE NULL
        END AS winner_aid,
    g.white__aid AS white_aid,
    g.black__aid AS black_aid
FROM chess_playground.games g;
