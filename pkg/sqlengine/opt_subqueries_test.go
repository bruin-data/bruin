package sqlengine

import "testing"

// Parity cases for UnnestSubqueries / MergeSubqueries. Inputs are already qualified (by Python
// sqlglot 30.13.0 qualify / the optimizer pipeline); expected outputs come from running the Python
// rule on the same SQL text with the default dialect.
var optSubqueriesCases = []struct {
	rule string
	lti  bool
	sql  string
	want string
}{
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a = (SELECT SUM(y.a) AS a FROM y)",
		"SELECT * FROM x CROSS JOIN (SELECT SUM(y.a) AS a FROM y) AS _u_0 WHERE x.a = _u_0.a",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y)",
		"SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT y.b AS b FROM y)",
		"SELECT * FROM x LEFT JOIN (SELECT y.b AS b FROM y GROUP BY y.b) AS _u_0 ON x.a = _u_0.b WHERE NOT _u_0.b IS NULL",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a = ANY (SELECT y.a AS a FROM y)",
		"SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a = (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a)",
		"SELECT * FROM x LEFT JOIN (SELECT SUM(y.b) AS b, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0._u_1 WHERE x.a = _u_0.b",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a > (SELECT SUM(y.b) AS b FROM y WHERE x.a = y.a)",
		"SELECT * FROM x LEFT JOIN (SELECT SUM(y.b) AS b, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0._u_1 WHERE x.a > _u_0.b",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a <> ANY (SELECT y.a AS a FROM y WHERE y.a = x.a)",
		"SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0.a = x.a WHERE x.a <> _u_0.a",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y WHERE y.a = x.a)",
		"SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0.a = x.a WHERE NOT x.a = _u_0.a",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y WHERE y.b = x.a)",
		"SELECT * FROM x LEFT JOIN (SELECT ARRAY_AGG(y.a) AS a, y.b AS _u_1 FROM y WHERE TRUE GROUP BY y.b) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ANY(_u_0.a, _x -> _x = x.a)",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a < (SELECT SUM(y.a) AS a FROM y WHERE y.a = x.a and y.a = x.b and y.b <> x.d)",
		"SELECT * FROM x LEFT JOIN (SELECT SUM(y.a) AS a, y.a AS _u_1, ARRAY_AGG(y.b) AS _u_2 FROM y WHERE TRUE AND TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a AND _u_0._u_1 = x.b WHERE (x.a < _u_0.a AND ARRAY_ANY(_u_0._u_2, _x -> _x <> x.d))",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE EXISTS (SELECT y.a AS a, y.b AS b FROM y WHERE x.a = y.a)",
		"SELECT * FROM x LEFT JOIN (SELECT y.a AS a FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a > ALL (SELECT y.c AS c FROM y WHERE y.a = x.a)",
		"SELECT * FROM x LEFT JOIN (SELECT ARRAY_AGG(y.c) AS c, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ALL(_u_0.c, _x -> x.a > _x)",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a > (SELECT COUNT(*) as d FROM y WHERE y.a = x.a)",
		"SELECT * FROM x LEFT JOIN (SELECT COUNT(*) AS d, y.a AS _u_1 FROM y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE x.a > COALESCE(_u_0.d, 0)",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT max(y.b) AS b FROM y GROUP BY y.a)",
		"SELECT * FROM x LEFT JOIN (SELECT _q.b AS b FROM (SELECT MAX(y.b) AS b FROM y GROUP BY y.a) AS _q GROUP BY _q.b) AS _u_0 ON x.a = _u_0.b WHERE NOT _u_0.b IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a > (SELECT SUM(y.a) AS b FROM y) FROM x",
		"SELECT x.a > _u_0.b FROM x CROSS JOIN (SELECT SUM(y.a) AS b FROM y) AS _u_0",
	},
	{
		"unnest", false,
		"SELECT (SELECT MAX(t2.c1) AS c1 FROM t2 WHERE t2.c2 = t1.c2 AND t2.c3 <= TRUNC(t1.c3)) AS c FROM t1",
		"SELECT _u_0.c1 AS c FROM t1 LEFT JOIN (SELECT MAX(t2.c1) AS c1, t2.c2 AS _u_1, MAX(t2.c3) AS _u_2 FROM t2 WHERE TRUE AND TRUE GROUP BY t2.c2) AS _u_0 ON _u_0._u_1 = t1.c2 WHERE _u_0._u_2 <= TRUNC(t1.c3)",
	},
	{
		"unnest", false,
		"SELECT s.t AS t FROM s WHERE 1 IN (SELECT t.a AS a FROM t WHERE t.b > 1)",
		"SELECT s.t AS t FROM s LEFT JOIN (SELECT t.a AS a FROM t WHERE t.b > 1 GROUP BY t.a) AS _u_0 ON 1 = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT s.t FROM s WHERE 1 IN (SELECT MAX(t.a) AS t1 FROM t)",
		"SELECT s.t FROM s LEFT JOIN (SELECT MAX(t.a) AS t1 FROM t) AS _u_0 ON 1 = _u_0.t1 WHERE NOT _u_0.t1 IS NULL",
	},
	{
		"unnest", false,
		"SELECT s.t FROM s WHERE 1 IN (SELECT MAX(t.a) + 1 AS t1 FROM t)",
		"SELECT s.t FROM s LEFT JOIN (SELECT MAX(t.a) + 1 AS t1 FROM t) AS _u_0 ON 1 = _u_0.t1 WHERE NOT _u_0.t1 IS NULL",
	},
	{
		"unnest", false,
		"SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0 FROM t1, t0 GROUP BY t0.c2",
		"SELECT NOT MAX(_u_0.\"1\") IS NULL AS ref0 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE GROUP BY t0.c2",
	},
	{
		"unnest", false,
		"SELECT EXISTS (SELECT 1 WHERE TRUE) AS ref0 FROM t1, t0 GROUP BY t0.c2",
		"SELECT NOT MAX(_u_0.\"1\") IS NULL AS ref0 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE TRUE) AS _u_0 ON TRUE GROUP BY t0.c2",
	},
	{
		"unnest", false,
		"SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0, EXISTS (SELECT 1 WHERE TRUE) AS ref1 FROM t1, t0 GROUP BY t0.c2",
		"SELECT NOT MAX(_u_0.\"1\") IS NULL AS ref0, NOT MAX(_u_1.\"1\") IS NULL AS ref1 FROM t1, t0 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE LEFT JOIN (SELECT 1 WHERE TRUE) AS _u_1 ON TRUE GROUP BY t0.c2",
	},
	{
		"unnest", false,
		"SELECT EXISTS (SELECT 1 WHERE FALSE) AS ref0 FROM t1 GROUP BY t1.c0 HAVING COUNT(*) > 0",
		"SELECT NOT MAX(_u_0.\"1\") IS NULL AS ref0 FROM t1 LEFT JOIN (SELECT 1 WHERE FALSE) AS _u_0 ON TRUE GROUP BY t1.c0 HAVING COUNT(*) > 0",
	},
	{
		"unnest", false,
		"SELECT t1.c1 > (SELECT SUM(y.a) AS b FROM y) FROM x JOIN GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1) ON t1.c1 > x.a",
		"SELECT t1.c1 > _u_0.b FROM x JOIN GENERATE_SERIES((SELECT MAX(x.a) FROM x AS x), 10, 1) AS t1(c1) ON t1.c1 > x.a CROSS JOIN (SELECT SUM(y.a) AS b FROM y) AS _u_0",
	},
	{
		"unnest", false,
		"SELECT COALESCE((SELECT MAX(b.val) FROM t b WHERE b.val < a.val AND b.id = a.id), a.val) AS result FROM t a",
		"SELECT COALESCE((SELECT MAX(b.val) FROM t AS b WHERE b.val < a.val AND b.id = a.id), a.val) AS result FROM t AS a",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z)",
		"SELECT * FROM x LEFT JOIN (SELECT _u_0.a AS a FROM (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z) AS _u_0 GROUP BY _u_0.a) AS _u_1 ON x.a = _u_1.a WHERE NOT _u_1.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y)",
		"SELECT * FROM x WHERE NOT x.a IN (SELECT y.a AS a FROM y)",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a NOT IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z)",
		"SELECT * FROM x WHERE NOT x.a IN (SELECT y.a AS a FROM y UNION ALL SELECT z.a AS a FROM z)",
	},
	{
		"merge", false,
		"SELECT _0.a AS a, _0.b AS b FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS _0",
		"SELECT x.a AS a, x.b AS b FROM x AS x",
	},
	{
		"merge", false,
		"SELECT _0.c * 2 AS d FROM (SELECT x.a + x.b AS c FROM x AS x) AS _0",
		"SELECT (x.a + x.b) * 2 AS d FROM x AS x",
	},
	{
		"merge", false,
		"SELECT _0.c + _0.d AS e FROM (SELECT x.a + x.b AS c, x.a AS d FROM x AS x) AS _0",
		"SELECT (x.a + x.b) + x.a AS e FROM x AS x",
	},
	{
		"merge", false,
		"WITH cte AS (SELECT x.a * x.b AS c, x.a AS d FROM x AS x) SELECT cte.c + cte.d AS e FROM cte AS cte",
		"SELECT (x.a * x.b) + x.a AS e FROM x AS x",
	},
	{
		"merge", false,
		"SELECT 2 * _0.foo AS bar FROM (SELECT CAST(x.b AS DOUBLE) AS foo FROM x AS x) AS _0",
		"SELECT 2 * CAST(x.b AS DOUBLE) AS bar FROM x AS x",
	},
	{
		"merge", false,
		"SELECT _0.foo * 2 AS bar FROM (SELECT (1 + 2 + 3) AS foo FROM x AS x) AS _0",
		"SELECT (1 + 2 + 3) * 2 AS bar FROM x AS x",
	},
	{
		"merge", false,
		"SELECT r.a AS a, r.b AS b FROM (SELECT q.a AS a, q.b AS b FROM x AS q) AS r",
		"SELECT q.a AS a, q.b AS b FROM x AS q",
	},
	{
		"merge", false,
		"SELECT _1.a AS a, _1.b AS b FROM (SELECT _0.a AS a, _0.b AS b FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS _0) AS _1",
		"SELECT x.a AS a, x.b AS b FROM x AS x",
	},
	{
		"merge", false,
		"SELECT _0.a AS a, SUM(_0.b) AS b FROM (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.a > 1) AS _0 GROUP BY _0.a",
		"SELECT x.a AS a, SUM(x.b) AS b FROM x AS x WHERE x.a > 1 GROUP BY x.a",
	},
	{
		"merge", false,
		"SELECT x.a AS a, y.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.a > 1) AS x JOIN y AS y ON x.b = y.b",
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1",
	},
	{
		"merge", false,
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN (SELECT y.b AS b, y.c AS c FROM y AS y) AS y ON x.b = y.b",
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT _0.a AS a, _0.c AS c FROM (SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b) AS _0",
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT x.a AS a, q.c AS c FROM (SELECT q.a AS a, q.b AS b FROM x AS q) AS x JOIN y AS q ON x.b = q.b",
		"SELECT q_2.a AS a, q.c AS c FROM x AS q_2 JOIN y AS q ON q_2.b = q.b",
	},
	{
		"merge", false,
		"SELECT x.a AS a, q.c AS c FROM (SELECT x.a AS a, x.b AS b FROM x AS x JOIN y AS q ON x.b = q.b) AS x JOIN y AS q ON x.b = q.b",
		"SELECT x.a AS a, q.c AS c FROM x AS x JOIN y AS q_2 ON x.b = q_2.b JOIN y AS q ON x.b = q.b",
	},
	{
		"merge", false,
		"SELECT x.a AS a, q.c AS c, r.c AS c FROM (SELECT q.a AS a, r.b AS b FROM x AS q JOIN y AS r ON q.b = r.b) AS x JOIN y AS q ON x.b = q.b JOIN y AS r ON x.b = r.b ORDER BY x.a, q.c, r.c",
		"SELECT q_2.a AS a, q.c AS c, r.c AS c FROM x AS q_2 JOIN y AS r_2 ON q_2.b = r_2.b JOIN y AS q ON r_2.b = q.b JOIN y AS r ON r_2.b = r.b ORDER BY q_2.a, q.c, r.c",
	},
	{
		"merge", false,
		"SELECT r.b AS b FROM (SELECT x.b AS b FROM x AS x) AS q JOIN (SELECT x.b AS b FROM x AS x) AS r ON q.b = r.b",
		"SELECT x_2.b AS b FROM x AS x JOIN x AS x_2 ON x.b = x_2.b",
	},
	{
		"merge", false,
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN (SELECT y.b AS b, y.c AS c FROM y AS y WHERE y.c > 1) AS y ON x.b = y.b ORDER BY x.a",
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b AND y.c > 1 ORDER BY x.a",
	},
	{
		"merge", false,
		"SELECT x.a AS a, y.c AS c FROM (SELECT x.a AS a FROM x AS x) AS x, (SELECT y.c AS c FROM y AS y) AS y",
		"SELECT x.a AS a, y.c AS c FROM x AS x, y AS y",
	},
	{
		"merge", false,
		"SELECT x.a AS a, x.c AS c FROM (SELECT x.a AS a, z.c AS c FROM x AS x, y AS z) AS x",
		"SELECT x.a AS a, z.c AS c FROM x AS x, y AS z",
	},
	{
		"merge", false,
		"SELECT _1.a AS a, _1.b AS b FROM (SELECT _0.a AS a, _0.b AS b FROM (SELECT x.a AS a, x.b AS b FROM x AS x) AS _0) AS _1 ORDER BY _1.a LIMIT 1",
		"SELECT x.a AS a, x.b AS b FROM x AS x ORDER BY x.a LIMIT 1",
	},
	{
		"merge", false,
		"WITH x AS (SELECT x.a AS a, x.b AS b FROM main.x AS x) SELECT x.a AS a, x.b AS b FROM x AS x",
		"SELECT x.a AS a, x.b AS b FROM main.x AS x",
	},
	{
		"merge", false,
		"WITH y AS (SELECT x.a AS a, x.b AS b FROM x AS x) SELECT z.a AS a, z.b AS b FROM y AS z",
		"SELECT x.a AS a, x.b AS b FROM x AS x",
	},
	{
		"merge", false,
		"WITH x2 AS (SELECT x.a AS a FROM main.x AS x), x3 AS (SELECT x2.a AS a FROM x2 AS x2) SELECT x3.a AS a FROM x3 AS x3",
		"SELECT x.a AS a FROM main.x AS x",
	},
	{
		"merge", false,
		"WITH x AS (SELECT x.a AS a, x.b AS b FROM main.x AS x WHERE x.a > 1) SELECT x.a AS a, SUM(x.b) AS b FROM x AS x GROUP BY x.a",
		"SELECT x.a AS a, SUM(x.b) AS b FROM main.x AS x WHERE x.a > 1 GROUP BY x.a",
	},
	{
		"merge", false,
		"WITH x2 AS (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.a > 1) SELECT x.a AS a, y.c AS c FROM x2 AS x JOIN y AS y ON x.b = y.b",
		"SELECT x.a AS a, y.c AS c FROM x AS x JOIN y AS y ON x.b = y.b WHERE x.a > 1",
	},
	{
		"merge", false,
		"WITH y AS (SELECT q.a AS a, q.b AS b FROM x AS q) SELECT z.a AS a, z.b AS b FROM y AS z",
		"SELECT q.a AS a, q.b AS b FROM x AS q",
	},
	{
		"merge", false,
		"SELECT _0.a AS a, _0.b AS b FROM (WITH x AS (SELECT x.a AS a, x.b AS b FROM main.x AS x) SELECT x.a AS a, x.b AS b FROM x AS x) AS _0",
		"SELECT x.a AS a, x.b AS b FROM main.x AS x",
	},
	{
		"merge", false,
		"SELECT x.a AS a FROM (SELECT x.a AS a FROM (SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b) AS x) AS x",
		"SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b",
	},
	{
		"merge", false,
		"WITH x2 AS (SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b) SELECT x.a AS a FROM (SELECT x.a AS a FROM x2 AS x) AS x",
		"SELECT COALESCE(x.a) AS a FROM x AS x LEFT JOIN y AS y ON x.a = y.b",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x FULL OUTER JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x FULL OUTER JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x LEFT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x LEFT JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b WHERE x.b = 1",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x LEFT JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x LEFT JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x RIGHT JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x RIGHT JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x INNER JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x INNER JOIN y AS y ON x.b = y.b AND y.b = 2 WHERE x.b = 1",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x INNER JOIN (SELECT y.b AS b FROM y AS y) AS y ON x.b = y.b",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x INNER JOIN y AS y ON x.b = y.b",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x WHERE x.b = 1) AS x CROSS JOIN (SELECT y.b AS b FROM y AS y WHERE y.b = 2) AS y",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x JOIN y AS y ON y.b = 2 WHERE x.b = 1",
	},
	{
		"merge", false,
		"SELECT x.b AS b, y.b AS b2 FROM (SELECT x.b AS b FROM x AS x) AS x CROSS JOIN (SELECT y.b AS b FROM y AS y) AS y",
		"SELECT x.b AS b, y.b AS b2 FROM x AS x CROSS JOIN y AS y",
	},
	{
		"merge", false,
		"WITH t1 AS (SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x ORDER BY x.a, x.b, row_num) SELECT t1.a AS a, t1.b AS b, t1.row_num AS row_num FROM t1 AS t1",
		"SELECT x.a AS a, x.b AS b, ROW_NUMBER() OVER (PARTITION BY x.a ORDER BY x.a) AS row_num FROM x AS x ORDER BY x.a, x.b, row_num",
	},
	{
		"merge", false,
		"WITH t AS (SELECT t1.x AS x, t1.y AS y, t2.a AS a, t2.b AS b FROM t1 AS t1(x, y) CROSS JOIN t2 AS t2(a, b) ORDER BY t2.a) SELECT t.x AS x, t.y AS y, t.a AS a, t.b AS b FROM t AS t",
		"SELECT t1.x AS x, t1.y AS y, t2.a AS a, t2.b AS b FROM t1 AS t1(x, y) CROSS JOIN t2 AS t2(a, b) ORDER BY t2.a",
	},
	{
		"merge", false,
		"WITH i AS (SELECT x.a AS a FROM x AS x), j AS (SELECT x.a AS a, x.b AS b FROM x AS x), k AS (SELECT j.a AS a, j.b AS b FROM j AS j) SELECT i.a AS a, k.b AS b FROM i AS i LEFT JOIN k AS k ON i.a = k.a",
		"SELECT x.a AS a, x_2.b AS b FROM x AS x LEFT JOIN x AS x_2 ON x.a = x_2.a",
	},
	{
		"merge", false,
		"WITH _q_0 AS (SELECT x.a AS a FROM x AS x), y_2 AS (SELECT y.b AS b FROM y AS y) SELECT y.b AS b FROM (_q_0 AS _q_0 JOIN y_2 AS y ON _q_0.a = y.b)",
		"SELECT y.b AS b FROM (x AS x JOIN y AS y ON x.a = y.b)",
	},
	{
		"merge", false,
		"WITH q AS (SELECT y.b AS a FROM y AS y) SELECT q.a AS a FROM x AS q WHERE q.a IN (SELECT q.a AS a FROM q AS q)",
		"SELECT q.a AS a FROM x AS q WHERE q.a IN (SELECT y.b AS a FROM y AS y)",
	},
	{
		"merge", false,
		"WITH tbl AS (SELECT 1 AS id) SELECT ITBL.id AS id FROM (SELECT OTBL.id AS id FROM (SELECT OTBL.id AS id FROM (SELECT OTBL.id AS id FROM tbl AS OTBL LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id) AS OTBL LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id) AS OTBL LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id) AS ITBL",
		"WITH tbl AS (SELECT 1 AS id) SELECT OTBL.id AS id FROM tbl AS OTBL LEFT OUTER JOIN tbl AS ITBL_2 ON OTBL.id = ITBL_2.id LEFT OUTER JOIN tbl AS ITBL_3 ON OTBL.id = ITBL_3.id LEFT OUTER JOIN tbl AS ITBL ON OTBL.id = ITBL.id",
	},
	{
		"merge", false,
		"WITH i AS (SELECT conflict.a AS a FROM (SELECT 1 AS a) AS conflict), j AS (SELECT 1 AS a) SELECT i.a AS a, conflict.a AS a FROM i AS i LEFT JOIN j AS conflict ON i.a = conflict.a",
		"WITH j AS (SELECT 1 AS a) SELECT conflict_2.a AS a, conflict.a AS a FROM (SELECT 1 AS a) AS conflict_2 LEFT JOIN j AS conflict ON conflict_2.a = conflict.a",
	},
	{
		"merge", false,
		"WITH cte AS (SELECT x.a * x.b AS mult FROM x AS x) SELECT cte.mult AS mult FROM cte AS cte",
		"SELECT x.a * x.b AS mult FROM x AS x",
	},
	{
		"merge", false,
		"WITH t1 AS (SELECT 1 AS col) SELECT t.a AS a, SUM(t.b) AS b FROM (SELECT 6 AS a, t1.col AS b FROM t1 AS t1) AS t GROUP BY t.a ORDER BY a",
		"WITH t1 AS (SELECT 1 AS col) SELECT 6 AS a, SUM(t1.col) AS b FROM t1 AS t1 GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"SELECT s.a AS a, SUM(s.b) AS b FROM (SELECT 6 AS a, x.b AS b FROM x AS x) AS s GROUP BY s.a ORDER BY a",
		"SELECT 6 AS a, SUM(x.b) AS b FROM x AS x GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"WITH c AS (SELECT DISTINCT x.a AS a, x.b AS b FROM x AS x) SELECT s.a AS a, SUM(s.b) AS b FROM (SELECT 6 AS a, c.b AS b FROM c AS c) AS s GROUP BY s.a ORDER BY a",
		"WITH c AS (SELECT DISTINCT x.a AS a, x.b AS b FROM x AS x) SELECT 6 AS a, SUM(c.b) AS b FROM c AS c GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"WITH c AS (SELECT DISTINCT x.b AS b FROM x AS x) SELECT s.a AS a, SUM(s.b) AS b FROM (SELECT 6 AS a, c.b AS b FROM c AS c) AS s CROSS JOIN x AS x GROUP BY s.a ORDER BY a",
		"WITH c AS (SELECT DISTINCT x.b AS b FROM x AS x) SELECT 6 AS a, SUM(c.b) AS b FROM c AS c CROSS JOIN x AS x GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"SELECT s.a AS a, SUM(s.b) AS b FROM (SELECT 6 AS a, y.b AS b FROM y AS y) AS s CROSS JOIN unknown_tbl AS unknown_tbl GROUP BY s.a ORDER BY a",
		"SELECT 6 AS a, SUM(y.b) AS b FROM y AS y CROSS JOIN unknown_tbl AS unknown_tbl GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"SELECT s.a AS a, SUM(s.b) AS b FROM (SELECT 6 AS a, x.b AS b FROM x AS x) AS s GROUP BY (s.a) ORDER BY a",
		"SELECT 6 AS a, SUM(x.b) AS b FROM x AS x GROUP BY 1 ORDER BY a",
	},
	{
		"merge", false,
		"WITH \"cte1\" AS (SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\"), \"cte2\" AS (SELECT \"cte1\".\"a\" + 1 AS \"a\" FROM \"cte1\" AS \"cte1\") SELECT \"cte1\".\"a\" AS \"a\" FROM \"cte1\" AS \"cte1\" UNION ALL SELECT \"cte2\".\"a\" AS \"a\" FROM \"cte2\" AS \"cte2\"",
		"WITH \"cte1\" AS (SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\") SELECT \"cte1\".\"a\" AS \"a\" FROM \"cte1\" AS \"cte1\" UNION ALL SELECT \"cte1\".\"a\" + 1 AS \"a\" FROM \"cte1\" AS \"cte1\"",
	},
	{
		"unnest", false,
		"SELECT \"d\".\"a\" AS \"a\", SUM(\"d\".\"b\") AS \"sum_b\" FROM (SELECT \"x\".\"a\" AS \"a\", \"y\".\"b\" AS \"b\" FROM \"x\" AS \"x\", \"y\" AS \"y\" WHERE (SELECT MAX(\"y\".\"b\") AS \"_col_0\" FROM \"y\" AS \"y\" WHERE \"x\".\"b\" = \"y\".\"b\") >= 0 AND \"x\".\"b\" = \"y\".\"b\") AS \"d\" WHERE (('a' = 'b' OR TRUE) AND ('a' = 'b' OR TRUE)) AND \"d\".\"a\" > 1 GROUP BY \"d\".\"a\"",
		"SELECT \"d\".\"a\" AS \"a\", SUM(\"d\".\"b\") AS \"sum_b\" FROM (SELECT \"x\".\"a\" AS \"a\", \"y\".\"b\" AS \"b\" FROM \"x\" AS \"x\", \"y\" AS \"y\" LEFT JOIN (SELECT MAX(\"y\".\"b\") AS \"_col_0\", \"y\".\"b\" AS _u_1 FROM \"y\" AS \"y\" WHERE TRUE GROUP BY \"y\".\"b\") AS _u_0 ON \"x\".\"b\" = _u_0._u_1 WHERE _u_0._col_0 >= 0 AND \"x\".\"b\" = \"y\".\"b\") AS \"d\" WHERE (('a' = 'b' OR TRUE) AND ('a' = 'b' OR TRUE)) AND \"d\".\"a\" > 1 GROUP BY \"d\".\"a\"",
	},
	{
		"merge", false,
		"WITH _u_0 AS (SELECT MAX(\"y\".\"b\") AS \"_col_0\", \"y\".\"b\" AS _u_1 FROM \"y\" AS \"y\" WHERE TRUE GROUP BY \"y\".\"b\"), d AS (SELECT \"x\".\"a\" AS \"a\", \"y\".\"b\" AS \"b\" FROM \"x\" AS \"x\" JOIN \"y\" AS \"y\" ON \"x\".\"b\" = \"y\".\"b\" LEFT JOIN _u_0 AS _u_0 ON \"x\".\"b\" = _u_0._u_1 WHERE \"x\".\"a\" > 1 AND TRUE AND _u_0._col_0 >= 0) SELECT \"d\".\"a\" AS \"a\", SUM(\"d\".\"b\") AS \"sum_b\" FROM d AS d WHERE TRUE GROUP BY \"d\".\"a\"",
		"WITH _u_0 AS (SELECT MAX(\"y\".\"b\") AS \"_col_0\", \"y\".\"b\" AS _u_1 FROM \"y\" AS \"y\" WHERE TRUE GROUP BY \"y\".\"b\") SELECT \"x\".\"a\" AS \"a\", SUM(\"y\".\"b\") AS \"sum_b\" FROM \"x\" AS \"x\" JOIN \"y\" AS \"y\" ON \"x\".\"b\" = \"y\".\"b\" LEFT JOIN _u_0 AS _u_0 ON \"x\".\"b\" = _u_0._u_1 WHERE TRUE AND (\"x\".\"a\" > 1 AND TRUE AND _u_0._col_0 >= 0) GROUP BY \"x\".\"a\"",
	},
	{
		"merge", false,
		"WITH cte2 AS (SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\"), cte3 AS (SELECT 1 AS \"1\"), _0 AS (SELECT \"cte2\".\"a\" AS \"a1\" FROM cte2 AS cte2), \"cte1\" AS (SELECT \"_0\".\"a1\" AS \"a1\" FROM _0 AS _0) SELECT \"cte1\".\"a1\" AS \"a1\" FROM \"cte1\" AS \"cte1\"",
		"WITH cte3 AS (SELECT 1 AS \"1\") SELECT \"x\".\"a\" AS \"a1\" FROM \"x\" AS \"x\"",
	},
	{
		"merge", false,
		"WITH \"cte\" AS (SELECT \"x\".\"a\" * \"x\".\"b\" AS \"c\", \"x\".\"a\" AS \"d\", \"x\".\"b\" AS \"e\" FROM \"x\" AS \"x\") SELECT \"cte\".\"c\" + \"cte\".\"d\" - (\"cte\".\"c\" - \"cte\".\"e\") AS \"f\" FROM \"cte\" AS \"cte\"",
		"SELECT (\"x\".\"a\" * \"x\".\"b\") + \"x\".\"a\" - (((\"x\".\"a\" * \"x\".\"b\")) - \"x\".\"b\") AS \"f\" FROM \"x\" AS \"x\"",
	},
	{
		"merge", false,
		"WITH t AS (SELECT \"x\".\"a\" AS \"a\", \"y\".\"c\" AS \"c\" FROM \"x\" AS \"x\" LEFT JOIN \"y\" AS \"y\" ON \"x\".\"a\" = \"y\".\"c\") SELECT \"t\".\"a\" AS \"a\", \"t\".\"c\" AS \"c\" FROM t AS t",
		"SELECT \"x\".\"a\" AS \"a\", \"y\".\"c\" AS \"c\" FROM \"x\" AS \"x\" LEFT JOIN \"y\" AS \"y\" ON \"x\".\"a\" = \"y\".\"c\"",
	},
	{
		"merge", false,
		"WITH _0 AS (SELECT \"x\".\"a\" AS \"a\", \"x\".\"b\" AS \"b\" FROM \"x\" AS \"x\") SELECT \"y\".\"b\" AS \"b\", \"y\".\"c\" AS \"c\", \"_0\".\"a\" AS \"a\", \"_0\".\"b\" AS \"b\" FROM (_0 AS _0 JOIN \"y\" AS \"y\" ON \"_0\".\"a\" = \"y\".\"c\")",
		"SELECT \"y\".\"b\" AS \"b\", \"y\".\"c\" AS \"c\", \"x\".\"a\" AS \"a\", \"x\".\"b\" AS \"b\" FROM (\"x\" AS \"x\" JOIN \"y\" AS \"y\" ON \"x\".\"a\" = \"y\".\"c\")",
	},
	{
		"merge", false,
		"WITH _0 AS (SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\") SELECT \"y\".\"b\" AS \"b\" FROM (_0 AS _0 JOIN \"y\" AS \"y\" ON \"_0\".\"a\" = \"y\".\"b\")",
		"SELECT \"y\".\"b\" AS \"b\" FROM (\"x\" AS \"x\" JOIN \"y\" AS \"y\" ON \"x\".\"a\" = \"y\".\"b\")",
	},
	{
		"merge", false,
		"WITH _0 AS (SELECT \"x\".\"a\" AS \"a\", \"x\".\"b\" AS \"b\" FROM \"x\" AS \"x\"), _1 AS (SELECT \"y\".\"b\" AS \"b\", \"y\".\"c\" AS \"c\" FROM \"y\" AS \"y\") SELECT \"_0\".\"a\" AS \"a\", \"_0\".\"b\" AS \"b\", \"_1\".\"b\" AS \"b\", \"_1\".\"c\" AS \"c\" FROM (_0 AS _0 JOIN _1 AS _1 ON \"_0\".\"a\" = \"_1\".\"c\")",
		"SELECT \"x\".\"a\" AS \"a\", \"x\".\"b\" AS \"b\", \"y\".\"b\" AS \"b\", \"y\".\"c\" AS \"c\" FROM (\"x\" AS \"x\" JOIN \"y\" AS \"y\" ON \"x\".\"a\" = \"y\".\"c\")",
	},
	{
		"unnest", false,
		"SELECT \"x\".\"a\" AS \"a\", SUM(\"y\".\"c\") / (SELECT SUM(\"y\".\"c\") AS \"_col_0\" FROM \"y\" AS \"y\") * 100 AS \"foo\" FROM \"y\" AS \"y\" INNER JOIN \"x\" AS \"x\" ON \"y\".\"b\" = \"x\".\"b\" GROUP BY \"x\".\"a\"",
		"SELECT \"x\".\"a\" AS \"a\", SUM(\"y\".\"c\") / MAX(_u_0._col_0) * 100 AS \"foo\" FROM \"y\" AS \"y\" INNER JOIN \"x\" AS \"x\" ON \"y\".\"b\" = \"x\".\"b\" CROSS JOIN (SELECT SUM(\"y\".\"c\") AS \"_col_0\" FROM \"y\" AS \"y\") AS _u_0 GROUP BY \"x\".\"a\"",
	},
	{
		"merge", false,
		"WITH t1 AS (SELECT MAX(\"x\".\"a\") AS \"c1\" FROM \"x\" AS \"x\"), \"t3\" AS (SELECT CAST(\"t1\".\"c1\" AS BIGINT) AS \"ref1\" FROM t1 AS t1 JOIN GENERATE_SERIES((SELECT MAX(\"x\".\"a\") AS \"_col_0\" FROM \"x\" AS \"x\"), 10, 1) AS \"t2\"(\"c1\") ON \"t1\".\"c1\" < \"t2\".\"c1\") SELECT \"t3\".\"ref1\" AS \"ref1\" FROM \"t3\" AS \"t3\"",
		"WITH t1 AS (SELECT MAX(\"x\".\"a\") AS \"c1\" FROM \"x\" AS \"x\") SELECT CAST(\"t1\".\"c1\" AS BIGINT) AS \"ref1\" FROM t1 AS t1 JOIN GENERATE_SERIES((SELECT MAX(\"x\".\"a\") AS \"_col_0\" FROM \"x\" AS \"x\"), 10, 1) AS \"t2\"(\"c1\") ON \"t1\".\"c1\" < \"t2\".\"c1\"",
	},
	{
		"unnest", false,
		"SELECT * FROM x AS x WHERE (SELECT y.a AS a FROM y AS y WHERE x.a = y.a) = 1",
		"SELECT * FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE _u_0.a = 1",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT y.a AS a FROM y AS y UNION SELECT z.a AS a FROM z AS z)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT _u_0.a AS a FROM (SELECT y.a AS a FROM y AS y UNION SELECT z.a AS a FROM z AS z) AS _u_0 GROUP BY _u_0.a) AS _u_1 ON x.a = _u_1.a WHERE NOT _u_1.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x GROUP BY x.a HAVING x.a = (SELECT MAX(y.a) AS a FROM y AS y)",
		"SELECT x.a AS a FROM x AS x CROSS JOIN (SELECT MAX(y.a) AS a FROM y AS y) AS _u_0 GROUP BY x.a HAVING x.a = MAX(_u_0.a)",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x JOIN z AS z ON z.a IN (SELECT y.a AS a FROM y AS y)",
		"SELECT x.a AS a FROM x AS x JOIN z AS z ON TRUE LEFT JOIN (SELECT y.a AS a FROM y AS y GROUP BY y.a) AS _u_0 ON z.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT y.a AS a FROM y AS y GROUP BY y.a)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y GROUP BY y.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT y.a AS a FROM y AS y GROUP BY y.a, y.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT _q.a AS a FROM (SELECT y.a AS a FROM y AS y GROUP BY y.a, y.b) AS _q GROUP BY _q.a) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT MAX(y.a) AS a FROM y AS y)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT MAX(y.a) AS a FROM y AS y) AS _u_0 ON x.a = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a + 1 IN (SELECT y.a AS a FROM y AS y)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y GROUP BY y.a) AS _u_0 ON (x.a + 1) = _u_0.a WHERE NOT _u_0.a IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a = ANY (SELECT y.a AS a FROM y AS y WHERE y.b = x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT ARRAY_AGG(y.a) AS a, y.b AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.b) AS _u_0 ON _u_0._u_1 = x.b WHERE x.a = ARRAY_ANY(_u_0.a, _x -> x.a = _x)",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a > ANY (SELECT y.a AS a FROM y AS y WHERE y.b = x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT ARRAY_AGG(y.a) AS a, y.b AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.b) AS _u_0 ON _u_0._u_1 = x.b WHERE x.a > ARRAY_ANY(_u_0.a, _x -> x.a > _x)",
	},
	{
		"unnest", false,
		"SELECT x.a AS a, (SELECT y.b AS b FROM y AS y WHERE y.a = x.a) AS c FROM x AS x",
		"SELECT x.a AS a, _u_0.b AS c FROM x AS x LEFT JOIN (SELECT MAX(y.b) AS b, y.a AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a",
	},
	{
		"unnest", false,
		"SELECT x.a AS a, (SELECT COUNT(y.b) AS b FROM y AS y WHERE y.a = x.a AND y.c > x.c) AS c FROM x AS x",
		"SELECT x.a AS a, COALESCE(_u_0.b, 0) AS c FROM x AS x LEFT JOIN (SELECT COUNT(y.b) AS b, y.a AS _u_1, MAX(y.c) AS _u_2 FROM y AS y WHERE TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE _u_0._u_2 > x.c",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE EXISTS(SELECT 1 AS one FROM y AS y WHERE y.a = x.a AND y.b = x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT y.a AS _u_1, y.b AS _u_2 FROM y AS y WHERE TRUE AND TRUE GROUP BY y.a, y.b) AS _u_0 ON _u_0._u_1 = x.a AND _u_0._u_2 = x.b WHERE NOT _u_0._u_1 IS NULL",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE NOT EXISTS(SELECT 1 AS one FROM y AS y WHERE y.a = x.a AND y.b > x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT y.a AS _u_1, ARRAY_AGG(y.b) AS _u_2 FROM y AS y WHERE TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE NOT (NOT _u_0._u_1 IS NULL AND ARRAY_ANY(_u_0._u_2, _x -> _x > x.b))",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT y.a AS a FROM y AS y WHERE y.a = x.a)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT y.a AS a FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0.a = x.a WHERE x.a = _u_0.a",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a IN (SELECT y.c AS c FROM y AS y WHERE y.a = x.a AND y.b < x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT ARRAY_AGG(y.c) AS c, y.a AS _u_1, ARRAY_AGG(y.b) AS _u_2 FROM y AS y WHERE TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE (ARRAY_ANY(_u_0.c, _x -> _x = x.a) AND ARRAY_ANY(_u_0._u_2, _x -> _x < x.b))",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a = (SELECT SUM(y.c) AS c FROM y AS y WHERE y.a = x.a AND y.a = x.b)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT SUM(y.c) AS c, y.a AS _u_1 FROM y AS y WHERE TRUE AND TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a AND _u_0._u_1 = x.b WHERE x.a = _u_0.c",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a < ALL (SELECT y.c AS c FROM y AS y WHERE y.a = x.a)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT ARRAY_AGG(y.c) AS c, y.a AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a WHERE ARRAY_ALL(_u_0.c, _x -> x.a < _x)",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a NOT IN (SELECT y.a AS a FROM y AS y)",
		"SELECT x.a AS a FROM x AS x WHERE NOT x.a IN (SELECT y.a AS a FROM y AS y)",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a = (SELECT SUM(y.c) AS c FROM y AS y WHERE y.a = x.a + 1)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT SUM(y.c) AS c, y.a AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.a) AS _u_0 ON _u_0._u_1 = x.a + 1 WHERE x.a = _u_0.c",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE x.a = (SELECT SUM(y.c) AS c FROM y AS y WHERE y.a + 1 = x.a)",
		"SELECT x.a AS a FROM x AS x LEFT JOIN (SELECT SUM(y.c) AS c, y.a + 1 AS _u_1 FROM y AS y WHERE TRUE GROUP BY y.a + 1) AS _u_0 ON _u_0._u_1 = x.a WHERE x.a = _u_0.c",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q CROSS JOIN y AS y",
		"SELECT x.a AS a FROM x AS x CROSS JOIN y AS y",
	},
	{
		"merge", false,
		"WITH q AS (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.b > 1) SELECT q.a AS a FROM q AS q",
		"SELECT x.a AS a FROM x AS x WHERE x.b > 1",
	},
	{
		"merge", true,
		"WITH q AS (SELECT x.a AS a, x.b AS b FROM x AS x WHERE x.b > 1) SELECT q.a AS a FROM q AS q",
		"SELECT x.a AS a FROM x AS x WHERE x.b > 1",
	},
	{
		"merge", false,
		"SELECT q.a + 1 AS a FROM (SELECT x.a + x.b AS a FROM x AS x) AS q",
		"SELECT (x.a + x.b) + 1 AS a FROM x AS x",
	},
	{
		"merge", true,
		"SELECT q.a + 1 AS a FROM (SELECT x.a + x.b AS a FROM x AS x) AS q",
		"SELECT (x.a + x.b) + 1 AS a FROM x AS x",
	},
	{
		"merge", false,
		"SELECT q.a AS b FROM (SELECT x.a + x.b AS a FROM x AS x) AS q",
		"SELECT x.a + x.b AS b FROM x AS x",
	},
	{
		"merge", true,
		"SELECT q.a AS b FROM (SELECT x.a + x.b AS a FROM x AS x) AS q",
		"SELECT x.a + x.b AS b FROM x AS x",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT 1 AS a, x.b AS b FROM x AS x) AS q GROUP BY q.a",
		"SELECT 1 AS a FROM x AS x GROUP BY 1",
	},
	{
		"merge", true,
		"SELECT q.a AS a FROM (SELECT 1 AS a, x.b AS b FROM x AS x) AS q GROUP BY q.a",
		"SELECT 1 AS a FROM x AS x GROUP BY 1",
	},
	{
		"merge", false,
		"SELECT q.a AS a, q.b AS b FROM (SELECT 1 AS a, x.b AS b FROM x AS x) AS q GROUP BY q.a, q.b",
		"SELECT 1 AS a, x.b AS b FROM x AS x GROUP BY 1, x.b",
	},
	{
		"merge", true,
		"SELECT q.a AS a, q.b AS b FROM (SELECT 1 AS a, x.b AS b FROM x AS x) AS q GROUP BY q.a, q.b",
		"SELECT 1 AS a, x.b AS b FROM x AS x GROUP BY 1, x.b",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x ORDER BY x.a) AS q",
		"SELECT x.a AS a FROM x AS x ORDER BY x.a",
	},
	{
		"merge", true,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x ORDER BY x.a) AS q",
		"SELECT x.a AS a FROM x AS x ORDER BY x.a",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a) AS q JOIN z AS z ON q.a = z.a",
		"SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a JOIN z AS z ON x.a = z.a",
	},
	{
		"merge", false,
		"SELECT x.a AS a FROM x AS x JOIN (SELECT y.a AS a FROM y AS y WHERE y.b > 1) AS q ON x.a = q.a",
		"SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a AND y.b > 1",
	},
	{
		"merge", false,
		"SELECT x.a AS a FROM x AS x JOIN (SELECT y.a AS a FROM y AS y WHERE y.b > x.a) AS q ON x.a = q.a",
		"SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a AND y.b > x.a",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q JOIN (SELECT x.b AS b FROM x AS x) AS r ON q.a = r.b",
		"SELECT x.a AS a FROM x AS x JOIN x AS x_2 ON x.a = x_2.b",
	},
	{
		"merge", false,
		"SELECT q.rn AS rn FROM (SELECT x.a AS a, ROW_NUMBER() OVER (ORDER BY x.a) AS rn FROM x AS x) AS q",
		"SELECT ROW_NUMBER() OVER (ORDER BY x.a) AS rn FROM x AS x",
	},
	{
		"merge", true,
		"SELECT q.rn AS rn FROM (SELECT x.a AS a, ROW_NUMBER() OVER (ORDER BY x.a) AS rn FROM x AS x) AS q",
		"SELECT ROW_NUMBER() OVER (ORDER BY x.a) AS rn FROM x AS x",
	},
	{
		"merge", false,
		"SELECT /*+ BROADCAST(y) */ q.a AS a FROM (SELECT /*+ MERGE(x) */ x.a AS a FROM x AS x) AS q JOIN y AS y ON q.a = y.a",
		"SELECT /*+ BROADCAST(y), MERGE(x) */ x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a",
	},
	{
		"merge", false,
		"WITH q AS (SELECT x.a AS a FROM x AS x), r AS (SELECT q.a AS a FROM q AS q) SELECT r.a AS a FROM r AS r",
		"SELECT x.a AS a FROM x AS x",
	},
	{
		"merge", true,
		"WITH q AS (SELECT x.a AS a FROM x AS x), r AS (SELECT q.a AS a FROM q AS q) SELECT r.a AS a FROM r AS r",
		"SELECT x.a AS a FROM x AS x",
	},
	{
		"merge", false,
		"SELECT x.a AS a FROM (SELECT x.a AS a FROM x AS x) AS x",
		"SELECT x.a AS a FROM x AS x",
	},
	{
		"merge", true,
		"SELECT x.a AS a FROM (SELECT x.a AS a FROM x AS x) AS x",
		"SELECT x.a AS a FROM x AS x",
	},
	{
		"merge", false,
		"SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a) AS y",
		"SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a",
	},
	{
		"merge", true,
		"SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a) AS y",
		"SELECT x.a AS a FROM x AS x JOIN y AS y ON x.a = y.a",
	},
	{
		"merge", false,
		"SELECT q.a AS a, y.b AS b FROM (SELECT y.a AS a FROM y AS y) AS q JOIN y AS y ON q.a = y.b",
		"SELECT y_2.a AS a, y.b AS b FROM y AS y_2 JOIN y AS y ON y_2.a = y.b",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q UNION ALL SELECT r.a AS a FROM (SELECT y.a AS a FROM y AS y ORDER BY y.a) AS r",
		"SELECT x.a AS a FROM x AS x UNION ALL SELECT r.a AS a FROM (SELECT y.a AS a FROM y AS y ORDER BY y.a) AS r",
	},
	{
		"merge", true,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q UNION ALL SELECT r.a AS a FROM (SELECT y.a AS a FROM y AS y ORDER BY y.a) AS r",
		"SELECT x.a AS a FROM x AS x UNION ALL SELECT r.a AS a FROM (SELECT y.a AS a FROM y AS y ORDER BY y.a) AS r",
	},
	{
		"unnest", false,
		"SELECT * FROM (\"tbl\" AS \"tbl\")",
		"SELECT * FROM (\"tbl\" AS \"tbl\")",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a = SUM(SELECT 1)",
		"SELECT * FROM x WHERE x.a = SUM(SELECT 1)",
	},
	{
		"unnest", false,
		"(SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\") LIMIT 1",
		"(SELECT \"x\".\"a\" AS \"a\" FROM \"x\" AS \"x\") LIMIT 1",
	},
	{
		"unnest", false,
		"SELECT * FROM x.a WHERE x.a > ANY (SELECT y.a FROM y)",
		"SELECT * FROM x.a WHERE x.a > ANY (SELECT y.a FROM y)",
	},
	{
		"unnest", false,
		"SELECT * FROM ((SELECT * FROM \"tbl\" AS \"tbl\") AS \"_0\")",
		"SELECT * FROM ((SELECT * FROM \"tbl\" AS \"tbl\") AS \"_0\")",
	},
	{
		"unnest", false,
		"SELECT \"foO\".\"x\" AS \"x\" FROM (SELECT 1 AS \"x\") AS \"foO\"",
		"SELECT \"foO\".\"x\" AS \"x\" FROM (SELECT 1 AS \"x\") AS \"foO\"",
	},
	{
		"unnest", false,
		"SELECT * FROM \"db1\".\"tbl\" AS \"tbl\", \"db2\".\"tbl\" AS \"tbl_2\"",
		"SELECT * FROM \"db1\".\"tbl\" AS \"tbl\", \"db2\".\"tbl\" AS \"tbl_2\"",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT SUM(x.a) AS a FROM x AS x) AS q",
		"SELECT q.a AS a FROM (SELECT SUM(x.a) AS a FROM x AS x) AS q",
	},
	{
		"unnest", false,
		"SELECT BIT_COUNT(EXISTS(SELECT 1 WHERE FALSE)) AS col FROM t0",
		"SELECT BIT_COUNT(EXISTS(SELECT 1 WHERE FALSE)) AS col FROM t0",
	},
	{
		"unnest", false,
		"SELECT \"x\".\"a\" + 1 AS \"b\", \"x\".\"b\" + 1 AS \"c\" FROM \"x\" AS \"x\"",
		"SELECT \"x\".\"a\" + 1 AS \"b\", \"x\".\"b\" + 1 AS \"c\" FROM \"x\" AS \"x\"",
	},
	{
		"unnest", false,
		"SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y LIMIT 10)",
		"SELECT * FROM x WHERE x.a IN (SELECT y.a AS a FROM y LIMIT 10)",
	},
	{
		"unnest", false,
		"SELECT \"q\".\"x\" AS \"x\" FROM UNNEST(ARRAY(1, 2)) AS \"q\"(\"x\", \"y\")",
		"SELECT \"q\".\"x\" AS \"x\" FROM UNNEST(ARRAY(1, 2)) AS \"q\"(\"x\", \"y\")",
	},
	{
		"merge", false,
		"SELECT q.a AS a FROM (SELECT DISTINCT x.a AS a FROM x AS x) AS q",
		"SELECT q.a AS a FROM (SELECT DISTINCT x.a AS a FROM x AS x) AS q",
	},
	{
		"unnest", false,
		"SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a FROM y OFFSET 10)",
		"SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a FROM y OFFSET 10)",
	},
	{
		"unnest", false,
		"SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a, y.b AS b FROM y)",
		"SELECT * FROM x.a WHERE x.a IN (SELECT y.a AS a, y.b AS b FROM y)",
	},
	{
		"unnest", false,
		"SELECT \"x\".\"a\" + 1 AS \"c\", \"x\".\"a\" + 1 + 1 AS \"d\" FROM \"x\" AS \"x\"",
		"SELECT \"x\".\"a\" + 1 AS \"c\", \"x\".\"a\" + 1 + 1 AS \"d\" FROM \"x\" AS \"x\"",
	},
	{
		"merge", false,
		"WITH _0 AS (SELECT * FROM \"tbl\" AS \"tbl\") SELECT * FROM (_0 AS _0)",
		"WITH _0 AS (SELECT * FROM \"tbl\" AS \"tbl\") SELECT * FROM (_0 AS _0)",
	},
	{
		"merge", false,
		"SELECT * FROM \"db1\".\"tbl\" AS \"tbl\" CROSS JOIN \"db2\".\"tbl\" AS \"tbl_2\"",
		"SELECT * FROM \"db1\".\"tbl\" AS \"tbl\" CROSS JOIN \"db2\".\"tbl\" AS \"tbl_2\"",
	},
	{
		"unnest", false,
		"SELECT x.a AS a, (SELECT MAX(y.b) AS b FROM y AS y) AS m FROM x AS x",
		"SELECT x.a AS a, (SELECT MAX(y.b) AS b FROM y AS y) AS m FROM x AS x",
	},
	{
		"merge", false,
		"WITH foO AS (SELECT 1 AS \"x\") SELECT \"foO\".\"x\" AS \"x\" FROM foO AS foO",
		"WITH foO AS (SELECT 1 AS \"x\") SELECT \"foO\".\"x\" AS \"x\" FROM foO AS foO",
	},
	{
		"unnest", false,
		"SELECT SUM(x.a) AS a, (SELECT MAX(y.b) AS b FROM y AS y) AS m FROM x AS x",
		"SELECT SUM(x.a) AS a, (SELECT MAX(y.b) AS b FROM y AS y) AS m FROM x AS x",
	},
	{
		"merge", true,
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q CROSS JOIN y AS y",
		"SELECT q.a AS a FROM (SELECT x.a AS a FROM x AS x) AS q CROSS JOIN y AS y",
	},
	{
		"unnest", false,
		"SELECT x.a AS a FROM x AS x WHERE EXISTS(SELECT MAX(y.b) AS b FROM y AS y)",
		"SELECT x.a AS a FROM x AS x WHERE EXISTS(SELECT MAX(y.b) AS b FROM y AS y)",
	},
	{
		"unnest", false,
		"SELECT * FROM GENERATE_SERIES((SELECT MAX(y.a) AS a FROM y AS y), 10) AS g",
		"SELECT * FROM GENERATE_SERIES((SELECT MAX(y.a) AS a FROM y AS y), 10) AS g",
	},
	{
		"merge", false,
		"SELECT s.b AS b FROM (SELECT 6 AS a, x.b AS b FROM x AS x) AS s ORDER BY s.a",
		"SELECT s.b AS b FROM (SELECT 6 AS a, x.b AS b FROM x AS x) AS s ORDER BY s.a",
	},
}

func TestOptSubqueriesParity(t *testing.T) {
	t.Parallel()
	d := MustDialect("")
	for i, c := range optSubqueriesCases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("case %d (%s): panic %v\nSQL: %s", i, c.rule, r, c.sql)
				}
			}()
			e, err := d.ParseOne(c.sql, nil)
			if err != nil {
				t.Fatalf("case %d: parse error %v", i, err)
			}
			if c.rule == "unnest" {
				e = UnnestSubqueries(e)
			} else {
				e = MergeSubqueries(e, c.lti)
			}
			got, err := d.Generate(e, nil)
			if err != nil {
				t.Fatalf("case %d: generate error %v", i, err)
			}
			if got != c.want {
				t.Errorf("case %d (%s lti=%v):\nSQL:  %s\nwant: %s\ngot:  %s", i, c.rule, c.lti, c.sql, c.want, got)
			}
		}()
	}
}

// Python raises AttributeError here (`select.parent` is None after wrapping a set operation that
// must be MAX-aggregated); the port must fail too rather than silently produce SQL.
func TestUnnestSubqueriesSetOperationHavingPanics(t *testing.T) {
	t.Parallel()
	d := MustDialect("")
	e, err := d.ParseOne("SELECT x.a AS a FROM x AS x GROUP BY x.a HAVING x.a = (SELECT y.a AS a FROM y AS y UNION SELECT z.a AS a FROM z AS z)", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Errorf("expected a panic")
		}
	}()
	UnnestSubqueries(e)
}
