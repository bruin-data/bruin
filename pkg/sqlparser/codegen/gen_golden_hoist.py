"""Records testdata/golden/hoist.json.gz: multi-statement scripts and hook lists built around DECLARE
statements, through hoist-declares and hoist-declares-list in every dialect Bruin hoists for,
answered by the Python reference implementation.

The conformance corpus holds single statements, so it barely reaches the hoisting logic (splitting
on top-level semicolons, procedural block nesting, reordering). This script composes scripts from
declarations, plain statements, procedural blocks, comments and separators instead, plus malformed
variants of them. Commands that run longer than 5 s are recorded as {"__timeout__": true}.

usage: gen_golden_hoist.py [out.json.gz] [seed]
"""

import multiprocessing as mp
import os
import random
import sys

import golden_common as g
from gen_golden_fuzz import mutants, timed_run

# The dialects pkg/sqlparser sends (the values of assetTypeDialectMap), "vertica" (normalized to
# postgres by both implementations) and an unknown one.
DIALECTS = sorted(g.BRUIN_DIALECTS | {"vertica"})
UNKNOWN_DIALECT = "not_a_dialect"

SCRIPTS_PER_DIALECT = 700
LISTS_PER_DIALECT = 350
CORPUS_PER_DIALECT = 150

DECLARES = [
    "DECLARE x INT64",
    "DECLARE x INT",
    "declare lower_x int",
    "Declare mixed_x Int64",
    "DECLARE a, b STRING",
    "DECLARE s STRING DEFAULT 'a;b'",
    "DECLARE s STRING DEFAULT 'DECLARE nested INT; END'",
    "DECLARE d DATE DEFAULT CURRENT_DATE()",
    "DECLARE arr ARRAY<STRING>",
    "DECLARE distinct_keys array<STRING>",
    "DECLARE arr ARRAY<STRING> DEFAULT ['x;', 'DECLARE y']",
    "DECLARE st STRUCT<a INT64, b STRING>",
    "DECLARE t TIMESTAMP DEFAULT (SELECT MAX(ts) FROM ds.events)",
    "DECLARE n NUMERIC(10, 2) DEFAULT 1.5",
    "DECLARE `quoted` INT64",
    'DECLARE "quoted" INT',
    "DECLARE café STRING",
    "DECLARE @x INT",
    "DECLARE @x INT = 5",
    "DECLARE @a INT = 1, @b VARCHAR(10) = 'x;y'",
    "DECLARE @t TABLE (id INT, name VARCHAR(10))",
    "DECLARE x INT = 5",
    "DECLARE x VARIANT",
    "DECLARE res RESULTSET DEFAULT (SELECT 1)",
    "DECLARE c CURSOR FOR SELECT 1",
    "DECLARE x INT -- trailing comment",
    "DECLARE x INT64 # trailing comment",
    "/* lead; */ DECLARE x INT",
    "-- lead\nDECLARE x INT",
    "DECLARE\n  multi_line\n  INT64",
]

STATEMENTS = [
    "SELECT 1",
    "select 2 as two",
    "SET x = 1",
    "SET (a, b) = (1, 2)",
    "SET @x = 1",
    "SELECT 'declare bankruptcy' AS msg",
    "SELECT ';' AS semi",
    "SELECT 'it''s; fine'",
    'SELECT "a;b" FROM t',
    "SELECT `a;b` FROM t",
    "SELECT 'pre; 雪' AS message",
    "SELECT CASE WHEN x > 0 THEN 'a' ELSE 'b' END AS c FROM t",
    "SELECT CASE x WHEN 1 THEN (SELECT 1) END",
    "SELECT (SELECT MAX(a) FROM (SELECT 1 AS a)) AS m",
    "INSERT INTO t VALUES (1, 'x;y')",
    "CREATE TABLE t AS SELECT 1 AS id",
    "CREATE OR REPLACE TABLE ds.t AS SELECT * FROM ds.src",
    "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
    "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)",
    "UPDATE t SET declare_count = 1",
    "SELECT declare FROM t",
    "SELECT $$a;b$$",
    "SELECT $tag$DECLARE x INT;$tag$",
    "EXECUTE IMMEDIATE 'DECLARE x INT64; SELECT 1'",
    "CALL proc()",
    "SELECT 1 /* DECLARE x INT; */",
    "SELECT 1 -- DECLARE x INT;",
    "COMMIT",
    "COMMIT TRANSACTION",
    "ROLLBACK",
    "BEGIN TRANSACTION",
    "BEGIN",
    "BEGIN WORK",
    "BEGIN TRAN",
    "START TRANSACTION",
    "TRUNCATE TABLE t",
    "DROP TABLE IF EXISTS t",
    "WITH c AS (SELECT 1 AS a) SELECT * FROM c",
    "SELECT [1, 2]",
    "USE db",
    "SHOW TABLES",
    "PRINT 'x'",
    "RETURN",
    "x := 1",
    "CREATE TEMP FUNCTION f(x INT64) AS (x + 1)",
    "RAISE USING MESSAGE = 'boom; END'",
]

# Procedural constructs; {} is filled with one or more statements joined by semicolons.
BLOCKS = [
    "BEGIN\n  {};\nEND",
    "BEGIN {}; END",
    "BEGIN {} END",
    "BEGIN\n  BEGIN {}; END;\n  {};\nEND",
    "BEGIN\n  SELECT CASE WHEN 1 = 1 THEN 'a' END;\n  {};\nEND",
    "BEGIN {}; EXCEPTION WHEN ERROR THEN SELECT @@error.message; END",
    "IF x > 0 THEN {}; END IF",
    "IF x > 0 THEN {}; ELSEIF x < 0 THEN {}; ELSE {}; END IF",
    "BEGIN IF TRUE THEN {}; END IF; {}; END",
    "LOOP {}; END LOOP",
    "BEGIN LOOP {}; LEAVE; END LOOP; {}; END",
    "WHILE x < 3 DO {}; END WHILE",
    "BEGIN WHILE x < 3 DO {}; END WHILE; {}; END",
    "REPEAT {}; UNTIL x > 3 END REPEAT",
    "FOR r IN (SELECT 1 AS a) DO {}; END FOR",
    "CASE x WHEN 1 THEN {}; ELSE {}; END CASE",
    "BEGIN TRANSACTION; {}; COMMIT TRANSACTION",
    "CREATE PROCEDURE p() BEGIN {}; END",
    "CREATE OR REPLACE PROCEDURE p() RETURNS STRING LANGUAGE SQL AS $$ BEGIN {}; END $$",
    "EXECUTE IMMEDIATE $$ DECLARE y INT; BEGIN {}; END $$",
    "DO $$ BEGIN {}; END $$",
    "CREATE FUNCTION f() RETURNS INT AS $body$ BEGIN {}; END $body$ LANGUAGE plpgsql",
    "BEGIN TRY {}; END TRY BEGIN CATCH SELECT 1; END CATCH",
    "IF @x > 0 BEGIN {}; END",
    "WHILE @x < 3 BEGIN {}; END",
    "DECLARE y INT; BEGIN {}; END",
]

SEPARATORS = [
    ";\n",
    "; ",
    ";",
    ";\n\n",
    ";\r\n",
    " ;\n",
    ";\n-- note\n",
    ";\n/* c; DECLARE z INT */\n",
    ";;\n",
    "\n;\n",
]
LEADS = ["", "", "\n", "  ", "-- header\n", "/* header; */\n"]
ENDS = ["", ";", ";", ";\n", "\n", " ;  ", ";\n-- trailing\n"]

# Scripts with a known shape, recorded in every dialect.
FIXED_SCRIPTS = [
    "",
    "   ",
    ";",
    ";;",
    "DECLARE x INT64",
    "SELECT 1",
    "SELECT 1; SELECT 2;",
    "DECLARE x INT64;\nSELECT 1;",
    "SET x = 1;\nDECLARE y INT64;\nSELECT 1;",
    "SELECT 'declare bankruptcy' AS msg;",
    "SET separator = ';';\nDECLARE y INT64;",
    "SET x = 1;\nBEGIN\n  DECLARE y INT64;\n  SELECT y;\nEND;",
    "SET x = 1;\nBEGIN\n  SELECT CASE WHEN x>0 THEN 'a' ELSE 'b' END;\n  DECLARE y INT64;\nEND;",
    "SET x = 1;\n-- setup\nDECLARE y INT64;",
    "SET x = 1;\nDECLARE distinct_keys array<STRING>;",
    "DECLARE distinct_keys array<STRING>;\nBEGIN TRANSACTION;\nSELECT 1;\nCOMMIT TRANSACTION;",
    "BEGIN TRANSACTION;\nSET x = 1;\nDECLARE y INT64;\nCOMMIT TRANSACTION;",
    "SELECT 'pre; 雪' AS message;\nDECLARE y INT64;",
    "SELECT 0;\nBEGIN\n BEGIN\n  DECLARE nested INT;\n END;\nEND;\nDECLARE top_level INT;",
    "SELECT 1; IF TRUE THEN DECLARE x INT64; END IF; DECLARE y INT64;",
    "SELECT 1; LOOP DECLARE x INT64; END LOOP; DECLARE y INT64;",
    "SELECT 1; BEGIN IF TRUE THEN SELECT 2; END IF; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT 1; BEGIN LOOP SELECT 2; END LOOP; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT 1; BEGIN WHILE TRUE DO SELECT 2; END WHILE; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT 1; BEGIN REPEAT SELECT 2; UNTIL TRUE END REPEAT; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT 1; BEGIN FOR r IN (SELECT 1) DO SELECT 2; END FOR; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT 1; BEGIN CASE WHEN TRUE THEN SELECT 2; END CASE; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
    "SELECT $$DECLARE fake INT;$$; DECLARE real INT;",
    "SELECT 1; DECLARE @t TABLE(id INT); SELECT * FROM @t; DECLARE @x INT;",
    "SELECT 'é;DECLARE fake INT' AS café;\n/* setup ; */ DECLARE first_value INT;\n"
    "DECLARE second_value STRING;\nSELECT 2;",
    "DECLARE x INT64;\n-- π;\nSELECT ';' AS semi;",
    "SELECT 1 -- note\n;\nDECLARE x INT64;\nSELECT 2;",
    "SELECT 1; -- DECLARE x INT64;\nSELECT 2;",
    "SELECT 1;\r\nDECLARE x INT64;\r\nSELECT 2;\r\n",
    "SELECT 1;\n\n\nDECLARE x INT64;\n\n",
    "SELECT (1;\nDECLARE x INT64;",
    "SELECT 1);\nDECLARE x INT64;",
    "BEGIN;\nDECLARE x INT64;\nCOMMIT;",
    "BEGIN\nSELECT 1;\nDECLARE x INT64;",
    "END;\nDECLARE x INT64;",
    "SELECT 1; DECLARE x STRING; SELECT 'unterminated",
    "SELECT 1; DECLARE a INT, b STRING; DECLARE c INT; SELECT 2",
]

# Hook lists with a known shape, recorded in every dialect.
FIXED_LISTS = [
    [],
    [""],
    ["  "],
    ["SELECT 1"],
    ["DECLARE x INT64"],
    ["SELECT 1", "SELECT 2"],
    ["DECLARE x INT64", "SELECT 1"],
    ["SET x = 1", "DECLARE y INT64", "SELECT 1"],
    ["SELECT 'pre; 雪' AS message", "DECLARE y INT64"],
    [
        "SELECT 1; DECLARE embedded INT",
        "  DECLARE α STRING  ",
        "-- lead\nDECLARE beta INT",
        "SELECT ';'",
    ],
    ["SELECT 'unterminated", "DECLARE x INT"],
    ["SELECT 1", "DECLARE a INT, b STRING", "DECLARE c INT; SELECT 2"],
    [
        "SELECT 17",
        "DECLARE first_var INT;\nSELECT 29",
        "DECLARE last_var INT",
        "SELECT 42",
    ],
    ["SELECT 1", "", "DECLARE x INT64", "  "],
    ["SELECT 1", "DECLARE x INT64;", "DECLARE y INT64 ;  "],
    ["BEGIN DECLARE x INT64; END", "DECLARE y INT64"],
]


def pick_statement(rnd, corpus):
    if corpus and rnd.random() < 0.3:
        return rnd.choice(corpus)
    return rnd.choice(STATEMENTS)


def piece(rnd, corpus, depth=0):
    r = rnd.random()
    if r < 0.35:
        return rnd.choice(DECLARES)
    if r < 0.8 or depth >= 2:
        return pick_statement(rnd, corpus)
    block = rnd.choice(BLOCKS)
    fills = []
    for _ in range(block.count("{}")):
        inner = [piece(rnd, corpus, depth + 1) for _ in range(rnd.randint(1, 3))]
        fills.append(rnd.choice(["; ", ";\n  "]).join(inner))
    return block.format(*fills)


def script(rnd, corpus):
    parts = [piece(rnd, corpus) for _ in range(rnd.randint(1, 7))]
    out = rnd.choice(LEADS) + parts[0]
    for p in parts[1:]:
        out += rnd.choice(SEPARATORS) + p
    return out + rnd.choice(ENDS)


def hook_entry(rnd, corpus):
    r = rnd.random()
    if r < 0.4:
        entry = rnd.choice(DECLARES)
    elif r < 0.75:
        entry = pick_statement(rnd, corpus)
    elif r < 0.9:
        entry = script(rnd, corpus)
    else:
        entry = rnd.choice(["", "  ", "-- only a comment", "\n"])
    return (
        rnd.choice(["", "", " ", "\n"]) + entry + rnd.choice(["", "", ";", " ", "\n"])
    )


def commands(seed):
    rnd = random.Random(seed)
    corpus_by_dialect = {}
    for e in g.load_corpus():
        corpus_by_dialect.setdefault(e["dialect"], []).append(e["sql"])

    cmds = []
    for d in DIALECTS:
        pool = corpus_by_dialect.get(d) or corpus_by_dialect[""]
        corpus = rnd.sample(pool, min(CORPUS_PER_DIALECT, len(pool)))
        scripts = list(FIXED_SCRIPTS) + [
            script(rnd, corpus) for _ in range(SCRIPTS_PER_DIALECT)
        ]
        for s in list(scripts[len(FIXED_SCRIPTS) :]):
            variants = [m for m in mutants(s, rnd) if m.strip()]
            if variants:
                scripts.append(rnd.choice(variants))
        for s in scripts:
            cmds.append(
                {"command": "hoist-declares", "contents": {"query": s, "dialect": d}}
            )
        lists = [list(q) for q in FIXED_LISTS] + [
            [hook_entry(rnd, corpus) for _ in range(rnd.randint(1, 6))]
            for _ in range(LISTS_PER_DIALECT)
        ]
        for q in lists:
            cmds.append(
                {
                    "command": "hoist-declares-list",
                    "contents": {"queries": q, "dialect": d},
                }
            )
    for s in FIXED_SCRIPTS[:8]:
        cmds.append(
            {
                "command": "hoist-declares",
                "contents": {"query": s, "dialect": UNKNOWN_DIALECT},
            }
        )
    for q in FIXED_LISTS[:8]:
        cmds.append(
            {
                "command": "hoist-declares-list",
                "contents": {"queries": list(q), "dialect": UNKNOWN_DIALECT},
            }
        )
    return cmds


def process(cmd):
    return g.record([cmd], runner=timed_run)[0]


if __name__ == "__main__":
    out = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.path.join(g.GOLDEN_DIR, "hoist.json.gz")
    )
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    cmds = commands(seed)
    with mp.get_context("fork").Pool(mp.cpu_count(), initializer=g.init_worker) as pool:
        results = list(pool.imap(process, cmds, chunksize=32))
    g.write(out, results)
