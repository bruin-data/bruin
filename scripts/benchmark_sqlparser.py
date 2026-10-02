"""Compare SQLGlot environments through Bruin's real subprocess protocol.

Create separate Python environments with the SQLGlot versions/extras to compare,
then pass each interpreter with --python. For example:

    uv venv --python 3.13.14 /tmp/sqlglot-new
    uv pip install --python /tmp/sqlglot-new/bin/python 'sqlglot==30.21.0'
    python3 scripts/benchmark_sqlparser.py --python /tmp/sqlglot-new/bin/python

Reports median warm-cache timings, including IPC and normal parser logging.
Each sample starts a new process. The first sample warms bytecode/filesystem
caches and is discarded. Runtime extraction is deliberately not measured here.
Lineage uses the existing fixture corpus; table extraction uses four dialects.
"""

import argparse
import json
from pathlib import Path
import statistics
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
TABLE_QUERY = (
    "WITH c AS (SELECT id FROM raw.orders) "
    "SELECT c.id FROM c JOIN analytics.customers u ON c.id = u.id"
)


def normalized_columns(columns):
    # Match the existing Go fixture test's equivalent type spellings.
    aliases = {"UNKNOWN": "TEXT", "TIMESTAMP": "TIMESTAMPLTZ"}
    return [{**c, "type": aliases.get(c["type"], c["type"])} for c in columns]


def benchmark(python, runs, repetitions):
    environment = json.loads(
        subprocess.check_output(
            [
                python,
                "-c",
                "import json, sys, sqlglot, sqlglot.parser; "
                "print(json.dumps({'python_version': sys.version.split()[0], "
                "'sqlglot_version': sqlglot.__version__, "
                "'parser_module': sqlglot.parser.__file__}))",
            ],
            text=True,
        )
    )
    fixtures = json.loads(
        (ROOT / "pkg/sqlparser/testdata/python_main_lineage_cases.json").read_text()
    )
    tables = [
        (
            {
                "command": "get-tables",
                "contents": {"query": TABLE_QUERY, "dialect": dialect},
            },
            {"tables": ["analytics.customers", "raw.orders"]},
        )
        for dialect in ("postgres", "bigquery", "snowflake", "tsql")
    ]
    samples = []
    for sample in range(runs + 1):
        start = time.perf_counter()
        with subprocess.Popen(
            [python, str(ROOT / "pythonsrc/main.py")],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        ) as process:

            def request(command):
                process.stdin.write(json.dumps(command) + "\n")
                process.stdin.flush()
                response = process.stdout.readline()
                if not response:
                    raise RuntimeError("parser exited before returning a response")
                return json.loads(response)

            try:
                assert request({"command": "init"}) == {}
                init_ms = (time.perf_counter() - start) * 1000
                start = time.perf_counter()
                assert request(tables[0][0]) == tables[0][1]
                first_ms = (time.perf_counter() - start) * 1000

                def table_batch():
                    for command, expected in tables:
                        assert request(command) == expected

                def lineage_batch():
                    for fixture in fixtures:
                        result = request(
                            {
                                "command": "lineage",
                                "contents": {
                                    k: fixture[k]
                                    for k in ("query", "dialect", "schema")
                                },
                            }
                        )
                        assert normalized_columns(
                            result["columns"]
                        ) == normalized_columns(fixture["expected"]), fixture["name"]
                        assert (
                            result["non_selected_columns"]
                            == fixture["expected_non_selected"]
                        ), fixture["name"]

                row = {"init_ms": init_ms, "first_tables_ms": first_ms}
                for name, batch in (
                    ("tables", table_batch),
                    ("lineage", lineage_batch),
                ):
                    batch()  # Load the required dialects before timing throughput.
                    start = time.perf_counter()
                    for _ in range(repetitions):
                        batch()
                    row[name + "_batch_ms"] = (
                        (time.perf_counter() - start) * 1000 / repetitions
                    )
                if sample:
                    samples.append(row)
            finally:
                if process.poll() is None:
                    process.stdin.write('{"command":"exit"}\n')
                    process.stdin.flush()
                process.wait(timeout=10)
            if process.returncode:
                raise RuntimeError(f"parser exited with status {process.returncode}")

    return {
        "python": python,
        **environment,
        "runs": runs,
        "tables_per_batch": len(tables),
        "lineage_per_batch": len(fixtures),
        "median_ms": {
            key: round(statistics.median(row[key] for row in samples), 3)
            for key in samples[0]
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", action="append", required=True)
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    if args.runs < 1 or args.repetitions < 1:
        parser.error("runs and repetitions must be positive")
    for python in args.python:
        print(json.dumps(benchmark(python, args.runs, args.repetitions)), flush=True)


if __name__ == "__main__":
    main()
