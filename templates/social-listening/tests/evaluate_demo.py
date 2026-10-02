"""Compare the demo warehouse with fixtures/evaluation_cases.json.

Run after `bruin run` with default variables (deterministic scoring):

    python3 tests/evaluate_demo.py [path-to-bruin]

Run it from the directory that holds the project's .bruin.yml. Exits non-zero
and prints every mismatch when an outcome differs from the expectation.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONNECTION = "social-listening-warehouse"


def query(bruin: str, sql: str) -> list[dict]:
    out = subprocess.run(
        [
            bruin,
            "query",
            "--connection",
            CONNECTION,
            "--output",
            "json",
            "--query",
            sql,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    data = json.loads(out[out.index("{") :])
    names = [c["name"] for c in data["columns"]]
    return [dict(zip(names, row)) for row in data.get("rows") or []]


def evaluate(bruin: str) -> list[str]:
    cases = json.loads((ROOT / "fixtures" / "evaluation_cases.json").read_text())[
        "cases"
    ]
    rows = query(
        bruin,
        """
        SELECT s.source, s.external_id,
               COALESCE(c.accepted_terms, 0) > 0 AS accepted_match,
               c.is_eligible AS eligible,
               c.exclusion_reason,
               EXISTS (
                   SELECT 1 FROM operations.fct_alert_decision d
                   WHERE d.content_id = s.content_id AND d.routing_policy_version = 'v1'
               ) AS routed
        FROM staging.stg_content_item s
        LEFT JOIN enrichment.fct_mention_candidate c ON c.content_id = s.content_id
        """,
    )
    actual = {(r["source"], r["external_id"]): r for r in rows}
    failures = []
    for case in cases:
        key = (case["source"], case["external_id"])
        expect = case["expect"]
        row = actual.get(key)
        if expect.get("present") is False:
            if row is not None:
                failures.append(
                    f"{key} ({case['category']}): expected absent, found in staging"
                )
            continue
        if row is None:
            failures.append(f"{key} ({case['category']}): missing from staging")
            continue
        for field, wanted in expect.items():
            if field == "present":
                continue
            got = row.get(field)
            if got != wanted:
                failures.append(
                    f"{key} ({case['category']}): {field} expected {wanted!r}, got {got!r}"
                )
    return failures


def main() -> int:
    bruin = sys.argv[1] if len(sys.argv) > 1 else "bruin"
    failures = evaluate(bruin)
    cases = json.loads((ROOT / "fixtures" / "evaluation_cases.json").read_text())[
        "cases"
    ]
    if failures:
        print("evaluation FAILED:\n  " + "\n  ".join(failures))
        return 1
    print(f"evaluation passed: {len(cases)} cases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
