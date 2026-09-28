"""@bruin
name: enrichment.llm_assessment_result
type: python
description: |
  Optional structured model assessment for eligible candidates only
  (llm_enabled, off by default). Each result is validated before it is
  stored: scores must be in [0, 1], the intent must be in the taxonomy, and
  every evidence snippet must appear verbatim in the content. Failures are
  stored as status `error` or `invalid_response` with every score NULL; the
  pipeline never invents a score. Items are retried on later runs up to
  llm_max_attempts. Each run also writes a run_summary row.
tags: [enrich, llm]
depends:
  - enrichment.fct_mention_candidate
materialization:
  type: table
  strategy: merge
secrets:
  - key: sl-llm-api-key
    inject_as: LLM_API_KEY
columns:
  - name: result_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
  - name: record_kind
    type: varchar
    checks:
      - name: accepted_values
        value: [result, run_summary]
  - name: content_id
    type: varchar
  - name: provider
    type: varchar
  - name: model_id
    type: varchar
  - name: prompt_version
    type: varchar
  - name: status
    type: varchar
    checks:
      - name: accepted_values
        value: [ok, error, invalid_response, disabled, completed]
  - name: llm_relevance
    type: double
  - name: llm_intent
    type: varchar
  - name: llm_fit
    type: double
  - name: llm_confidence
    type: double
  - name: reasons_json
    type: varchar
  - name: evidence_json
    type: varchar
  - name: error_message
    type: varchar
  - name: attempt_count
    type: integer
  - name: assessed_at
    type: timestamp
  - name: run_id
    type: varchar
custom_checks:
  - name: failed assessments carry no scores
    query: |
      SELECT COUNT(*) FROM enrichment.llm_assessment_result
      WHERE status <> 'ok' AND (llm_relevance IS NOT NULL OR llm_fit IS NOT NULL OR llm_confidence IS NOT NULL OR llm_intent IS NOT NULL)
    value: 0
@bruin"""

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from social_listening import llm, warehouse  # noqa: E402
from social_listening.frames import to_frame  # noqa: E402
from social_listening.settings import load_context  # noqa: E402

TABLE = "enrichment.llm_assessment_result"


COLUMNS = [
    {"name": "result_key", "type": "varchar"},
    {"name": "record_kind", "type": "varchar"},
    {"name": "content_id", "type": "varchar"},
    {"name": "provider", "type": "varchar"},
    {"name": "model_id", "type": "varchar"},
    {"name": "prompt_version", "type": "varchar"},
    {"name": "status", "type": "varchar"},
    {"name": "llm_relevance", "type": "double"},
    {"name": "llm_intent", "type": "varchar"},
    {"name": "llm_fit", "type": "double"},
    {"name": "llm_confidence", "type": "double"},
    {"name": "reasons_json", "type": "varchar"},
    {"name": "evidence_json", "type": "varchar"},
    {"name": "error_message", "type": "varchar"},
    {"name": "attempt_count", "type": "integer"},
    {"name": "assessed_at", "type": "timestamp"},
    {"name": "run_id", "type": "varchar"},
]


def _key(*parts) -> str:
    return hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).hexdigest()


def materialize():
    ctx = load_context()
    v = ctx.vars
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    provider_name = v.get("llm_provider", "fixture")
    model_id = v.get("llm_model") or (
        "fixture-model" if provider_name == "fixture" else ""
    )
    prompt_version = v.get("llm_prompt_version", "v1")
    summary = {
        "status": "disabled",
        "attempted": 0,
        "ok": 0,
        "error": 0,
        "invalid_response": 0,
        "skipped_max_attempts": 0,
    }
    rows = []

    if v.get("llm_enabled"):
        summary["status"] = "completed"
        provider = llm.build_provider(v, ctx.secret("llm_api_key"))
        previous = {}
        if warehouse.table_exists("enrichment", "llm_assessment_result"):
            for row in warehouse.read(
                f"SELECT content_id, status, attempt_count FROM {TABLE} "
                f"WHERE record_kind = 'result' AND provider = {warehouse.literal(provider_name)} "
                f"AND model_id = {warehouse.literal(model_id)} AND prompt_version = {warehouse.literal(prompt_version)}"
            ):
                previous[row["content_id"]] = row
        candidates = warehouse.read(
            "SELECT content_id, source, external_id, content_type, community, title, body, matched_terms "
            "FROM enrichment.fct_mention_candidate WHERE is_eligible ORDER BY published_at DESC, content_id"
        )
        max_attempts = int(v.get("llm_max_attempts", 3))
        budget = int(v.get("llm_max_items_per_run", 25))
        for cand in candidates:
            prior = previous.get(cand["content_id"])
            if prior and prior["status"] == "ok":
                continue
            attempts = int(prior["attempt_count"] or 0) if prior else 0
            if attempts >= max_attempts:
                summary["skipped_max_attempts"] += 1
                continue
            if summary["attempted"] >= budget:
                break
            summary["attempted"] += 1
            result = llm.assess(cand, provider, v)
            summary[result["status"]] += 1
            rows.append(
                {
                    "result_key": _key(
                        cand["content_id"], provider_name, model_id, prompt_version
                    ),
                    "record_kind": "result",
                    "content_id": cand["content_id"],
                    "provider": provider_name,
                    "model_id": model_id,
                    "prompt_version": prompt_version,
                    **result,
                    "attempt_count": attempts + 1,
                    "assessed_at": now,
                    "run_id": ctx.run_id,
                }
            )

    rows.append(
        {
            "result_key": _key("run", ctx.run_id),
            "record_kind": "run_summary",
            "content_id": None,
            "provider": provider_name,
            "model_id": model_id,
            "prompt_version": prompt_version,
            "status": summary["status"],
            "llm_relevance": None,
            "llm_intent": None,
            "llm_fit": None,
            "llm_confidence": None,
            "reasons_json": None,
            "evidence_json": None,
            "error_message": json.dumps(summary),
            "attempt_count": 0,
            "assessed_at": now,
            "run_id": ctx.run_id,
        }
    )
    print(f"llm assessment: {json.dumps(summary)}")
    return to_frame(rows, COLUMNS)
