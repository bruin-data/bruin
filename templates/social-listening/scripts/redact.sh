#!/usr/bin/env bash
# Delete one source record everywhere and keep it suppressed.
#
#   bash scripts/redact.sh <source> <external_id> "<reason>" [--env <environment>]
#
# 1. Appends the request to assets/config/redaction_requests.csv (commit it),
#    so staging drops the record on every future run even if it is re-collected.
# 2. Deletes the raw versions and every derived row now, instead of waiting
#    for the next run.
# Delivered notifications that already left the warehouse (Slack, webhooks)
# must be removed in those tools by hand; the outbox row tells you where.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="$1"; EXTERNAL_ID="$2"; REASON="${3:-deletion request}"; shift 3 || true
ENV_ARGS=()
if [[ "${1:-}" == "--env" ]]; then ENV_ARGS=(--environment "$2"); fi

case "$SOURCE" in
  reddit|hackernews|github|stackoverflow|authorised_export) ;;
  *) echo "unknown source: $SOURCE" >&2; exit 2 ;;
esac
if [[ ! "$EXTERNAL_ID" =~ ^[A-Za-z0-9_:.-]+$ ]]; then echo "unexpected characters in external_id" >&2; exit 2; fi

printf '%s,%s,%s,"%s"\n' "$SOURCE" "$EXTERNAL_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${REASON//\"/\'}" >> "$ROOT/assets/config/redaction_requests.csv"

ID="md5('${SOURCE}:${EXTERNAL_ID}')"
run() { bruin query --connection social-listening-warehouse ${ENV_ARGS[@]+"${ENV_ARGS[@]}"} --query "$1" >/dev/null; }
run "DELETE FROM raw.raw_${SOURCE}_content WHERE source = '${SOURCE}' AND external_id = '${EXTERNAL_ID}'"
run "DELETE FROM operations.alert_delivery_attempt WHERE alert_key IN (SELECT alert_key FROM operations.fct_alert_decision WHERE content_id = ${ID})"
run "DELETE FROM operations.fct_alert_decision WHERE content_id = ${ID}"
run "DELETE FROM enrichment.llm_assessment_result WHERE content_id = ${ID}"
run "DELETE FROM enrichment.fct_mention_assessment WHERE content_id = ${ID}"
echo "Deleted ${SOURCE}:${EXTERNAL_ID}. Run the pipeline to rebuild staging and marts, and commit redaction_requests.csv."
