"""@bruin
name: raw.raw_github_content
type: python
description: |
  Public GitHub issues and pull requests through the REST search API. Disabled by default (github_enabled).
  Source records for the run's bounded window (Bruin's interval widened by
  late_arrival_hours). Keyed by an event key of source, external ID and a hash
  of the content (engagement counters excluded). Rows are never rewritten: a
  record already stored is skipped, so re-running a window adds nothing, and an
  edited record is kept as a new version. Each run also writes one run_summary row with request,
  retry, rate-limit and error counts and the high watermark.
tags: [collect, raw]
depends:
  - config.settings
materialization:
  type: table
  strategy: merge
secrets:
  - key: sl-github-token
    inject_as: GITHUB_TOKEN
columns:
  - name: event_key
    type: varchar
    primary_key: true
    checks:
      - name: not_null
  - name: source
    type: varchar
    checks:
      - name: not_null
  - name: external_id
    type: varchar
    checks:
      - name: not_null
  - name: record_kind
    type: varchar
    checks:
      - name: accepted_values
        value: [content, run_summary]
  - name: content_type
    type: varchar
  - name: payload_json
    type: varchar
    checks:
      - name: not_null
  - name: payload_sha256
    type: varchar
  - name: published_at
    type: timestamp
  - name: collected_at
    type: timestamp
    checks:
      - name: not_null
  - name: source_cursor
    type: varchar
  - name: ingest_run_id
    type: varchar
    checks:
      - name: not_null
  - name: window_start
    type: timestamp
  - name: window_end
    type: timestamp
  - name: collection_mode
    type: varchar
    checks:
      - name: accepted_values
        value: [demo, live]
@bruin"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from social_listening.collect import run_source  # noqa: E402
from social_listening.frames import to_frame  # noqa: E402
from social_listening.sources.base import RAW_COLUMNS  # noqa: E402


def materialize():
    return to_frame(run_source("github", lookup_stored=True), RAW_COLUMNS)
