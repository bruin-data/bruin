"""@bruin
name: config.settings
type: python
description: |
  Validates every custom variable against the schema in pipeline.yml, then the
  cross-field policy rules (reply drafts need human approval, enabled sources
  need their credentials, thresholds are in range, the backfill window is
  bounded). Every other asset depends on this one, so an invalid setting stops
  the run before any source is contacted. Stores a redacted snapshot of the
  settings used by each run; secrets are recorded as present or absent only.
tags: [collect, enrich, report]
materialization:
  type: table
  strategy: merge
secrets:
  - key: sl-reddit-client-id
    inject_as: REDDIT_CLIENT_ID
  - key: sl-reddit-client-secret
    inject_as: REDDIT_CLIENT_SECRET
  - key: sl-reddit-user-agent
    inject_as: REDDIT_USER_AGENT
  - key: sl-github-token
    inject_as: GITHUB_TOKEN
  - key: sl-stackexchange-key
    inject_as: STACKEXCHANGE_KEY
  - key: sl-webhook-url
    inject_as: SOCIAL_LISTENING_WEBHOOK_URL
  - key: sl-slack-webhook-url
    inject_as: SOCIAL_LISTENING_SLACK_WEBHOOK_URL
  - key: sl-llm-api-key
    inject_as: LLM_API_KEY
columns:
  - name: run_id
    type: varchar
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: template_version
    type: varchar
  - name: validated_at
    type: timestamp
    checks:
      - name: not_null
  - name: window_start
    type: timestamp
  - name: window_end
    type: timestamp
  - name: settings_json
    type: varchar
  - name: secrets_present_json
    type: varchar
@bruin"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from social_listening import TEMPLATE_VERSION  # noqa: E402
from social_listening.frames import to_frame  # noqa: E402
from social_listening.settings import load_context, redacted_snapshot, validate  # noqa: E402


COLUMNS = [
    {"name": "run_id", "type": "varchar"},
    {"name": "template_version", "type": "varchar"},
    {"name": "validated_at", "type": "timestamp"},
    {"name": "window_start", "type": "timestamp"},
    {"name": "window_end", "type": "timestamp"},
    {"name": "settings_json", "type": "varchar"},
    {"name": "secrets_present_json", "type": "varchar"},
]


def materialize():
    ctx = load_context()
    validate(ctx)
    snapshot = redacted_snapshot(ctx)
    window = ctx.collection_window()
    print(
        f"settings valid; window {window.start.isoformat()} .. {window.end.isoformat()}; demo_mode={ctx.vars.get('demo_mode')}"
    )
    return to_frame(
        [
            {
                "run_id": ctx.run_id,
                "template_version": TEMPLATE_VERSION,
                "validated_at": datetime.now(timezone.utc).replace(
                    tzinfo=None, microsecond=0
                ),
                "window_start": window.start.replace(tzinfo=None),
                "window_end": window.end.replace(tzinfo=None),
                "settings_json": json.dumps(snapshot["vars"], sort_keys=True),
                "secrets_present_json": json.dumps(
                    snapshot["secrets_present"], sort_keys=True
                ),
            }
        ],
        COLUMNS,
    )
