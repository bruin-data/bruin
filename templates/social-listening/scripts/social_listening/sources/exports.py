"""User-provided exports from platforms without a supported API path.

LinkedIn, X and Quora are only supported through data you are entitled to:
an official API under your own agreement, an export you requested, or an
approved data provider. This adapter loads such an export from a CSV file; it
never fetches anything. Required columns:

    platform, external_id, url, author, title, body, published_at, content_type

``metrics_json`` is optional. ``published_at`` is ISO 8601; relative fixture
values such as ``@-3600`` (one hour before the window end) are accepted for tests.
"""

from __future__ import annotations

import csv
import json
from datetime import timedelta
from pathlib import Path
from typing import Iterator

from ..http import utc_from_iso
from ..settings import TEMPLATE_ROOT, Window
from .base import RawRecord, SourceCollector

REQUIRED = (
    "platform",
    "external_id",
    "url",
    "author",
    "title",
    "body",
    "published_at",
    "content_type",
)
ALLOWED_PLATFORMS = {"linkedin", "x", "quora", "other"}


class ExportFileCollector(SourceCollector):
    source = "authorised_export"

    def collect(self, window: Window) -> Iterator[RawRecord]:
        configured = self.ctx.vars.get("authorised_export_path", "")
        path = Path(configured)
        if not path.is_absolute():
            path = TEMPLATE_ROOT / configured
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            missing = [c for c in REQUIRED if c not in (reader.fieldnames or [])]
            if missing:
                raise ValueError(
                    f"{path.name} is missing columns: {', '.join(missing)}"
                )
            self.stats.pages += 1
            for row in reader:
                platform = row["platform"].strip().lower()
                if platform not in ALLOWED_PLATFORMS:
                    self.partial_error(
                        row.get("external_id", "?"),
                        ValueError(f"unsupported platform {platform!r}"),
                    )
                    continue
                published = row["published_at"].strip()
                if published.startswith("@-"):
                    ts = window.end - timedelta(seconds=int(published[2:]))
                else:
                    ts = utc_from_iso(published)
                payload = {k: row.get(k) for k in REQUIRED}
                payload["metrics"] = json.loads(row.get("metrics_json") or "{}")
                yield RawRecord(
                    self.source,
                    f"{platform}:{row['external_id']}",
                    row["content_type"],
                    payload,
                    ts,
                    published,
                )
