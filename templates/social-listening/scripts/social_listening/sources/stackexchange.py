"""Stack Overflow questions through the Stack Exchange API 2.3.

Disabled by default (``stackoverflow_enabled``). A key (``STACKEXCHANGE_KEY``)
is optional but raises the daily quota. The API sends a ``backoff`` field when
a client must pause; the collector sleeps for it before the next request.
Content is CC BY-SA: keep the link and author attribution when you display it.
"""

from __future__ import annotations

import urllib.parse
from typing import Iterator

from ..http import utc_from_epoch
from ..settings import Window, search_phrases, slug
from .base import RawRecord, SourceCollector

API = "https://api.stackexchange.com/2.3/search/advanced"


def fixture_route(method: str, url: str) -> str | None:
    params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    return f"stackexchange/{slug(params.get('q', ''))}_p{params.get('page', '1')}.json"


class StackExchangeCollector(SourceCollector):
    source = "stackoverflow"

    def collect(self, window: Window) -> Iterator[RawRecord]:
        key = self.ctx.secret("stackexchange_key")
        max_pages = int(self.ctx.vars.get("max_pages_per_source", 5))
        for phrase in search_phrases(self.ctx.vars):
            try:
                for page in range(1, max_pages + 1):
                    params = {
                        "q": phrase,
                        "site": "stackoverflow",
                        "fromdate": window.start_epoch,
                        "todate": window.end_epoch - 1,
                        "sort": "creation",
                        "order": "desc",
                        "pagesize": 100,
                        "page": page,
                        "filter": "withbody",
                    }
                    if key:
                        params["key"] = key
                    body = self.http.get_json(API, params=params) or {}
                    self.stats.pages += 1
                    for item in body.get("items") or []:
                        payload = {
                            "question_id": item.get("question_id"),
                            "title": item.get("title"),
                            "body": item.get("body"),
                            "link": item.get("link"),
                            "author": (item.get("owner") or {}).get("display_name"),
                            "creation_date": item.get("creation_date"),
                            "score": item.get("score"),
                            "answer_count": item.get("answer_count"),
                            "view_count": item.get("view_count"),
                            "tags": item.get("tags"),
                        }
                        yield RawRecord(
                            self.source,
                            str(item.get("question_id")),
                            "question",
                            payload,
                            utc_from_epoch(item.get("creation_date")),
                            str(item.get("creation_date") or ""),
                        )
                    if body.get("backoff"):
                        self.http.pause(float(body["backoff"]))
                    if not body.get("has_more"):
                        break
                else:
                    self.partial_error(
                        f"query {phrase!r}",
                        RuntimeError(f"stopped after max_pages_per_source={max_pages}"),
                    )
            except Exception as err:
                if getattr(err, "status", None) in (401, 403):
                    raise
                self.partial_error(f"query {phrase!r}", err)
