"""Hacker News through the public Algolia HN Search API.

One ``search_by_date`` query per phrase, bounded with
``numericFilters=created_at_i>=start,created_at_i<end`` and paginated with
``page``. Algolia returns at most 1,000 hits per query, so when a window has
more than that the window is split in half (down to one hour) instead of
silently truncating.
"""

from __future__ import annotations

import urllib.parse
from datetime import timedelta
from typing import Iterator

from ..http import utc_from_epoch
from ..settings import Window, search_phrases, slug
from .base import RawRecord, SourceCollector

API = "https://hn.algolia.com/api/v1/search_by_date"
ALGOLIA_HIT_CAP = 1000
KEEP_FIELDS = (
    "objectID",
    "_tags",
    "title",
    "url",
    "story_text",
    "comment_text",
    "author",
    "points",
    "num_comments",
    "parent_id",
    "story_id",
    "story_title",
    "created_at_i",
)


def fixture_route(method: str, url: str) -> str | None:
    params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    return f"hackernews/{slug(params.get('query', '').strip(chr(34)))}_p{params.get('page', '0')}.json"


class HackerNewsCollector(SourceCollector):
    source = "hackernews"

    def _tags(self) -> str:
        if self.ctx.vars.get("hackernews_include_comments", True):
            return "(story,comment)"
        return "story"

    def _search(self, phrase: str, window: Window, depth: int = 0) -> Iterator[dict]:
        max_pages = int(self.ctx.vars.get("max_pages_per_source", 5))
        page = 0
        while page < max_pages:
            body = self.http.get_json(
                API,
                params={
                    "query": f'"{phrase}"',
                    "tags": self._tags(),
                    "numericFilters": f"created_at_i>={window.start_epoch},created_at_i<{window.end_epoch}",
                    "hitsPerPage": 100,
                    "page": page,
                },
            )
            self.stats.pages += 1
            body = body or {}
            if page == 0 and body.get("nbHits", 0) > ALGOLIA_HIT_CAP:
                if depth < 6 and window.end - window.start > timedelta(hours=1):
                    left, right = window.split()
                    yield from self._search(phrase, left, depth + 1)
                    yield from self._search(phrase, right, depth + 1)
                    return
                self.partial_error(
                    f"query {phrase!r}",
                    RuntimeError("more than 1,000 hits in one hour; results truncated"),
                )
            yield from body.get("hits") or []
            page += 1
            if page >= int(body.get("nbPages") or 0):
                return
        self.partial_error(
            f"query {phrase!r}",
            RuntimeError(f"stopped after max_pages_per_source={max_pages}"),
        )

    def collect(self, window: Window) -> Iterator[RawRecord]:
        for phrase in search_phrases(self.ctx.vars):
            try:
                for hit in self._search(phrase, window):
                    tags = hit.get("_tags") or []
                    content_type = "story" if "story" in tags else "comment"
                    self.stats.cursor = str(
                        hit.get("created_at_i") or self.stats.cursor
                    )
                    yield RawRecord(
                        source=self.source,
                        external_id=str(hit.get("objectID")),
                        content_type=content_type,
                        payload={k: hit.get(k) for k in KEEP_FIELDS if k in hit},
                        published_at=utc_from_epoch(hit.get("created_at_i")),
                        cursor=str(hit.get("created_at_i") or ""),
                    )
            except Exception as err:
                if getattr(err, "status", None) in (401, 403):
                    raise
                self.partial_error(f"query {phrase!r}", err)
