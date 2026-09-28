"""GitHub public issues and pull requests through the REST search API.

Disabled by default (``github_enabled``). Needs a token (``GITHUB_TOKEN``) with
read access to public repositories only; the search API allows 30 requests per
minute, which the 2.1 s pacing respects. Results are limited to ``is:public``.
The search API caps results at 1,000 per query; windows over the cap are split.
"""

from __future__ import annotations

import urllib.parse
from datetime import timedelta
from typing import Iterator

from ..http import utc_from_iso
from ..settings import Window, search_phrases, slug
from .base import RawRecord, SourceCollector

API = "https://api.github.com/search/issues"
HIT_CAP = 1000


def fixture_route(method: str, url: str) -> str | None:
    params = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    phrase = params.get("q", "").split('"')[1] if '"' in params.get("q", "") else "term"
    return f"github/{slug(phrase)}_p{params.get('page', '1')}.json"


def _iso(ts) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


class GitHubCollector(SourceCollector):
    source = "github"

    def _search(
        self, phrase: str, window: Window, headers: dict, depth: int = 0
    ) -> Iterator[dict]:
        max_pages = int(self.ctx.vars.get("max_pages_per_source", 5))
        # GitHub's created: range is inclusive on both ends; the collector's window check trims the end.
        q = f'"{phrase}" in:title,body is:public created:{_iso(window.start)}..{_iso(window.end)}'
        for page in range(1, max_pages + 1):
            body = self.http.get_json(
                API,
                params={
                    "q": q,
                    "sort": "created",
                    "order": "desc",
                    "per_page": 100,
                    "page": page,
                },
                headers=headers,
            )
            self.stats.pages += 1
            body = body or {}
            if (
                page == 1
                and body.get("total_count", 0) > HIT_CAP
                and depth < 6
                and window.end - window.start > timedelta(hours=1)
            ):
                left, right = window.split()
                yield from self._search(phrase, left, headers, depth + 1)
                yield from self._search(phrase, right, headers, depth + 1)
                return
            items = body.get("items") or []
            yield from items
            if len(items) < 100:
                return
        self.partial_error(
            f"query {phrase!r}",
            RuntimeError(f"stopped after max_pages_per_source={max_pages}"),
        )

    def collect(self, window: Window) -> Iterator[RawRecord]:
        token = self.ctx.secret("github_token")
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        for phrase in search_phrases(self.ctx.vars):
            try:
                for item in self._search(phrase, window, headers):
                    kind = "pull_request" if item.get("pull_request") else "issue"
                    payload = {
                        "id": item.get("id"),
                        "number": item.get("number"),
                        "html_url": item.get("html_url"),
                        "repository_url": item.get("repository_url"),
                        "title": item.get("title"),
                        "body": item.get("body"),
                        "author": (item.get("user") or {}).get("login"),
                        "author_type": (item.get("user") or {}).get("type"),
                        "created_at": item.get("created_at"),
                        "comments": item.get("comments"),
                        "reactions": (item.get("reactions") or {}).get("total_count"),
                        "state": item.get("state"),
                    }
                    yield RawRecord(
                        self.source,
                        str(item.get("id")),
                        kind,
                        payload,
                        utc_from_iso(item.get("created_at")),
                        item.get("created_at") or "",
                    )
            except Exception as err:
                if getattr(err, "status", None) in (401, 403):
                    raise
                self.partial_error(f"query {phrase!r}", err)
