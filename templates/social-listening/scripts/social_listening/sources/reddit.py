"""Reddit through the official OAuth API (application-only client credentials).

* Posts: ``/search`` (or ``/r/<community>/search`` for each included community),
  sorted newest first, paginated with ``after`` until the window start is passed
  or ``max_pages_per_source`` is reached.
* Comments: ``/r/<community>/comments`` for included communities only, and only
  when ``reddit_include_comments`` is true. Reddit search does not index
  comments, so comments are pre-filtered locally to those containing a
  configured phrase; nothing else is stored.
* Excluded communities are dropped before storage.
* Only a minimal set of public fields is kept (see ``KEEP_FIELDS``).

Reddit's API terms require a registered app, a descriptive User-Agent, and
honouring deletion. See docs/sources-and-privacy.md.
"""

from __future__ import annotations

import base64
import urllib.parse
from datetime import timedelta
from typing import Iterator

from ..http import utc_from_epoch
from ..settings import Window, search_phrases
from .base import RawRecord, SourceCollector

AUTH_URL = "https://www.reddit.com/api/v1/access_token"
API = "https://oauth.reddit.com"
KEEP_FIELDS = (
    "id",
    "name",
    "subreddit",
    "author",
    "title",
    "selftext",
    "body",
    "permalink",
    "url",
    "created_utc",
    "score",
    "num_comments",
    "parent_id",
    "link_id",
    "over_18",
    "stickied",
    "distinguished",
    "removed_by_category",
    "crosspost_parent",
    "is_self",
)
MAX_QUERY_CHARS = 400


def time_filter(window: Window) -> str:
    span = window.end - window.start
    for name, limit in (
        ("hour", 1 / 24),
        ("day", 1),
        ("week", 7),
        ("month", 31),
        ("year", 366),
    ):
        if span <= timedelta(days=limit):
            return name
    return "all"


def query_chunks(phrases: list[str]) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    for phrase in phrases:
        quoted = '"' + phrase.replace('"', "") + '"'
        candidate = " OR ".join(current + [quoted])
        if current and len(candidate) > MAX_QUERY_CHARS:
            chunks.append(" OR ".join(current))
            current = [quoted]
        else:
            current.append(quoted)
    if current:
        chunks.append(" OR ".join(current))
    return chunks


def minimal(data: dict) -> dict:
    kept = {k: data.get(k) for k in KEEP_FIELDS if k in data}
    parents = data.get("crosspost_parent_list") or []
    if parents and not kept.get("crosspost_parent"):
        kept["crosspost_parent"] = parents[0].get("name")
    return kept


def fixture_route(method: str, url: str) -> str | None:
    parts = urllib.parse.urlsplit(url)
    params = dict(urllib.parse.parse_qsl(parts.query))
    after = params.get("after") or "first"
    segments = [s for s in parts.path.split("/") if s]
    if parts.path.endswith("/access_token"):
        return "reddit/token.json"
    if segments[-1:] == ["search"]:
        community = segments[1] if segments[0] == "r" else "all"
        return f"reddit/search_{community}_{after}.json"
    if segments[-1:] == ["comments"] and segments[0] == "r":
        return f"reddit/comments_{segments[1]}_{after}.json"
    return None


class RedditCollector(SourceCollector):
    source = "reddit"

    def _token(self) -> str:
        client_id = self.ctx.secret("reddit_client_id") or "demo"
        client_secret = self.ctx.secret("reddit_client_secret") or "demo"
        basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        resp = self.http.request(
            "POST",
            AUTH_URL,
            headers={"Authorization": f"Basic {basic}"},
            form={"grant_type": "client_credentials"},
        )
        return resp.json()["access_token"]

    def _listing(
        self, url: str, params: dict, headers: dict, window: Window, scope: str
    ) -> Iterator[dict]:
        after = None
        max_pages = int(self.ctx.vars.get("max_pages_per_source", 5))
        for _ in range(max_pages):
            page_params = dict(params, limit=100, raw_json=1)
            if after:
                page_params["after"] = after
            body = self.http.get_json(url, params=page_params, headers=headers)
            self.stats.pages += 1
            data = (body or {}).get("data") or {}
            children = data.get("children") or []
            reached_start = False
            for child in children:
                item = child.get("data") or {}
                created = utc_from_epoch(item.get("created_utc"))
                if created is not None and created < window.start:
                    reached_start = True
                    continue
                yield item
            after = data.get("after")
            self.stats.cursor = after or self.stats.cursor
            if not after or reached_start or not children:
                return
        self.partial_error(
            scope,
            RuntimeError(
                f"stopped after max_pages_per_source={max_pages}; window may be incomplete"
            ),
        )

    def collect(self, window: Window) -> Iterator[RawRecord]:
        v = self.ctx.vars
        headers = {"Authorization": f"Bearer {self._token()}"}
        include = [c.lower() for c in v.get("reddit_communities_include", [])]
        exclude = {c.lower() for c in v.get("reddit_communities_exclude", [])}
        phrases = search_phrases(v)
        lowered = [p.lower() for p in phrases]
        scopes = include or [None]

        for community in scopes:
            url = f"{API}/r/{community}/search" if community else f"{API}/search"
            for q in query_chunks(phrases):
                params = {
                    "q": q,
                    "sort": "new",
                    "t": time_filter(window),
                    "type": "link",
                }
                if community:
                    params["restrict_sr"] = 1
                try:
                    for item in self._listing(
                        url, params, headers, window, f"search {community or 'all'}"
                    ):
                        if (item.get("subreddit") or "").lower() in exclude:
                            continue
                        yield self._record(item, "post")
                # One community failing must not hide the others.
                except Exception as err:
                    if getattr(err, "status", None) in (401, 403) and community is None:
                        raise
                    self.partial_error(f"search {community or 'all'}", err)

        if v.get("reddit_include_comments", True):
            for community in include:
                try:
                    for item in self._listing(
                        f"{API}/r/{community}/comments",
                        {},
                        headers,
                        window,
                        f"comments {community}",
                    ):
                        text = (item.get("body") or "").lower()
                        if any(p in text for p in lowered):
                            yield self._record(item, "comment")
                except Exception as err:
                    self.partial_error(f"comments {community}", err)

    def _record(self, item: dict, content_type: str) -> RawRecord:
        return RawRecord(
            source=self.source,
            external_id=item.get("name")
            or f"{'t3' if content_type == 'post' else 't1'}_{item.get('id')}",
            content_type=content_type,
            payload=minimal(item),
            published_at=utc_from_epoch(item.get("created_utc")),
            cursor=item.get("name") or "",
        )
