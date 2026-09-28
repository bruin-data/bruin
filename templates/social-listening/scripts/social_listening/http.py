"""Small HTTP client with pacing, bounded retries and rate-limit handling.

Collectors never call ``urllib`` directly. They go through ``HttpClient`` so
that every source gets the same behaviour: a minimum gap between requests,
``Retry-After`` and ``X-Ratelimit-*`` support, and a hard retry ceiling. In
demo mode the transport is swapped for ``FixtureTransport``, which serves
recorded API responses, so the same parsing and pagination code runs offline.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
TIME_KEYS = {"created_utc", "created_at_i", "creation_date", "last_activity_date", "ts"}


class HttpError(RuntimeError):
    def __init__(self, status: int, url: str, message: str = ""):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} for {redact_url(url)} {message}".strip())


def redact_url(url: str) -> str:
    """Drop query-string values that could carry credentials before logging."""
    parts = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    safe = [
        (
            k,
            "***"
            if k.lower() in {"key", "access_token", "token", "client_secret"}
            else v,
        )
        for k, v in query
    ]
    return urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(safe)))


@dataclass
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8") or "null")


class Transport:
    def send(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> Response:
        raise NotImplementedError


class UrllibTransport(Transport):
    def send(self, method, url, headers, body, timeout):
        request = urllib.request.Request(
            url, data=body, method=method, headers=dict(headers)
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as resp:  # noqa: S310 - URLs come from source adapters
                return Response(
                    resp.status,
                    {k.lower(): v for k, v in resp.headers.items()},
                    resp.read(),
                )
        except urllib.error.HTTPError as err:
            return Response(
                err.code,
                {k.lower(): v for k, v in err.headers.items()},
                err.read() or b"",
            )


def _rebase(value: Any, anchor: datetime) -> Any:
    """Turn relative fixture times into absolute ones anchored at the run's end."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            if key in TIME_KEYS and isinstance(item, (int, float)) and item <= 0:
                out[key] = int(anchor.timestamp() + item)
            else:
                out[key] = _rebase(item, anchor)
        return out
    if isinstance(value, list):
        return [_rebase(v, anchor) for v in value]
    if isinstance(value, str) and value.startswith("@-") and value[2:].isdigit():
        ts = anchor - timedelta(seconds=int(value[2:]))
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    return value


class FixtureTransport(Transport):
    """Serves recorded responses. ``route`` maps a request URL to a fixture file name.

    Fixtures store times relative to the run (``created_utc: -3600`` means one hour
    before the window end) so the demo stays inside Bruin's default interval.
    """

    def __init__(
        self, root: Path, anchor: datetime, route: Callable[[str, str], str | None]
    ):
        self.root = root
        self.anchor = anchor
        self.route = route
        self.requests: list[str] = []

    def send(self, method, url, headers, body, timeout):
        self.requests.append(url)
        name = self.route(method, url)
        if name is None:
            return Response(404, {}, b'{"error": "no fixture"}')
        path = self.root / name
        if not path.exists():
            return Response(200, {}, b"{}")
        data = _rebase(json.loads(path.read_text()), self.anchor)
        return Response(
            200, {"content-type": "application/json"}, json.dumps(data).encode()
        )


@dataclass
class HttpStats:
    requests: int = 0
    retries: int = 0
    rate_limit_waits: int = 0
    errors: int = 0
    waited_seconds: float = 0.0
    error_messages: list[str] = field(default_factory=list)


class HttpClient:
    def __init__(
        self,
        transport: Transport,
        *,
        min_interval_seconds: float = 1.0,
        max_retries: int = 3,
        max_backoff_seconds: float = 60.0,
        timeout_seconds: float = 30.0,
        user_agent: str = "social-listening-template/1.0",
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.transport = transport
        self.min_interval = min_interval_seconds
        self.max_retries = max_retries
        self.max_backoff = max_backoff_seconds
        self.timeout = timeout_seconds
        self.user_agent = user_agent
        self.sleep = sleep
        self.clock = clock
        self.stats = HttpStats()
        self._last_request: float | None = None
        self._blocked_until: float = 0.0

    def _wait(self, seconds: float, rate_limited: bool = False) -> None:
        if seconds <= 0:
            return
        seconds = min(seconds, self.max_backoff)
        if rate_limited:
            self.stats.rate_limit_waits += 1
        self.stats.waited_seconds += seconds
        self.sleep(seconds)

    def pause(self, seconds: float) -> None:
        """Honour a server-requested pause (for example Stack Exchange ``backoff``)."""
        self._wait(seconds, rate_limited=True)

    def _pace(self) -> None:
        now = self.clock()
        if self._blocked_until > now:
            self._wait(self._blocked_until - now, rate_limited=True)
            now = self.clock()
        if self._last_request is not None:
            gap = self.min_interval - (now - self._last_request)
            if gap > 0:
                self._wait(gap)
        self._last_request = self.clock()

    def _note_rate_headers(self, headers: Mapping[str, str]) -> None:
        remaining = headers.get("x-ratelimit-remaining")
        reset = headers.get("x-ratelimit-reset")
        if remaining is not None and reset is not None:
            try:
                if float(remaining) < 1:
                    wait = float(reset)
                    if (
                        wait > 1e9
                    ):  # GitHub sends an epoch timestamp; Reddit sends seconds until reset
                        wait = max(0.0, wait - time.time())
                    self._blocked_until = self.clock() + wait
            except ValueError:
                pass

    def _retry_delay(self, attempt: int, headers: Mapping[str, str]) -> float:
        retry_after = headers.get("retry-after")
        if retry_after:
            try:
                return float(retry_after)
            except ValueError:
                pass
        return min(self.max_backoff, 2.0**attempt)

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        json_body: Any = None,
        form: Mapping[str, str] | None = None,
    ) -> Response:
        if params:
            url = f"{url}{'&' if '?' in url else '?'}{urllib.parse.urlencode(params)}"
        all_headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json",
            **(headers or {}),
        }
        body = None
        if json_body is not None:
            body = json.dumps(json_body).encode()
            all_headers["Content-Type"] = "application/json"
        elif form is not None:
            body = urllib.parse.urlencode(form).encode()
            all_headers["Content-Type"] = "application/x-www-form-urlencoded"

        attempt = 0
        while True:
            self._pace()
            self.stats.requests += 1
            try:
                resp = self.transport.send(method, url, all_headers, body, self.timeout)
            except (urllib.error.URLError, TimeoutError, OSError) as err:
                resp = Response(599, {}, str(err).encode())
            self._note_rate_headers(resp.headers)
            if resp.status < 400:
                return resp
            if resp.status in RETRYABLE_STATUS and attempt < self.max_retries:
                attempt += 1
                self.stats.retries += 1
                self._wait(
                    self._retry_delay(attempt, resp.headers),
                    rate_limited=resp.status == 429,
                )
                continue
            self.stats.errors += 1
            error = HttpError(
                resp.status, url, resp.body[:200].decode("utf-8", "replace")
            )
            self.stats.error_messages.append(str(error)[:300])
            raise error

    def get_json(self, url: str, **kwargs) -> Any:
        return self.request("GET", url, **kwargs).json()


def utc_from_epoch(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def utc_from_iso(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
