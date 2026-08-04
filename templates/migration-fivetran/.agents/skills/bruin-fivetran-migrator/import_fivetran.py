#!/usr/bin/env python3
"""Capture one redacted Fivetran connection and its schema configuration.

The script performs GET requests only. It reads Fivetran credentials from an
untracked Bruin config, then writes safe JSON files under `.artifacts/`.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, parse, request

try:
    import yaml
except ModuleNotFoundError as exc:  # pragma: no cover - environment setup error
    raise SystemExit("PyYAML is required: python3 -m pip install PyYAML") from exc


API_BASE = "https://api.fivetran.com/v1"
MIGRATION_ROOT = Path(__file__).resolve().parents[3]
ARTIFACT_ROOT = MIGRATION_ROOT / ".artifacts"
SECRET_FIELD = re.compile(
    r"api[_-]?key|secret|password|token|authorization|private[_-]?key|"
    r"client[_-]?secret|connection[_-]?string|credential",
    re.IGNORECASE,
)
NETWORK_FIELD = re.compile(
    r"(?:^|[_-])(?:host(?:name)?|endpoint|url|uri|address|port)(?:[_-]|$)",
    re.IGNORECASE,
)
ENV_TOKEN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
BRUIN_ENV = re.compile(
    r"^\{\{\s*env_var\(\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*\)\s*\}\}$"
)


class ImportError(RuntimeError):
    """An error safe to show to the user."""


def fail(message: str) -> None:
    raise ImportError(message)


def artifact_roots() -> tuple[Path, ...]:
    return (ARTIFACT_ROOT.resolve(),)


def require_artifact_path(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    for root in artifact_roots():
        try:
            relative = resolved.relative_to(root)
            if len(relative.parts) < 2:
                fail("output must name one capture directory at least two levels below an artifact root")
            return resolved
        except ValueError:
            pass
    fail(f"output must be below {ARTIFACT_ROOT}")


def load_config(path: Path, environment_name: str | None) -> dict[str, Any]:
    try:
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"Bruin config not found: {path}")
    except yaml.YAMLError as exc:
        fail(f"Bruin config is not valid YAML: {exc}")
    if not isinstance(config, dict):
        fail("Bruin config must be a YAML mapping")
    environment_name = environment_name or str(config.get("default_environment", "default"))
    environments = config.get("environments")
    if isinstance(environments, dict) and isinstance(environments.get(environment_name), dict):
        return environments[environment_name]
    if environment_name == "default" and isinstance(config.get("connections"), dict):
        return config
    fail(f"environment {environment_name!r} was not found")


def expand(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty string")
    match = BRUIN_ENV.fullmatch(value)
    if match:
        name = match.group(1)
        if name not in os.environ:
            fail(f"environment variable {name!r} is required for {label}")
        return os.environ[name]

    def replace(token: re.Match[str]) -> str:
        name = token.group(1) or token.group(2)
        if name not in os.environ:
            fail(f"environment variable {name!r} is required for {label}")
        return os.environ[name]

    return ENV_TOKEN.sub(replace, value)


def fivetran_authorization(config_path: Path, environment_name: str | None) -> str:
    connections = load_config(config_path, environment_name).get("connections", {})
    generic = connections.get("generic", []) if isinstance(connections, dict) else []
    values: dict[str, Any] = {}
    if isinstance(generic, list):
        for entry in generic:
            if isinstance(entry, dict) and isinstance(entry.get("name"), str):
                values[entry["name"]] = entry.get("value")
    elif isinstance(generic, dict):
        for name, entry in generic.items():
            values[str(name)] = entry.get("value") if isinstance(entry, dict) else entry
    key = expand(values.get("fivetran_api_key"), "fivetran_api_key")
    secret = expand(values.get("fivetran_api_secret"), "fivetran_api_secret")
    token = base64.b64encode(f"{key}:{secret}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def get_json(authorization: str, endpoint: str, label: str, timeout: float, query: dict[str, str] | None = None) -> dict[str, Any]:
    """Fetch one Fivetran resource without placing identifiers in errors."""

    url = f"{API_BASE.rstrip('/')}/{endpoint.lstrip('/')}"
    if query:
        url = f"{url}?{parse.urlencode(query)}"
    for attempt in range(3):
        try:
            with request.urlopen(
                request.Request(
                    url,
                    headers={"Authorization": authorization, "Accept": "application/json;version=2"},
                    method="GET",
                ),
                timeout=timeout,
            ) as response:
                result = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code in {429, 500, 502, 503, 504} and attempt < 2:
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                delay = min(float(retry_after), 10.0) if retry_after and retry_after.isdigit() else attempt + 1
                time.sleep(delay)
                continue
            fail(f"Fivetran GET {label} failed with HTTP {exc.code}")
        except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            fail(f"Fivetran GET {label} failed: {type(exc).__name__}")
        if not isinstance(result, dict):
            fail(f"Fivetran GET {label} returned invalid JSON")
        return result
    fail(f"Fivetran GET {label} exhausted retry attempts")


def data_object(payload: dict[str, Any], endpoint: str) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        fail(f"Fivetran GET {endpoint} did not return an object")
    return data


def list_connections(authorization: str, timeout: float) -> list[dict[str, Any]]:
    connections: list[dict[str, Any]] = []
    cursor: str | None = None
    for _ in range(100):
        query = {"limit": "1000"}
        if cursor:
            query["cursor"] = cursor
        data = get_json(authorization, "connections", "connections list", timeout, query).get("data")
        if isinstance(data, dict):
            items, cursor = data.get("items", []), data.get("next_cursor")
        elif isinstance(data, list):
            items, cursor = data, None
        else:
            fail("Fivetran connections response did not contain a list")
        if not isinstance(items, list):
            fail("Fivetran connections response did not contain items")
        connections.extend(item for item in items if isinstance(item, dict))
        if not cursor:
            return connections
        if not isinstance(cursor, str):
            fail("Fivetran returned an invalid pagination cursor")
    fail("Fivetran pagination exceeded 100 pages")


def select_connection_by_name(connections: list[dict[str, Any]], name: str) -> dict[str, Any]:
    matches = [item for item in connections if item.get("name") == name]
    ids = {str(item.get("id")) for item in matches}
    if len(ids) != 1:
        fail(f"expected exactly one Fivetran connection name match, found {len(ids)}")
    return matches[0]


def redact(value: Any) -> Any:
    if isinstance(value, list):
        return [redact(item) for item in value]
    if not isinstance(value, dict):
        return value
    result: dict[str, Any] = {}
    for key, nested in value.items():
        key = str(key)
        normalized_key = re.sub(r"(?<!^)(?=[A-Z])", "_", key)
        if SECRET_FIELD.search(key) or NETWORK_FIELD.search(normalized_key):
            result[key] = "[REDACTED]"
        elif key == "config" and isinstance(nested, dict):
            result[key] = {"redacted": True, "field_names": sorted(map(str, nested))}
        else:
            result[key] = redact(nested)
    return result


def capture_directory(args: argparse.Namespace, connection: dict[str, Any]) -> Path:
    if args.output_dir:
        return require_artifact_path(Path(args.output_dir))
    name = str(connection.get("name") or connection.get("id") or "connection")
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip(".-").lower() or "connection"
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return require_artifact_path(ARTIFACT_ROOT / "fivetran" / f"{slug}-{stamp}")


def write_capture(destination: Path, connection: dict[str, Any], schemas: dict[str, Any], replace: bool) -> None:
    if destination.exists():
        if not replace:
            fail(f"capture directory exists: {destination}; use --replace to replace it")
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        for name, value in (("connection.json", redact(connection)), ("schemas.json", redact(schemas))):
            (temporary / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def run(args: argparse.Namespace) -> None:
    authorization = fivetran_authorization(Path(args.config_file), args.environment)
    if args.replace and not args.output_dir:
        fail("--replace requires an explicit capture directory")
    if args.connector_id:
        connection_id = args.connector_id
        selected: dict[str, Any] = {"id": connection_id}
    else:
        if not args.connector_name:
            fail("a Fivetran connector name or ID is required")
        selected = select_connection_by_name(list_connections(authorization, args.timeout), args.connector_name)
        connection_id = selected.get("id")
    if not isinstance(connection_id, str) or not connection_id:
        fail("selected connection has no ID")
    connection_path = parse.quote(connection_id, safe="")
    detail = data_object(get_json(authorization, f"connections/{connection_path}", "connection details", args.timeout), "connection details")
    schemas = data_object(get_json(authorization, f"connections/{connection_path}/schemas", "connection schemas", args.timeout), "connection schemas")
    connection = {**selected, **detail}
    destination = capture_directory(args, connection)
    write_capture(destination, connection, schemas, args.replace)
    print(f"Captured redacted Fivetran configuration to {destination}")


def parser() -> argparse.ArgumentParser:
    parsed = argparse.ArgumentParser(description=__doc__)
    parsed.add_argument("--config-file", required=True)
    parsed.add_argument("--environment")
    selected = parsed.add_mutually_exclusive_group(required=True)
    selected.add_argument("--connector-name", help="exact Fivetran connection name")
    selected.add_argument("--connector-id", help="exact Fivetran connection ID")
    parsed.add_argument("--timeout", type=float, default=20.0)
    parsed.add_argument("--output-dir", help="artifact directory for this capture")
    parsed.add_argument("--replace", action="store_true")
    return parsed


def main() -> int:
    try:
        run(parser().parse_args())
    except ImportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
