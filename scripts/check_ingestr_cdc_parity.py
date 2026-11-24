"""Require an explicit Bruin mapping when ingestr's PostgreSQL CDC interface changes.

Reads tagged Go source without building or executing upstream code. The manifest
also supplies Bruin's runtime allowlist for CDC asset parameters. To audit a local
checkout, pass --upstream-dir; CI fetches the candidate tag through GitHub's API.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "pkg/python/ingestr_cdc_capabilities.json"
SOURCE_PATHS = (
    "cmd/ingest.go",
    "pkg/source/postgres_cdc/source.go",
    "internal/config/config.go",
    "internal/config/cdc_spool.go",
    "pkg/source/postgres/snapshot.go",
)
GO_TOKENS = re.compile(r'"(?:\\.|[^"\\])*"|`[^`]*`|//[^\n]*|/\*.*?\*/|[{}]', re.S)


def body_at(source, opening):
    """Extract a Go body without counting braces in comments or string literals."""
    depth = 0
    for token in GO_TOKENS.finditer(source, opening):
        if token.group() == "{":
            depth += 1
        elif token.group() == "}":
            depth -= 1
            if depth == 0:
                return source[opening + 1 : token.start()]
    raise ValueError("cannot read upstream Go body; review the parity parser")


def parse_cli(source):
    flags = {}
    for declaration in re.finditer(r"&cli\.(\w+Flag)\s*\{", source):
        body = body_at(source, declaration.end() - 1)
        name = re.search(r'\bName:\s*"([^"\n]+)"', body)
        if name is None:
            raise ValueError(
                "upstream flag has no literal name; review the parity parser"
            )
        name = name.group(1)
        if name in flags:
            raise ValueError(f"duplicate upstream flag: {name}")
        fields = {"type": declaration.group(1), "default": None}
        for field, target in (("Value", "default"), ("DefaultText", "default_text")):
            value = re.search(rf"^\s*{field}:\s*(.*),\s*$", body, re.M)
            if value:
                fields[target] = " ".join(value.group(1).split())
        flags[name] = fields
    if not any(name.startswith("cdc-") for name in flags):
        raise ValueError("no upstream CDC flags found; review the parity parser")
    return flags


def parse_uri(source, function="parseURIConfig", variable="query"):
    declaration = re.search(rf"func {function}\([^\n]+\)\s*\{{", source)
    if declaration is None:
        raise ValueError(f"PostgreSQL {function} moved; review the parity parser")
    body = body_at(source, declaration.end() - 1)
    keys = set(re.findall(rf'\b{variable}\.(?:Get|Del|Has)\("([^"\n]+)"\)', body))
    if not keys:
        raise ValueError(
            "no PostgreSQL CDC URI options found; review the parity parser"
        )
    return keys


def constant(source, name):
    value = re.search(
        rf"^\s*(?:const\s+)?{re.escape(name)}(?:\s+\w+)?\s*=\s*(.+?)(?:\s*//.*)?$",
        source,
        re.M,
    )
    if value is None:
        raise ValueError(f"upstream constant {name} moved; review its default")
    return " ".join(value.group(1).split())


def function_digest(source, name):
    declaration = re.search(
        rf"func\s+(?:\([^\n]*\)\s+)?{re.escape(name)}\([^\n]*\)\s*[^\n{{]*\{{", source
    )
    if declaration is None:
        raise ValueError(
            f"upstream function {name} moved; review its validation contract"
        )
    body = body_at(source, declaration.end() - 1)
    body = GO_TOKENS.sub(
        lambda token: "" if token.group().startswith(("//", "/*")) else token.group(),
        body,
    )
    return hashlib.sha256(" ".join(body.split()).encode()).hexdigest()


def check(manifest, sources):
    issues = []
    known = manifest["known_cdc_parameters"]
    if len(known) != len(set(known)) or any(
        not name.startswith("cdc_") for name in known
    ):
        issues.append("known_cdc_parameters must contain unique cdc_* names")
    flags = parse_cli(sources[SOURCE_PATHS[0]])
    expected = manifest["cli"]
    missing = {name for name in flags if name.startswith("cdc-")} - expected.keys()
    for name in sorted(missing):
        issues.append(f"new upstream flag --{name}: add a supported/deferred mapping")
    for name, entry in expected.items():
        if name not in flags:
            issues.append(f"upstream flag --{name} removed or renamed")
        else:
            signature = {
                key: entry[key]
                for key in ("type", "default", "default_text")
                if key in entry
            }
            if flags[name] != signature:
                issues.append(f"upstream --{name} type/default changed: {flags[name]}")
        validate_entry(name, entry, known, issues)
    for section, actual_uri, label in (
        ("uri", parse_uri(sources[SOURCE_PATHS[1]]), "PostgreSQL CDC"),
        (
            "postgres_uri",
            parse_uri(sources[SOURCE_PATHS[4]], "parseSnapshotURI", "params"),
            "PostgreSQL snapshot",
        ),
    ):
        for name in sorted(actual_uri - manifest[section].keys()):
            issues.append(
                f"new {label} URI option {name}: add a supported/deferred mapping"
            )
        for name, entry in manifest[section].items():
            if name not in actual_uri:
                issues.append(f"{label} URI option {name} removed or renamed")
            validate_entry(name, entry, known, issues)
    for path, values in manifest["constants"].items():
        for name, expected_value in values.items():
            actual = constant(sources[path], name)
            if actual != expected_value:
                issues.append(f"upstream {name} changed: {expected_value} -> {actual}")
    for path, functions in manifest["reviewed_functions"].items():
        for name, expected_digest in functions.items():
            if function_digest(sources[path], name) != expected_digest:
                issues.append(
                    f"upstream {name} changed: review accepted values, defaults and restrictions before updating its digest"
                )
    return issues


def validate_entry(name, entry, known, issues):
    status = entry.get("status")
    if status == "supported":
        parameter = entry.get("parameter")
        if not parameter:
            issues.append(f"{name}: supported options need a Bruin parameter mapping")
        elif parameter.startswith("cdc_") and parameter not in known:
            issues.append(
                f"{name}: {parameter} is missing from the runtime CDC allowlist"
            )
    elif status not in ("deferred", "excluded") or not entry.get("reason"):
        issues.append(f"{name}: requires supported mapping or deferred/excluded reason")


def fetch_source(version, path):
    result = subprocess.run(
        [
            "gh",
            "api",
            "--hostname",
            "github.com",
            "--method",
            "GET",
            f"repos/bruin-data/ingestr/contents/{path}",
            "-f",
            f"ref=v{version}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    if payload.get("encoding") != "base64":
        raise ValueError(f"unexpected GitHub content encoding for {path}")
    return base64.b64decode(payload["content"]).decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--version", help="candidate stable ingestr version (defaults to Bruin's pin)"
    )
    parser.add_argument(
        "--upstream-dir", type=Path, help="read source from a local candidate checkout"
    )
    args = parser.parse_args()
    version = args.version
    if version is None:
        match = re.search(
            r'IngestrVersionV1\s*=\s*"([^"]+)"', (ROOT / "pkg/python/uv.go").read_text()
        )
        if match is None:
            raise ValueError("cannot find Bruin's pinned ingestr version")
        version = match.group(1)
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("expected an exact stable ingestr version such as 1.1.87")
    sources = {
        path: (args.upstream_dir / path).read_text()
        if args.upstream_dir
        else fetch_source(version, path)
        for path in SOURCE_PATHS
    }
    manifest = json.loads(MANIFEST.read_text())
    issues = check(manifest, sources)
    if issues:
        raise SystemExit(
            "PostgreSQL CDC parity needs review:\n- " + "\n- ".join(issues)
        )
    print(
        f"ingestr {version}: PostgreSQL CDC interface matches the reviewed Bruin mappings"
    )


if __name__ == "__main__":
    main()
