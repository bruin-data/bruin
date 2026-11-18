"""Pin a dac release for `bruin dac`: its version and the SHA-256 of each archive.

Usage: python3 scripts/update_dac_pins.py 0.33.0   (or: make dac-pins DAC_VERSION=0.33.0)

Downloads every release archive, checks it against the release's checksums.txt,
and rewrites pkg/dac/release.json. Bruin verifies downloads against these
committed hashes, so review the diff like any other dependency bump.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "pkg/dac/release.json"
DOWNLOAD_URL = "https://github.com/bruin-data/dac/releases/download"
ARCHIVES = (
    "dac_Darwin_arm64.tar.gz",
    "dac_Darwin_x86_64.tar.gz",
    "dac_Linux_arm64.tar.gz",
    "dac_Linux_x86_64.tar.gz",
    "dac_Windows_x86_64.zip",
)


def fetch(url):
    with urllib.request.urlopen(url, timeout=300) as response:
        return response.read()


def parse_checksums(text):
    checksums = {}
    for line in text.splitlines():
        if line.strip():
            digest, name = line.split()
            checksums[name.lstrip("*")] = digest
    return checksums


def generate(version, fetch=fetch):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError(f"expected a stable dac version such as 0.33.0, got {version!r}")
    base = f"{DOWNLOAD_URL}/v{version}"
    published = parse_checksums(fetch(f"{base}/checksums.txt").decode())
    archives = {}
    for name in ARCHIVES:
        digest = hashlib.sha256(fetch(f"{base}/{name}")).hexdigest()
        if published.get(name) != digest:
            raise ValueError(f"{name} does not match the release's checksums.txt")
        archives[name] = digest
    return json.dumps({"archives": archives, "version": version}, indent=2, sort_keys=True) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("version", help="dac release version without the leading v, e.g. 0.33.0")
    args = parser.parse_args()
    generated = generate(args.version)
    # Write in one step so a failed run never leaves partial pins behind.
    temporary = DESTINATION.with_suffix(".json.tmp")
    temporary.write_text(generated)
    temporary.replace(DESTINATION)
    print(f"Pinned dac v{args.version} ({len(ARCHIVES)} archives) in {DESTINATION.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
