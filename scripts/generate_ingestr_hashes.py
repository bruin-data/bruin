"""Generate embedded hashes using GitHub's authenticated immutable-release verifier.

Requires a trusted installation of gh with `release verify` / `verify-asset`.
Never installs tools or falls back to unsigned metadata. Build commands and
release CI generate the ignored manifest before compiling Bruin.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
REPO = "bruin-data/ingestr"
ARCHIVES = (
    "ingestr_Darwin_arm64.tar.gz",
    "ingestr_Darwin_x86_64.tar.gz",
    "ingestr_Linux_arm64.tar.gz",
    "ingestr_Linux_x86_64.tar.gz",
    "ingestr_Windows_x86_64.zip",
)


def gh(*args):
    # Pin the host too: GH_HOST must not redirect verification to another forge.
    env = dict(os.environ, GH_HOST="github.com", GH_PROMPT_DISABLED="1")
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True, env=env
    ).stdout


def installer_hash(tag, verification):
    # Only consume the statement returned by successful official verification,
    # never the unverified bundle payload or release API target_commitish.
    statement = verification["verificationResult"]["statement"]
    purl = f"pkg:github/{REPO}@{tag}"
    predicate = statement["predicate"]
    if (
        statement["predicateType"] != "https://in-toto.io/attestation/release/v0.2"
        or predicate["repository"] != REPO
        or predicate["tag"] != tag
        or predicate["purl"] != purl
    ):
        raise ValueError("verified installer source has wrong repository or tag")
    subjects = [item for item in statement["subject"] if item.get("uri") == purl]
    if len(subjects) != 1:
        raise ValueError("expected one verified installer source commit")
    commit = subjects[0]["digest"]["sha1"]
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("invalid verified installer source commit")

    # Git validates its content-addressed objects; tie the fetched tree to the
    # signed commit before reading the script. Never execute fetched source.
    with tempfile.TemporaryDirectory(prefix="bruin-ingestr-source-") as directory:

        def git(*args):
            return subprocess.run(
                ["git", "-C", directory, "-c", "transfer.fsckObjects=true", *args],
                check=True,
                capture_output=True,
                env=dict(os.environ, GIT_TERMINAL_PROMPT="0"),
            ).stdout

        git("init", "--bare", "--quiet")
        git(
            "fetch",
            "--quiet",
            "--depth=1",
            f"https://github.com/{REPO}.git",
            f"refs/tags/{tag}",
        )
        if git("rev-parse", "FETCH_HEAD^{commit}").decode().strip() != commit:
            raise ValueError("installer source does not match verified release commit")
        script = git("show", f"{commit}:install.sh")
    return commit, hashlib.sha256(script).hexdigest()


def generate(version):
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("expected an exact stable ingestr version")
    tag = "v" + version
    release = json.loads(gh("api", f"repos/{REPO}/releases/tags/{tag}"))
    if release.get("tag_name") != tag or release.get("immutable") is not True:
        raise ValueError("expected the exact immutable ingestr release")
    verification = json.loads(
        gh("release", "verify", tag, "--repo", REPO, "--format", "json")
    )
    commit, script_hash = installer_hash(tag, verification)
    hashes = {}
    with tempfile.TemporaryDirectory(prefix="bruin-ingestr-verify-") as directory:
        for name in ARCHIVES:
            gh(
                "release",
                "download",
                tag,
                "--repo",
                REPO,
                "--pattern",
                name,
                "--dir",
                directory,
            )
            archive = Path(directory) / name
            gh("release", "verify-asset", tag, str(archive), "--repo", REPO)
            with archive.open("rb") as source:
                hashes[name] = hashlib.file_digest(source, "sha256").hexdigest()
            archive.unlink()
    return (
        json.dumps(
            {
                version: {
                    "archives": hashes,
                    "installer_commit": commit,
                    "installer_sha256": script_hash,
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    source = (ROOT / "pkg/python/uv.go").read_text()
    version = re.search(r'IngestrVersionV1\s*=\s*"([^"]+)"', source).group(1)
    generated = generate(version)
    destination = ROOT / "pkg/python/ingestr_hashes.json"
    if args.check:
        if destination.read_text() != generated:
            raise ValueError(
                "generated ingestr hashes differ from verified release; run generator"
            )
    else:
        # Do not leave partial trusted output after a failed verification.
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_text(generated)
        temporary.replace(destination)
    print(
        f"Verified ingestr v{version} installer source and all {len(ARCHIVES)} archives; generated embedded hashes"
    )


if __name__ == "__main__":
    main()
