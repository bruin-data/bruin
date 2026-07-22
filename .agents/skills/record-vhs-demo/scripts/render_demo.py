#!/usr/bin/env python3
"""Validate, render, probe, and create a contact sheet for a VHS demo tape."""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


MEDIA_SUFFIXES = {".gif", ".mp4", ".webm"}


def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(1)


def run(command: list[str], *, cwd: Path) -> None:
    print("+ " + shlex.join(command))
    subprocess.run(command, cwd=cwd, check=True)


def run_captured(command: list[str], *, cwd: Path) -> str:
    print("+ " + shlex.join(command))
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        fail(f"command exited with status {result.returncode}: {shlex.join(command)}")
    return result.stdout + result.stderr


def repository_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(result.stdout.strip()).resolve()


def tape_outputs(tape: Path) -> list[str]:
    outputs: list[str] = []
    for line_number, raw_line in enumerate(tape.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            fail(f"cannot parse {tape}:{line_number}: {exc}")
        if parts and parts[0] == "Output":
            if len(parts) != 2:
                fail(f"expected one Output path at {tape}:{line_number}")
            outputs.append(parts[1])
    if not outputs:
        fail(f"{tape} has no Output directive")
    return outputs


def require_tools() -> None:
    missing = [
        name for name in ("vhs", "ffmpeg", "ffprobe") if shutil.which(name) is None
    ]
    if missing:
        fail("missing required tools: " + ", ".join(missing))


def require_ignored_context(repo_root: Path) -> None:
    probe = ".context/vhs-demos/.ignore-probe"
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--", probe],
        cwd=repo_root,
    )
    if result.returncode != 0:
        fail(
            ".context/ is not ignored; add '.context/' to the repository's "
            "local .git/info/exclude before rendering"
        )


def probe(media: Path) -> tuple[float, int]:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration,size",
            "-of",
            "json",
            str(media),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)["format"]
    duration = float(data["duration"])
    size = int(data["size"])
    if duration <= 0 or size <= 0:
        fail(f"invalid rendered artifact: {media}")
    return duration, size


def contact_sheet(media: Path, destination: Path, duration: float) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    frames_per_second = 6 / duration
    run(
        [
            "ffmpeg",
            "-y",
            "-v",
            "error",
            "-i",
            str(media),
            "-vf",
            f"fps={frames_per_second:.8f},scale=640:-1,tile=3x2",
            "-frames:v",
            "1",
            str(destination),
        ],
        cwd=media.parent,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tape", type=Path, help="tape stored under the repository resources directory"
    )
    args = parser.parse_args()

    require_tools()
    repo_root = repository_root()
    require_ignored_context(repo_root)
    resources_root = (repo_root / "resources").resolve()
    tape = args.tape.resolve()

    try:
        tape.relative_to(resources_root)
    except ValueError:
        fail(f"canonical tapes must live under {resources_root}")
    if tape.suffix != ".tape" or not tape.is_file():
        fail(f"not a VHS tape file: {tape}")

    output_values = tape_outputs(tape)
    output_paths = [(tape.parent / value).resolve() for value in output_values]
    for output in output_paths:
        output.parent.mkdir(parents=True, exist_ok=True)

    run(["vhs", "validate", tape.name], cwd=tape.parent)
    render_log = run_captured(["vhs", tape.name], cwd=tape.parent)
    if any(line.lower().startswith("error:") for line in render_log.splitlines()):
        fail("VHS reported an internal error and may have produced a partial artifact")

    media_paths = [
        path for path in output_paths if path.suffix.lower() in MEDIA_SUFFIXES
    ]
    if not media_paths:
        fail("the tape has no GIF, MP4, or WebM output to inspect")

    qa_root = repo_root / ".context" / "vhs-demos" / "qa"
    for media in media_paths:
        if not media.is_file():
            fail(f"VHS did not create expected output: {media}")
        duration, size = probe(media)
        qa_path = qa_root / f"{tape.stem}-{media.stem}-contact-sheet.png"
        contact_sheet(media, qa_path, duration)
        print(f"artifact: {media}")
        print(f"duration: {duration:.2f}s")
        print(f"size: {size} bytes")
        print(f"qa: {qa_path}")


if __name__ == "__main__":
    main()
