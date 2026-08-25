#!/usr/bin/env python3
"""Generate docs pages from template READMEs.

A template README and its docs page carry the same content, so the docs copy is
generated rather than hand-maintained. Only the templates listed in SYNCED are
generated; every other docs page under templates-docs/ is written by hand and is
left alone.

The two copies differ in exactly one way: the template README resolves images
against its own `images/` folder, while VitePress serves them from `public/` at
the site root. Image files are copied across as part of the sync.

  make sync-template-docs    regenerate
  make test                  fails if a generated page has drifted
"""

from __future__ import annotations

import argparse
import filecmp
import pathlib
import re
import shutil
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES = REPO / "templates"
DOCS = REPO / "docs" / "getting-started" / "templates-docs"
PUBLIC = REPO / "docs" / "public"

# Templates whose docs page is generated from the template README.
SYNCED = (
    "posthog-bigquery",
    "stripe-bigquery",
)

HEADER = (
    "<!-- Generated from templates/{name}/README.md. Do not edit directly; "
    "run `make sync-template-docs`. -->\n"
)

IMAGE_REF = re.compile(r"\]\(images/([^)]+)\)")


def render(name: str) -> str:
    """Return the docs-page content for a template's README."""
    readme = (TEMPLATES / name / "README.md").read_text()
    return HEADER.format(name=name) + IMAGE_REF.sub(r"](/\1)", readme)


def sync_images(name: str) -> list[str]:
    """Copy a template's images into docs/public/, returning the names copied."""
    source = TEMPLATES / name / "images"
    if not source.is_dir():
        return []
    copied = []
    for image in sorted(source.iterdir()):
        if image.is_file() and not image.name.startswith("."):
            target = PUBLIC / image.name
            if not (target.exists() and filecmp.cmp(image, target, shallow=False)):
                shutil.copyfile(image, target)
                copied.append(image.name)
    return copied


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report drift without writing anything",
    )
    args = parser.parse_args()

    drifted = []
    for name in SYNCED:
        page = DOCS / f"{name}-README.md"
        want = render(name)
        if args.check:
            if not page.exists() or page.read_text() != want:
                drifted.append(str(page.relative_to(REPO)))
            continue
        if not page.exists() or page.read_text() != want:
            page.write_text(want)
            print(f"wrote {page.relative_to(REPO)}")
        for image in sync_images(name):
            print(f"wrote {(PUBLIC / image).relative_to(REPO)}")

    if drifted:
        print("template docs are out of date:", file=sys.stderr)
        for page in drifted:
            print(f"  {page}", file=sys.stderr)
        print("run `make sync-template-docs`", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
