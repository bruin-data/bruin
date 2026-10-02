#!/usr/bin/env bash
# Check links in the documentation with lychee (https://lychee.cli.rs).
#
#   scripts/check_docs_links.sh             Internal pages and #anchors in the built site.
#                                           Offline; run `npm run docs:build` first.
#   scripts/check_docs_links.sh --external  External URLs in docs, template and root Markdown.
#
# Extra arguments are passed through to lychee.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v lychee >/dev/null 2>&1; then
  echo "lychee not found. Install it with 'brew install lychee' or see https://github.com/lycheeverse/lychee#installation" >&2
  exit 1
fi

if [ "${1:-}" = "--external" ]; then
  shift
  exec lychee --config lychee.toml --scheme https --scheme http --root-dir "$PWD/docs" "$@" '*.md' 'docs/**/*.md' 'templates/**/*.md'
fi

dist=docs/.vitepress/dist
if [ ! -d "$dist" ]; then
  echo "$dist not found. Run 'npm run docs:build' first." >&2
  exit 1
fi

# The site is served under /bruin/, so stage it there to resolve absolute links.
site=$(mktemp -d)
trap 'rm -rf "$site"' EXIT
cp -R "$dist" "$site/bruin"

lychee --config lychee.toml --offline --include-fragments \
  --fallback-extensions html --index-files index.html \
  --root-dir "$site" "$@" "$site/bruin/**/*.html"
