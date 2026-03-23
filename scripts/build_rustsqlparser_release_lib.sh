#!/usr/bin/env bash

set -euo pipefail

target="${1:?missing target triple}"
lockdir="${HOME}/.cache/bruin-rustsqlparser-release.lock"

mkdir -p "$(dirname "${lockdir}")"

# Serialize Rust toolchain setup and static library compilation across parallel build hooks.
while ! mkdir "${lockdir}" 2>/dev/null; do
	sleep 1
done

cleanup() {
	rmdir "${lockdir}"
}

trap cleanup EXIT

if ! command -v rustup >/dev/null 2>&1; then
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi

# shellcheck source=/dev/null
. "${HOME}/.cargo/env"

rustup target add "${target}"
cargo build --release --manifest-path pkg/sqlparser/rustffi/Cargo.toml --target "${target}"
