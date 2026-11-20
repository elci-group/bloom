#!/usr/bin/env bash
# Full verification gate used by kaptaind and CI: format, lint, tests.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
