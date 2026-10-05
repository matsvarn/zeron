#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$repo_root"

for tool in cargo rustc node npm; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Missing $tool. Install the host tools described in CONTRIBUTORS.md, then run setup again." >&2
    exit 1
  fi
done

cargo fetch --locked
npm ci --prefix edge --no-audit --no-fund
WRANGLER_SEND_METRICS=false npm run build --prefix edge

echo 'Locked Rust dependencies and the local edge build are ready.'
echo 'Rust binaries compile on first cargo build/run. Native host libraries are listed in CONTRIBUTORS.md.'
