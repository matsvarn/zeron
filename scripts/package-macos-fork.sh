#!/usr/bin/env bash
# Forked packaging: produces "Zeron Orchestrator.app" with bundle id
# sh.zeron.app.orchestrator so it can sit beside stock Zeron.app without the
# stock updater (or the Finder) ever confusing the two. The app still uses the
# stock data dir (~/.zeron), so existing chats carry over. ZERON_FORK_BUILD=1
# compiles out the whole updater (see zeron_update::fork_build), since a stock
# update would silently drop fork features.
#
# Usage: scripts/package-macos-fork.sh
# Output: target/package/Zeron Orchestrator.app and
#         target/package/zeron-orchestrator-<version>-macos-<arch>.dmg,
#         ad-hoc signed (no CODESIGN_IDENTITY needed).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec env \
  ZERON_APP_NAME="Zeron Orchestrator" \
  ZERON_BUNDLE_ID="sh.zeron.app.orchestrator" \
  ZERON_FORK_BUILD=1 \
  ZERON_FORK_SHA="$(git -C "$ROOT" rev-parse --short HEAD)" \
  "$ROOT/scripts/package-macos.sh"
