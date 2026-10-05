#!/usr/bin/env bash
# Install the forked "Zeron Orchestrator.app" next to stock Zeron.app.
#
# It uses the same data directory as stock Zeron (~/.zeron), so your chats,
# projects and settings carry over. Only one of the two apps can run at a time
# (the engine takes a lock on ~/.zeron).
#
# What this does:
#   1. Refuses to run while stock Zeron (or any Zeron engine on ~/.zeron) is running.
#   2. Copies ~/.zeron to ~/.zeron.backup-<timestamp> (nothing is deleted).
#   3. Copies the built bundle to /Applications/Zeron Orchestrator.app
#      (replacing an older copy of the fork, never touching Zeron.app).
#   4. Opens it.
#
# To go back: quit Zeron Orchestrator, open Zeron.app. Note that stock Zeron
# auto-updates when opened; after it has updated, don't switch back to the
# fork until the fork is rebased onto that version.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/comet/target/package/Zeron Orchestrator.app"
DEST="/Applications/Zeron Orchestrator.app"
DATA="$HOME/.zeron"

[[ -d "$SRC" ]] || { echo "No built bundle at $SRC. Run comet/scripts/package-macos-fork.sh first." >&2; exit 1; }

if pgrep -f '/Applications/Zeron.app/Contents/MacOS/zeron' >/dev/null || pgrep -f "$DEST/Contents/MacOS/zeron" >/dev/null; then
  echo "Zeron is still running. Quit it (Cmd-Q) and run this again." >&2
  exit 1
fi
if lsof -t "$DATA/engine.lock" >/dev/null 2>&1; then
  echo "Some process still holds $DATA/engine.lock:" >&2
  lsof "$DATA/engine.lock" >&2 || true
  exit 1
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
if [[ -d "$DATA" ]]; then
  echo "Backing up $DATA -> $DATA.backup-$STAMP"
  cp -Rp "$DATA" "$DATA.backup-$STAMP"
fi

echo "Installing $DEST"
if [[ -d "$DEST" ]]; then
  mv "$DEST" "/tmp/Zeron Orchestrator.app.previous-$STAMP"
  echo "  (previous fork build moved to /tmp/Zeron Orchestrator.app.previous-$STAMP)"
fi
ditto "$SRC" "$DEST"
codesign -v "$DEST"

echo "Opening $DEST"
open "$DEST"
