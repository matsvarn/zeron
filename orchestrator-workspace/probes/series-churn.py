#!/usr/bin/env python3
"""Report lines that a commit in BASE..HEAD removes or rewrites which were
introduced by an earlier commit of the same series.

usage: series-churn.py [BASE] [HEAD]   (defaults: origin/main HEAD)
Exit status 1 when any such line exists.
"""
import re
import subprocess
import sys

base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
head = sys.argv[2] if len(sys.argv) > 2 else "HEAD"


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


series = git("rev-list", "--reverse", f"{base}..{head}").split()
in_series = set(series)
hunk = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+")
violations = 0

for commit in series:
    parent = f"{commit}~1"
    diff = git("diff", "-U0", "--no-renames", parent, commit)
    path = None
    for line in diff.splitlines():
        if line.startswith("--- "):
            path = None if line == "--- /dev/null" else line[6:]
            continue
        m = hunk.match(line)
        if not m or path is None:
            continue
        start, count = int(m.group(1)), int(m.group(2) or "1")
        if count == 0:
            continue
        blame = git("blame", "-l", "-s", "-L", f"{start},{start + count - 1}", parent, "--", path)
        for b in blame.splitlines():
            sha = b.split()[0].lstrip("^")
            if sha in in_series:
                violations += 1
                print(f"{commit[:8]} rewrites {sha[:8]}'s line in {path}: {b[len(b.split()[0]):].strip()[:120]}")

print(f"{violations} rewritten line(s) across {len(series)} commits")
sys.exit(1 if violations else 0)
