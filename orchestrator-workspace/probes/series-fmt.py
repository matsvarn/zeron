#!/usr/bin/env python3
"""For every commit in BASE..HEAD, rustfmt-check each .rs file the commit
touches and report formatting hunks that the series introduced: a file's
hunk count at the commit must not exceed its count on BASE (0 for new files).

usage: series-fmt.py [BASE] [HEAD]   (run from the repo root)
Exit status 1 when any commit adds rustfmt hunks.
"""
import subprocess
import sys

base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
head = sys.argv[2] if len(sys.argv) > 2 else "HEAD"


def git(*args, check=True):
    return subprocess.run(["git", *args], check=check, capture_output=True, text=True)


def hunks(rev, path):
    shown = git("show", f"{rev}:{path}", check=False)
    if shown.returncode != 0:
        return 0
    out = subprocess.run(
        ["rustfmt", "--check", "--edition", "2024", "--color", "never"],
        input=shown.stdout, capture_output=True, text=True,
    )
    return sum(1 for line in out.stdout.splitlines() if line.startswith("Diff in "))


bad = 0
for commit in git("rev-list", "--reverse", f"{base}..{head}").stdout.split():
    files = git("diff", "--name-only", "--diff-filter=AM", f"{commit}~1", commit).stdout.split()
    for path in (f for f in files if f.endswith(".rs")):
        now, before = hunks(commit, path), hunks(base, path)
        if now > before:
            bad += 1
            print(f"{commit[:8]} {path}: {now} rustfmt hunk(s), {before} on {base}")

print(f"{bad} file(s) with new rustfmt hunks")
sys.exit(1 if bad else 0)
