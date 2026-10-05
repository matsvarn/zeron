#!/usr/bin/env python3
"""For every commit in BASE..HEAD, report files whose diff contains
whitespace-only line changes (lines that change under `git diff` but not
under `git diff -w`).

usage: series-ws.py [BASE] [HEAD]   (run from the repo root)
Exit status 1 when any commit carries whitespace-only changes.
"""
import subprocess
import sys

base = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
head = sys.argv[2] if len(sys.argv) > 2 else "HEAD"


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def changed(*flags, rng, path):
    out = git("diff", "--numstat", *flags, *rng, "--", path).split()
    return int(out[0]) + int(out[1]) if out and out[0] != "-" else 0


bad = 0
for commit in git("rev-list", "--reverse", f"{base}..{head}").split():
    rng = (f"{commit}~1", commit)
    for path in git("diff", "--name-only", *rng).split():
        full, real = changed(rng=rng, path=path), changed("-w", rng=rng, path=path)
        if full > real:
            bad += 1
            print(f"{commit[:8]} {path}: {full} changed lines, {real} without whitespace")

print(f"{bad} file(s) with whitespace-only changes")
sys.exit(1 if bad else 0)
