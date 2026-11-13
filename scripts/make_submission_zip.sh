#!/usr/bin/env bash
# Package the submission ZIP: pure code, no execution-based files.
#   bash scripts/make_submission_zip.sh [TeamName]      -> OPS_<TeamName>_PROJECT_Finalterm_code.zip
# Uses `git archive`, so only committed files go in: no .venv, databases, __pycache__, results/,
# run logs or bundles (all git-ignored). The instructor's handbook PDF is added if it is present.
set -euo pipefail
cd "$(dirname "$0")/.."
TEAM="${1:-TeamA}"
OUT="OPS_${TEAM}_PROJECT_Finalterm_code.zip"
PREFIX="songdo-edge-intelligence/"
git diff --quiet || echo "warning: uncommitted changes are NOT in the zip (commit them first)"
git archive --format=zip --prefix="$PREFIX" -o "$OUT" HEAD
if [ -f Songdo_Edge_Intelligence_Handbook.pdf ] && command -v zip >/dev/null; then
  mkdir -p /tmp/songdo_zip/"$PREFIX" && cp Songdo_Edge_Intelligence_Handbook.pdf /tmp/songdo_zip/"$PREFIX"
  (cd /tmp/songdo_zip && zip -q "$OLDPWD/$OUT" "${PREFIX}Songdo_Edge_Intelligence_Handbook.pdf") && rm -rf /tmp/songdo_zip
fi
# refuse execution artefacts and external links
BAD=$(python3 - "$OUT" <<'PY'
import re, sys, zipfile
z = zipfile.ZipFile(sys.argv[1]); bad = []
for n in z.namelist():
    if re.search(r"(\.db(-wal|-shm)?|\.pyc|\.log|\.zip|/\.venv/|/__pycache__/|/results/)$|/results/", n):
        bad.append("execution file: " + n)
    elif n.endswith((".py", ".md", ".sh", ".yaml", ".html", ".txt", ".csv", ".sql")):
        text = z.read(n).decode("utf-8", "replace")
        for m in re.findall(r"https?://(?!127\.0\.0\.1|localhost|%s|\{)[^\s\"')]+", text):
            bad.append(f"external link in {n}: {m}")
print("\n".join(bad))
PY
)
if [ -n "$BAD" ]; then echo "$BAD"; echo "fix the above before submitting"; exit 1; fi
echo "$OUT: $(du -h "$OUT" | cut -f1), $(unzip -l "$OUT" | tail -1 | awk '{print $2}') files, no execution files, no external links"
