#!/usr/bin/env bash
# Build the anonymised artefact for double-blind review.
#
#   scripts/make_artifact.sh OUT_DIR
#
# Exports HEAD (no .git, no results/, no CLAUDE.md), rewrites the Go module
# path to anonymous/wfsched, clears notebook outputs (they embed local
# paths), removes the README citation section and any
# README paragraph naming the authors, sets the LICENSE holder to "Anonymous
# Authors", builds and tests the result, and fails if any identifying string
# is still present. The identifying strings are derived at run time from
# go.mod, the README's "Citing this work" section, LICENSE and git config;
# none is written in this repository.
set -euo pipefail

OUT=${1:?usage: scripts/make_artifact.sh OUT_DIR}
ANON_MODULE="anonymous/wfsched"
REPO=$(git rev-parse --show-toplevel)
cd "$REPO"

if [ -e "$OUT" ] && [ -n "$(ls -A "$OUT" 2>/dev/null)" ]; then
  echo "error: $OUT exists and is not empty" >&2
  exit 1
fi
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
IDS=$(mktemp)
trap 'rm -f "$IDS"' EXIT

# ── Identifying strings (from the source repo, before anything is changed) ──
MODULE=$(awk '/^module /{print $2}' go.mod)
python3 - "$MODULE" > "$IDS" <<'PY'
import re, subprocess, sys
module = sys.argv[1]
ids = {module}
parts = module.split("/")
if len(parts) >= 2:
    ids.add(parts[1])                      # repository owner
# LICENSE copyright holder.
for line in open("LICENSE"):
    m = re.match(r"\s*Copyright\s*(?:\(c\)|©)?\s*[\d\-, ]*\s*(.+)", line, re.I)
    if m:
        holder = m.group(1).strip()
        ids.add(holder)
        ids.update(w for w in holder.split() if len(w) > 2)
# README "Citing this work": "> Surname, Given. *Title*. Kind, Institution, Year."
text = open("README.md").read()
m = re.search(r"^## Citing this work\n(.*?)(?=^## |\Z)", text, re.S | re.M)
if m:
    for cite in re.findall(r"^>\s*(.+)$", m.group(1), re.M):
        author, _, rest = cite.partition(". ")
        ids.add(author.strip())
        ids.update(w.strip(",") for w in author.split() if len(w.strip(",")) > 2)
        title = re.search(r"\*([^*]+)\*", rest)
        if title:
            ids.add(title.group(1).strip())
        fields = [f.strip() for f in re.sub(r"\*[^*]+\*\.?", "", rest).split(",")]
        ids.update(f.rstrip(".") for f in fields if len(f) > 3 and not f.rstrip(".").isdigit()
                   and "thesis" not in f.lower())
# git identity.
for key in ("user.name", "user.email"):
    v = subprocess.run(["git", "config", key], capture_output=True, text=True).stdout.strip()
    if v:
        ids.add(v)
        if "@" in v:
            ids.add(v.split("@")[0])
for s in sorted(i for i in ids if i):
    print(s)
PY
echo "identifying strings derived: $(wc -l < "$IDS")"

# ── Export ──────────────────────────────────────────────────────────────────
git archive --format=tar HEAD | tar -x -C "$OUT"
rm -rf "$OUT/CLAUDE.md" "$OUT/Claude.md" "$OUT/results"

# ── Module path ─────────────────────────────────────────────────────────────
grep -rlF --null "$MODULE" "$OUT" | xargs -0 -r sed -i "s#${MODULE}#${ANON_MODULE}#g"

# ── LICENSE holder ──────────────────────────────────────────────────────────
sed -i -E 's/^(Copyright (\(c\) )?[0-9, -]*).*$/\1 Anonymous Authors/' "$OUT/LICENSE"
sed -i -E 's/  +Anonymous Authors/ Anonymous Authors/' "$OUT/LICENSE"

# ── Notebooks: clear saved outputs (they embed local paths) ────────────────
python3 - "$OUT" <<'PY'
import json, os, sys
for d, _, files in os.walk(sys.argv[1]):
    for f in files:
        if f.endswith(".ipynb"):
            p = os.path.join(d, f)
            nb = json.load(open(p))
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    cell["outputs"], cell["execution_count"] = [], None
            json.dump(nb, open(p, "w"), indent=1)
            open(p, "a").write("\n")
PY

# ── README: drop the citation section and every paragraph naming the authors
python3 - "$OUT/README.md" "$IDS" <<'PY'
import re, sys
path, ids_path = sys.argv[1], sys.argv[2]
ids = [l.strip() for l in open(ids_path) if l.strip()]
text = open(path).read()
text = re.sub(r"^## Citing this work\n.*?(?=^## |\Z)", "", text, flags=re.S | re.M)
pats = [re.compile(r"(?<!\w)" + re.escape(i) + r"(?!\w)", re.I) for i in ids]
# Drop whole paragraphs, so no half-sentence of an attribution survives.
paras = re.split(r"(\n[ \t]*\n)", text)
kept = [p for p in paras if not any(pt.search(p) for pt in pats)]
open(path, "w").write(re.sub(r"\n{3,}", "\n\n", "".join(kept)))
PY

# ── Build and test the artefact ─────────────────────────────────────────────
(cd "$OUT" && GOFLAGS=-buildvcs=false go build ./... && GOFLAGS=-buildvcs=false go test ./... > /dev/null)
echo "artefact builds and tests pass"

# ── Final check: no identifying string anywhere (whole words, any case) ─────
python3 - "$OUT" "$IDS" <<'PY'
import os, re, sys
root, ids_path = sys.argv[1], sys.argv[2]
ids = [l.strip() for l in open(ids_path) if l.strip()]
pats = [re.compile(r"(?<!\w)" + re.escape(i) + r"(?!\w)", re.I) for i in ids]
hits = 0
for d, _, files in os.walk(root):
    for f in files:
        p = os.path.join(d, f)
        try:
            text = open(p, encoding="utf-8").read()
        except (UnicodeDecodeError, OSError):
            continue  # binary
        for n, line in enumerate(text.splitlines(), 1):
            if any(pt.search(line) for pt in pats):
                hits += 1
                # Report the location only; never echo the matched string.
                print(f"identifying string at {os.path.relpath(p, root)}:{n}", file=sys.stderr)
sys.exit(1 if hits else 0)
PY
echo "no identifying strings found; artefact at $OUT"
