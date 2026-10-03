"""Shows how two versions of a golden file differ, e.g. to review a `go test -update` run:

    git show HEAD:pkg/sqlparser/testdata/golden/commands.json.gz > /tmp/old.json.gz
    python3 pkg/sqlparser/codegen/golden_diff.py /tmp/old.json.gz pkg/sqlparser/testdata/golden/commands.json.gz

usage: golden_diff.py <old.json.gz> <new.json.gz> [max examples]
"""

import collections
import gzip
import json
import sys


def load(path):
    with gzip.open(path, "rt") as f:
        return json.load(f)


old, new = load(sys.argv[1]), load(sys.argv[2])
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20
if [r["cmd"] for r in old] != [r["cmd"] for r in new]:
    print(
        "the command lists differ (different generator inputs); comparing by position anyway"
    )
by_cmd = collections.Counter()
shown = 0
for a, b in zip(old, new):
    if a["want"] == b["want"]:
        continue
    by_cmd[f"{a['cmd']['command']}/{a['cmd']['contents'].get('dialect')}"] += 1
    if shown < limit:
        shown += 1
        print(json.dumps(a["cmd"]))
        print("  old:", json.dumps(a["want"]))
        print("  new:", json.dumps(b["want"]))
print(f"{sum(by_cmd.values())}/{min(len(old), len(new))} responses changed")
for k, n in by_cmd.most_common():
    print(f"  {k}: {n}")
