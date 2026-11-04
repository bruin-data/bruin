"""TEMPORARY: print the inputs Go's test cache records for one package.

Usage: python3 scripts/debug_testlog.py <testlogfile>
Env values are printed as hashes only.
"""

import hashlib
import os
import sys

root = os.getcwd()
pwd = None
seen = set()
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    line = line.rstrip("\n")
    if line.startswith("#") or " " not in line:
        continue
    op, name = line.split(" ", 1)
    if op == "chdir":
        pwd = name
    if (op, name) in seen:
        continue
    seen.add((op, name))
    if op == "getenv":
        v = os.environ.get(name)
        h = "unset" if v is None else hashlib.sha256(v.encode()).hexdigest()[:12]
        print(f"getenv {name} {h}")
        continue
    path = name if os.path.isabs(name) else os.path.join(pwd or sys.argv[2], name)
    if not path.startswith(root):
        continue
    try:
        st = os.stat(path)
        info = f"size={st.st_size} mode={oct(st.st_mode)} mtime_ns={st.st_mtime_ns}"
        if os.path.isdir(path) and op == "open":
            for e in sorted(os.listdir(path)):
                s = os.stat(os.path.join(path, e), follow_symlinks=False)
                info += f"\n    {e} size={s.st_size} mtime_ns={s.st_mtime_ns}"
    except OSError as e:
        info = f"err {e}"
    print(f"{op} {os.path.relpath(path, root)} {info}")
