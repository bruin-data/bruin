"""Give every tracked file a modification time derived from its content.

Go's test cache keys on the size and modification time of each file a test
opens inside the module, not on the file's content. A fresh CI checkout gives
every file a new modification time, so every test that reads a fixture misses
the cache even when nothing changed. Setting each file's time from a hash of
its bytes makes the time stable across checkouts and still changes it whenever
the content changes. Directories, whose listings tests also read, and the
.git directory, which tests stat to find the repository root, get one fixed
time.

Run from the repository root after checkout and after any step that writes
tracked files, and before `go test`.
"""

import hashlib
import os
import subprocess

# 2001-09-09T01:46:40Z, well past Go's 2-second "file too new" cutoff.
BASE_NS = 1_000_000_000 * 10**9
# 48 bits of the hash in 100 ns steps, the finest precision NTFS keeps,
# spread over about 325 days.
STEP_NS = 100


def main() -> None:
    paths = (
        subprocess.run(["git", "ls-files", "-z"], check=True, capture_output=True)
        .stdout.decode()
        .split("\0")
    )

    dirs = set()
    for path in filter(None, paths):
        if os.path.islink(path) or not os.path.isfile(path):
            continue
        with open(path, "rb") as f:
            digest = hashlib.sha256(f.read()).digest()
        ns = BASE_NS + int.from_bytes(digest[:6], "big") * STEP_NS
        os.utime(path, ns=(ns, ns))
        parent = os.path.dirname(path)
        while parent and parent not in dirs:
            dirs.add(parent)
            parent = os.path.dirname(parent)

    # The new times make git's index stale, and the first git command to notice
    # would rewrite it and so change the time of .git, which tests stat to find
    # the repository root. Refresh the index now, then fix the time of .git.
    subprocess.run(["git", "update-index", "-q", "--refresh"], check=False)
    for d in dirs | {".", ".git"}:
        os.utime(d, ns=(BASE_NS, BASE_NS))


if __name__ == "__main__":
    main()
