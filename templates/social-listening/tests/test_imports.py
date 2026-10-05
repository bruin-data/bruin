"""The tested library must stay standard-library only."""

from __future__ import annotations

import importlib
import subprocess
import sys
import unittest

from .helpers import SCRIPTS, TEMPLATE_ROOT

MODULES = [
    "social_listening",
    "social_listening.settings",
    "social_listening.http",
    "social_listening.collect",
    "social_listening.llm",
    "social_listening.delivery",
    "social_listening.sources.base",
    "social_listening.sources.reddit",
    "social_listening.sources.hackernews",
    "social_listening.sources.github",
    "social_listening.sources.stackexchange",
    "social_listening.sources.exports",
    "social_listening.sources.slack_community",
]


class ImportTest(unittest.TestCase):
    def test_modules_import(self):
        for name in MODULES:
            with self.subTest(module=name):
                importlib.import_module(name)

    def test_no_third_party_imports(self):
        # A fresh interpreter, so modules imported by other tests do not hide a regression.
        code = (
            "import sys; sys.path.insert(0, sys.argv[1]); import importlib\n"
            f"for m in {MODULES!r}: importlib.import_module(m)\n"
            "bad = sorted(m for m in sys.modules if m.split('.')[0] in {'pandas', 'bruin', 'duckdb', 'numpy', 'requests'})\n"
            "print(','.join(bad))"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-c", code, str(SCRIPTS)],
            cwd=TEMPLATE_ROOT,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "")


if __name__ == "__main__":
    unittest.main()
