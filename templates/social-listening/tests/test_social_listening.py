import unittest

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
from deliver_outbox import deliver
from hackernews_collect import collect
from source_interfaces import require_read_only


class SocialListeningTests(unittest.TestCase):
    def test_bounded_collection_rejects_invalid_window(self):
        with self.assertRaises(ValueError):
            collect("2026-01-02", "2026-01-01", ["term"], 1)

    def test_delivered_alert_is_idempotent(self):
        self.assertEqual(deliver({"status": "delivered", "payload_json": "{}"}, lambda _: None), "already_delivered")

    def test_dry_run_never_sends(self):
        self.assertEqual(deliver({"status": "dry_run", "payload_json": "{}"}, lambda _: self.fail()), "dry_run")

    def test_authorised_interfaces_are_read_only(self):
        self.assertFalse(require_read_only("github")["mutation"])


if __name__ == "__main__":
    unittest.main()
