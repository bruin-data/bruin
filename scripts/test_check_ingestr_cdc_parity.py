import base64
import json
import subprocess
import unittest
from unittest.mock import patch

import check_ingestr_cdc_parity as parity


CLI = """package cmd
var flags = []cli.Flag{
    &cli.BoolFlag{
        Name: "cdc-snapshot-spool",
        Usage: "A quoted brace } must not close the declaration",
    },
    &cli.StringFlag{
        Name: "metrics-addr",
        Value: "",
    },
}
"""
URI = """package postgres_cdc
const defaultDiscoverInterval = 30 * time.Second
func parseURIConfig(uri string) (CDCConfig, string, error) {
    query := parsed.Query()
    cfg.SlotName = query.Get("slot")
    query.Del("slot")
    return cfg, "", nil
}
"""
CONFIG = """package config
const DefaultCDCWALBufferBytes = int64(64 << 20)
func (c *IngestConfig) validateCDCChunkedSnapshot() error {
    if c.CDCWALBufferBytes <= 0 { return errInvalid }
    return nil
}
"""
SNAPSHOT = """package postgres
func parseSnapshotURI(uri string) (string, string, error) {
    params.Del("snapshot")
    return "", "", nil
}
"""


class ParityTests(unittest.TestCase):
    def setUp(self):
        self.sources = dict(
            zip(parity.SOURCE_PATHS, (CLI, URI, CONFIG, "package config\n", SNAPSHOT))
        )
        self.manifest = {
            "known_cdc_parameters": ["cdc_slot", "cdc_snapshot_spool"],
            "cli": {
                "cdc-snapshot-spool": {
                    "parameter": "cdc_snapshot_spool",
                    "status": "supported",
                    "type": "BoolFlag",
                    "default": None,
                },
                "metrics-addr": {
                    "parameter": "metrics_addr",
                    "status": "supported",
                    "type": "StringFlag",
                    "default": '""',
                },
            },
            "uri": {"slot": {"parameter": "cdc_slot", "status": "supported"}},
            "postgres_uri": {
                "snapshot": {"parameter": "postgres_snapshot", "status": "supported"}
            },
            "constants": {
                "internal/config/config.go": {
                    "DefaultCDCWALBufferBytes": "int64(64 << 20)"
                }
            },
            "reviewed_functions": {
                "internal/config/config.go": {
                    "validateCDCChunkedSnapshot": parity.function_digest(
                        CONFIG, "validateCDCChunkedSnapshot"
                    )
                }
            },
        }

    def check(self):
        return "\n".join(parity.check(self.manifest, self.sources))

    def test_reviewed_interface_passes(self):
        self.assertEqual(self.check(), "")

    def test_new_cdc_flag_requires_a_decision(self):
        self.sources[parity.SOURCE_PATHS[0]] += (
            '\n&cli.BoolFlag{Name: "cdc-new-option"}\n'
        )
        self.assertIn("new upstream flag --cdc-new-option", self.check())
        self.manifest["cli"]["cdc-new-option"] = {
            "status": "deferred",
            "reason": "Needs destination support review",
            "type": "BoolFlag",
            "default": None,
        }
        self.assertEqual(self.check(), "")

    def test_removed_flag_and_changed_type_or_default(self):
        for replacement, expected in (
            (CLI.replace('"metrics-addr"', '"metrics-address"'), "removed or renamed"),
            (CLI.replace("&cli.BoolFlag", "&cli.StringFlag"), "type/default changed"),
            (
                CLI.replace('Value: ""', 'Value: "127.0.0.1:6060"'),
                "type/default changed",
            ),
        ):
            with self.subTest(expected=expected):
                self.sources[parity.SOURCE_PATHS[0]] = replacement
                self.assertIn(expected, self.check())

    def test_new_or_removed_uri_option(self):
        self.sources[parity.SOURCE_PATHS[1]] = URI.replace(
            'query.Del("slot")', 'query.Del("new_option")'
        )
        self.assertIn("new PostgreSQL CDC URI option new_option", self.check())
        self.sources[parity.SOURCE_PATHS[1]] = URI.replace('"slot"', '"slot_name"')
        self.assertIn("URI option slot removed or renamed", self.check())

    def test_manifest_mapping_must_agree_with_runtime_allowlist(self):
        self.manifest["known_cdc_parameters"].remove("cdc_snapshot_spool")
        self.assertIn("missing from the runtime CDC allowlist", self.check())

    def test_deferral_requires_a_reason(self):
        self.manifest["cli"]["cdc-snapshot-spool"]["status"] = "deferred"
        self.assertIn(
            "requires supported mapping or deferred/excluded reason", self.check()
        )

    def test_default_constant_change_requires_review(self):
        self.sources["internal/config/config.go"] = CONFIG.replace(
            "64 << 20", "128 << 20"
        )
        self.assertIn("upstream DefaultCDCWALBufferBytes changed", self.check())

    def test_validation_change_requires_review(self):
        self.sources["internal/config/config.go"] = CONFIG.replace("<= 0", "< 0")
        self.assertIn("review accepted values, defaults and restrictions", self.check())

    def test_comments_and_whitespace_do_not_change_validation_digest(self):
        self.sources["internal/config/config.go"] = CONFIG.replace(
            "return nil", "// a brace } in a comment\n    return    nil"
        )
        self.assertEqual(self.check(), "")

    def test_source_parser_fails_closed_when_upstream_moves(self):
        for operation in (
            lambda: parity.parse_cli("package cmd"),
            lambda: parity.parse_uri("package postgres_cdc"),
            lambda: parity.constant("package config", "DefaultCDCWALBufferBytes"),
            lambda: parity.function_digest(
                "package config", "validateCDCChunkedSnapshot"
            ),
        ):
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                operation()

    def test_fetch_pins_host_repository_tag_and_reads_only_content(self):
        payload = {
            "encoding": "base64",
            "content": base64.b64encode(CLI.encode()).decode(),
        }
        completed = subprocess.CompletedProcess([], 0, stdout=json.dumps(payload))
        with patch.object(parity.subprocess, "run", return_value=completed) as run:
            self.assertEqual(parity.fetch_source("1.1.87", "cmd/ingest.go"), CLI)
        self.assertEqual(
            run.call_args.args[0],
            [
                "gh",
                "api",
                "--hostname",
                "github.com",
                "--method",
                "GET",
                "repos/bruin-data/ingestr/contents/cmd/ingest.go",
                "-f",
                "ref=v1.1.87",
            ],
        )
        self.assertTrue(run.call_args.kwargs["check"])

    def test_network_failure_does_not_report_parity(self):
        with patch.object(
            parity.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "gh")
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                parity.fetch_source("1.1.87", "cmd/ingest.go")

    def test_invalid_version_rejected_before_fetch(self):
        for version in ("latest", "v1.1.87", "1.1.87;echo", "1.1.87/other"):
            with (
                patch("sys.argv", ["check", "--version", version]),
                patch.object(parity, "fetch_source") as fetch,
            ):
                with self.assertRaises(ValueError):
                    parity.main()
                fetch.assert_not_called()

    def test_real_manifest_is_consistent_and_covers_existing_source_aliases(self):
        manifest = json.loads(parity.MANIFEST.read_text())
        known = manifest["known_cdc_parameters"]
        self.assertEqual(known, sorted(set(known)))
        for name in (
            "cdc_backend",
            "cdc_mode",
            "cdc_stream_metrics_addr",
            "cdc_grpc_tls",
            "cdc_sql_capture",
            "cdc_max_await_time",
            "cdc_server_id",
        ):
            self.assertIn(name, known)
        issues = []
        for section in ("cli", "uri", "postgres_uri"):
            for name, entry in manifest[section].items():
                parity.validate_entry(name, entry, known, issues)
        self.assertEqual(issues, [])


if __name__ == "__main__":
    unittest.main()
