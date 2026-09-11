import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import generate_ingestr_hashes as generator


class GeneratorTests(unittest.TestCase):
    def fake_gh(self, *args):
        self.calls.append(args)
        if args[0] == "api":
            self.assertEqual(
                args, ("api", "repos/bruin-data/ingestr/releases/tags/v1.1.50")
            )
            return json.dumps(self.release)
        self.assertEqual(args[2], "v1.1.50")
        self.assertEqual(args[args.index("--repo") + 1], "bruin-data/ingestr")
        if args[1] == "download":
            name = args[args.index("--pattern") + 1]
            self.assertIn(name, generator.ARCHIVES)
            (Path(args[-1]) / name).write_bytes(name.encode())
        if args[1] == "verify-asset":
            self.assertTrue(Path(args[3]).is_file())
        if args[1] == self.fail:
            raise subprocess.CalledProcessError(1, args, stderr="invalid evidence")
        if args[1] == "verify":
            return json.dumps(self.verification)
        return ""

    def setUp(self):
        self.calls = []
        self.release = {"tag_name": "v1.1.50", "immutable": True}
        self.fail = None
        self.commit = "a" * 40
        self.git_commit = self.commit
        self.script = b"#!/bin/sh\nprintf fixture\n"
        self.verification = {
            "verificationResult": {
                "statement": {
                    "predicateType": "https://in-toto.io/attestation/release/v0.2",
                    "predicate": {
                        "repository": "bruin-data/ingestr",
                        "tag": "v1.1.50",
                        "purl": "pkg:github/bruin-data/ingestr@v1.1.50",
                    },
                    "subject": [
                        {
                            "uri": "pkg:github/bruin-data/ingestr@v1.1.50",
                            "digest": {"sha1": self.commit},
                        }
                    ],
                }
            }
        }
        self.git_calls = []
        git = patch.object(generator.subprocess, "run", side_effect=self.fake_git)
        git.start()
        self.addCleanup(git.stop)

    def fake_git(self, args, **kwargs):
        self.assertTrue(kwargs["check"])
        self.assertEqual(args[0], "git")
        self.assertEqual(args[3:5], ["-c", "transfer.fsckObjects=true"])
        command = args[5:]
        self.git_calls.append(command)
        output = b""
        if command[0] == "fetch":
            self.assertEqual(
                command,
                [
                    "fetch",
                    "--quiet",
                    "--depth=1",
                    "https://github.com/bruin-data/ingestr.git",
                    "refs/tags/v1.1.50",
                ],
            )
        elif command[0] == "rev-parse":
            self.assertEqual(command[1], "FETCH_HEAD^{commit}")
            output = (self.git_commit + "\n").encode()
        elif command[0] == "show":
            self.assertEqual(command[1], self.commit + ":install.sh")
            output = self.script
        return subprocess.CompletedProcess(args, 0, stdout=output)

    def test_exact_release_and_every_asset_verified(self):
        with patch.object(generator, "gh", self.fake_gh):
            result = json.loads(generator.generate("1.1.50"))
        self.assertEqual(
            result,
            {
                "1.1.50": {
                    "archives": {
                        name: hashlib.sha256(name.encode()).hexdigest()
                        for name in generator.ARCHIVES
                    },
                    "installer_commit": self.commit,
                    "installer_sha256": hashlib.sha256(self.script).hexdigest(),
                }
            },
        )
        self.assertEqual(
            self.calls[1],
            (
                "release",
                "verify",
                "v1.1.50",
                "--repo",
                "bruin-data/ingestr",
                "--format",
                "json",
            ),
        )
        self.assertEqual(sum(c[1] == "verify-asset" for c in self.calls), 5)

    def test_wrong_verified_source_identity(self):
        for key, wrong in (
            ("repository", "other/ingestr"),
            ("tag", "v1.1.49"),
            ("purl", "pkg:github/other/ingestr@v1.1.50"),
        ):
            predicate = self.verification["verificationResult"]["statement"][
                "predicate"
            ]
            with self.subTest(key=key), patch.dict(predicate, {key: wrong}):
                with self.assertRaisesRegex(ValueError, "wrong repository or tag"):
                    generator.installer_hash("v1.1.50", self.verification)
                self.assertEqual(self.git_calls, [])

    def test_missing_verified_commit(self):
        statement = self.verification["verificationResult"]["statement"]
        for subjects in (
            [],
            [
                {
                    "uri": "pkg:github/other/ingestr@v1.1.50",
                    "digest": {"sha1": self.commit},
                }
            ],
        ):
            with (
                self.subTest(subjects=subjects),
                patch.dict(statement, {"subject": subjects}),
            ):
                with self.assertRaisesRegex(
                    ValueError, "one verified installer source"
                ):
                    generator.installer_hash("v1.1.50", self.verification)
                self.assertEqual(self.git_calls, [])

    def test_fetched_commit_must_match_signed_commit(self):
        self.git_commit = "b" * 40
        with self.assertRaisesRegex(ValueError, "does not match"):
            generator.installer_hash("v1.1.50", self.verification)
        self.assertFalse(any(command[0] == "show" for command in self.git_calls))

    def test_git_failure_propagates(self):
        with patch.object(
            generator.subprocess,
            "run",
            side_effect=subprocess.CalledProcessError(1, "git"),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                generator.installer_hash("v1.1.50", self.verification)

    def test_missing_immutable_or_wrong_tag(self):
        for release in (
            {},
            {"tag_name": "v1.1.50", "immutable": False},
            {"tag_name": "v1.1.49", "immutable": True},
        ):
            with (
                self.subTest(release=release),
                patch.object(generator, "gh", self.fake_gh),
            ):
                self.calls = []
                self.release = release
                with self.assertRaises(ValueError):
                    generator.generate("1.1.50")
                self.assertEqual(len(self.calls), 1)

    def test_verification_or_download_failure_preserves_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pkg/python").mkdir(parents=True)
            (root / "pkg/python/uv.go").write_text('IngestrVersionV1 = "1.1.50"')
            output = root / "pkg/python/ingestr_hashes.json"
            output.write_text("previous trusted output")
            for failure in ("verify", "download", "verify-asset"):
                with (
                    self.subTest(failure=failure),
                    patch.object(generator, "ROOT", root),
                    patch.object(generator, "gh", self.fake_gh),
                    patch("sys.argv", ["generator"]),
                ):
                    self.fail = failure
                    with self.assertRaises(subprocess.CalledProcessError):
                        generator.main()
                    self.assertEqual(output.read_text(), "previous trusted output")

    def test_check_rejects_stale_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pkg/python").mkdir(parents=True)
            (root / "pkg/python/uv.go").write_text('IngestrVersionV1 = "1.1.50"')
            output = root / "pkg/python/ingestr_hashes.json"
            output.write_text("{}")
            with (
                patch.object(generator, "ROOT", root),
                patch.object(generator, "gh", self.fake_gh),
                patch("sys.argv", ["generator", "--check"]),
            ):
                with self.assertRaisesRegex(ValueError, "differ"):
                    generator.main()
                self.assertEqual(output.read_text(), "{}")

    def test_no_latest_or_version_injection(self):
        for version in ("latest", "v1.1.50", "1.1.50/other", "1.1.50;false"):
            with patch.object(generator, "gh") as gh, self.assertRaises(ValueError):
                generator.generate(version)
            gh.assert_not_called()

    def test_gh_errors_propagate_and_host_is_pinned(self):
        with (
            patch.dict("os.environ", {"GH_HOST": "other.example"}),
            patch(
                "subprocess.run", side_effect=subprocess.CalledProcessError(1, "gh")
            ) as run,
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                generator.gh("release", "verify", "v1.1.50", "--repo", generator.REPO)
            self.assertTrue(run.call_args.kwargs["check"])
            self.assertEqual(run.call_args.kwargs["env"]["GH_HOST"], "github.com")


if __name__ == "__main__":
    unittest.main()
