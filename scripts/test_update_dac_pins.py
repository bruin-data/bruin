import hashlib
import unittest

import update_dac_pins as pins


class GenerateTests(unittest.TestCase):
    def setUp(self):
        self.files = {name: name.encode() for name in pins.ARCHIVES}
        self.checksums = "".join(
            f"{hashlib.sha256(body).hexdigest()}  {name}\n" for name, body in self.files.items()
        )
        self.urls = []

    def fetch(self, url):
        self.urls.append(url)
        prefix = "https://github.com/bruin-data/dac/releases/download/v1.2.3/"
        self.assertTrue(url.startswith(prefix))
        name = url[len(prefix):]
        if name == "checksums.txt":
            return self.checksums.encode()
        return self.files[name]

    def test_pins_every_archive(self):
        generated = pins.generate("1.2.3", fetch=self.fetch)
        self.assertIn('"version": "1.2.3"', generated)
        for name, body in self.files.items():
            self.assertIn(f'"{name}": "{hashlib.sha256(body).hexdigest()}"', generated)
        self.assertEqual(len(self.urls), len(pins.ARCHIVES) + 1)

    def test_rejects_archive_that_does_not_match_checksums(self):
        self.files["dac_Linux_x86_64.tar.gz"] = b"tampered"
        with self.assertRaisesRegex(ValueError, "dac_Linux_x86_64.tar.gz does not match"):
            pins.generate("1.2.3", fetch=self.fetch)

    def test_rejects_archive_missing_from_checksums(self):
        self.checksums = self.checksums.replace("dac_Windows_x86_64.zip", "other.zip")
        with self.assertRaisesRegex(ValueError, "dac_Windows_x86_64.zip does not match"):
            pins.generate("1.2.3", fetch=self.fetch)

    def test_rejects_non_release_versions(self):
        for version in ("v1.2.3", "0.0.0-edge.130.b33e5af", "latest"):
            with self.assertRaises(ValueError):
                pins.generate(version, fetch=self.fetch)
        self.assertEqual(self.urls, [])


if __name__ == "__main__":
    unittest.main()
