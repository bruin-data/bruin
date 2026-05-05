package gong

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
)

// parseChecksumManifest parses a sha256sum-format checksum file and returns
// the expected SHA256 hex string for the given artifactName.
//
// The expected file format is one entry per line:
//
//	<sha256hex>  <filename>
//
// Empty lines and lines starting with '#' are ignored.
func parseChecksumManifest(contents []byte, artifactName string) (string, error) {
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		checksum := parts[0]
		filename := parts[1]
		if filename != artifactName {
			continue
		}
		if !isValidSHA256(checksum) {
			return "", fmt.Errorf("invalid checksum for %s", artifactName)
		}
		return checksum, nil
	}
	return "", fmt.Errorf("checksum entry not found for %s", artifactName)
}

// verifySHA256 computes the SHA256 digest of the file at path and compares it
// against the expected hex string. The comparison is case-insensitive so both
// uppercase and lowercase checksums are accepted.
func verifySHA256(path string, expected string) error {
	if !isValidSHA256(expected) {
		return fmt.Errorf("invalid expected checksum")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	actual := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(actual, expected) {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

// isValidSHA256 reports whether value is a valid SHA256 hex digest:
// exactly 64 hexadecimal characters (case-insensitive).
func isValidSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
