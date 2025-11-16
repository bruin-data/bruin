package r

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type RVersion struct {
	Major int
	Minor int
	Patch int
}

func (v RVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// GetInstalledRVersion returns the version of R that is currently installed.
func GetInstalledRVersion() (*RVersion, error) {
	cmd := exec.Command("R", "--version")
	output, err := cmd.Output()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get R version - is R installed?")
	}

	// Parse output like "R version 4.3.1 (2023-06-16) -- "Beagle Scouts""
	re := regexp.MustCompile(`R version (\d+)\.(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(string(output))
	if len(matches) != 4 {
		return nil, errors.New("failed to parse R version from output: " + string(output))
	}

	var major, minor, patch int
	if _, err := fmt.Sscanf(matches[1], "%d", &major); err != nil {
		return nil, errors.Wrap(err, "failed to parse major version")
	}
	if _, err := fmt.Sscanf(matches[2], "%d", &minor); err != nil {
		return nil, errors.Wrap(err, "failed to parse minor version")
	}
	if _, err := fmt.Sscanf(matches[3], "%d", &patch); err != nil {
		return nil, errors.Wrap(err, "failed to parse patch version")
	}

	return &RVersion{Major: major, Minor: minor, Patch: patch}, nil
}

// VerifyRInstalled checks if R is installed and returns an error with installation instructions if not.
func VerifyRInstalled() error {
	_, err := findPathToExecutable([]string{"R", "Rscript"})
	if err != nil {
		return errors.New(`R is not installed or not in PATH.

Please install R:
- macOS: brew install r
- Ubuntu/Debian: sudo apt-get install r-base
- Windows: Download from https://cran.r-project.org/bin/windows/base/
- Other Linux: See https://cran.r-project.org/`)
	}
	return nil
}

// ParseRVersionFromImage parses R version from image string like "r:4.3" or "r:4.3.1".
// Returns nil if no version specified (use system R).
func ParseRVersionFromImage(image string) (*RVersion, error) {
	if image == "" || image == "r" {
		return nil, nil
	}

	// Handle formats: "r:4.3", "r:4.3.1", "4.3", "4.3.1"
	parts := strings.Split(image, ":")
	var versionStr string
	switch len(parts) {
	case 2:
		if parts[0] != "r" {
			return nil, fmt.Errorf("invalid R image format: %s (expected 'r:VERSION')", image)
		}
		versionStr = parts[1]
	case 1:
		versionStr = parts[0]
	default:
		return nil, fmt.Errorf("invalid R image format: %s", image)
	}

	// Parse version string
	versionParts := strings.Split(versionStr, ".")
	if len(versionParts) < 2 || len(versionParts) > 3 {
		return nil, fmt.Errorf("invalid R version format: %s (expected X.Y or X.Y.Z)", versionStr)
	}

	version := &RVersion{}
	if _, err := fmt.Sscanf(versionParts[0], "%d", &version.Major); err != nil {
		return nil, errors.Wrap(err, "failed to parse major version")
	}
	if _, err := fmt.Sscanf(versionParts[1], "%d", &version.Minor); err != nil {
		return nil, errors.Wrap(err, "failed to parse minor version")
	}
	if len(versionParts) == 3 {
		if _, err := fmt.Sscanf(versionParts[2], "%d", &version.Patch); err != nil {
			return nil, errors.Wrap(err, "failed to parse patch version")
		}
	}

	return version, nil
}

// CheckRVersion verifies that the installed R version is compatible with the requested version.
// For now, we just warn if versions don't match - in the future we could support multiple R versions.
func CheckRVersion(ctx context.Context, requestedVersion *RVersion) error {
	if requestedVersion == nil {
		// No specific version requested, any R version is fine
		return nil
	}

	installedVersion, err := GetInstalledRVersion()
	if err != nil {
		return err
	}

	// For now, just check if major.minor match
	if installedVersion.Major != requestedVersion.Major || installedVersion.Minor != requestedVersion.Minor {
		return fmt.Errorf(
			"r version mismatch: requested %d.%d.x but found %s installed, "+
				"please install R %d.%d or remove the version specification from your asset",
			requestedVersion.Major, requestedVersion.Minor,
			installedVersion.String(),
			requestedVersion.Major, requestedVersion.Minor,
		)
	}

	return nil
}
