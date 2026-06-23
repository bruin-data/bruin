// Package bootstrap applies process-wide defaults before transitive driver
// imports initialize.
package bootstrap

import "os"

func init() { //nolint:gochecknoinits
	// Disable the Snowflake driver's platform detection. The driver starts this
	// in its own init function and otherwise probes EC2, Azure, and GCP metadata
	// services even for commands that do not use Snowflake.
	if os.Getenv("SNOWFLAKE_DISABLE_PLATFORM_DETECTION") == "" {
		_ = os.Setenv("SNOWFLAKE_DISABLE_PLATFORM_DETECTION", "true")
	}
}
