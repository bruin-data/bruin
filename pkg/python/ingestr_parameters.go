package python

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"golang.org/x/mod/semver"
)

//go:embed ingestr_cdc_capabilities.json
var ingestrCDCCapabilities []byte

var knownCDCParameters = func() map[string]bool {
	var inventory struct {
		Parameters []string `json:"known_cdc_parameters"`
	}
	if err := json.Unmarshal(ingestrCDCCapabilities, &inventory); err != nil {
		panic("invalid embedded ingestr CDC capabilities: " + err.Error())
	}
	parameters := make(map[string]bool, len(inventory.Parameters))
	for _, parameter := range inventory.Parameters {
		parameters[parameter] = true
	}
	return parameters
}()

var (
	ingestrEngineVersionPattern = regexp.MustCompile(`^v(0|[1-9]\d*)(\.\d+\.\d+)?$`)
	postgresSnapshotPattern     = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{8}-[1-9][0-9]*$`)
)

// ResolveIngestrVersion resolves family selectors and exact pins consistently
// for validation and execution.
func ResolveIngestrVersion(asset *pipeline.Asset) (string, error) {
	raw, _ := asset.Parameters.GetString("version")
	value := strings.TrimSpace(raw)
	if value == "" {
		return IngestrVersionV1, nil
	}
	match := ingestrEngineVersionPattern.FindStringSubmatch(value)
	if match == nil {
		return "", fmt.Errorf("invalid parameters.version %q: expected vMAJOR or vMAJOR.MINOR.PATCH", value)
	}
	if match[2] != "" {
		return strings.TrimPrefix(value, "v"), nil
	}
	if match[1] == "0" {
		return IngestrVersionV0, nil
	}
	return IngestrVersionV1, nil
}

// ValidateIngestrCDCParameters validates explicit options without resolving
// connections or changing engine defaults. The inventory covers every CDC source.
func ValidateIngestrCDCParameters(asset *pipeline.Asset) error {
	params := asset.Parameters
	var unknown []string
	for key := range params {
		if strings.HasPrefix(key, "cdc_") && !knownCDCParameters[key] {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		return fmt.Errorf("unknown ingestr CDC parameter(s): %s", strings.Join(unknown, ", "))
	}
	for _, name := range []string{"cdc_table_naming", "cdc_discover_interval", "cdc_binary"} {
		if _, present := params[name]; !present {
			continue
		}
		if cdc, _ := params.GetString("cdc"); cdc != "true" {
			return fmt.Errorf("%s requires cdc: true", name)
		}
		value, _ := params.GetString(name)
		switch name {
		case "cdc_table_naming":
			if value != "schema_table" && value != "table" {
				return fmt.Errorf("invalid %s: must be schema_table or table", name)
			}
		case "cdc_binary":
			if value != "true" && value != "false" {
				return fmt.Errorf("invalid %s: must be true or false", name)
			}
		case "cdc_discover_interval":
			if value != "0" && value != "off" {
				duration, err := time.ParseDuration(value)
				if err != nil || duration < 0 {
					return fmt.Errorf("invalid %s: must be a nonnegative duration, 0, or off", name)
				}
			}
		}
	}
	if raw, present := params["postgres_snapshot"]; present {
		value, ok := raw.(string)
		if !ok || !postgresSnapshotPattern.MatchString(value) {
			return fmt.Errorf("invalid postgres_snapshot: expected an exported PostgreSQL snapshot ID such as 00000003-0000001B-1")
		}
		if cdc, _ := params.GetString("cdc"); cdc == "true" {
			return fmt.Errorf("postgres_snapshot requires ordinary PostgreSQL ingestion without cdc: true")
		}
	}
	return nil
}

func ValidateIngestrCDCParameterVersion(asset *pipeline.Asset, version string) error {
	for _, option := range []struct{ name, minimum string }{
		{"cdc_table_naming", "1.1.49"},
		{"cdc_discover_interval", "1.0.71"},
		{"cdc_binary", "1.0.72"},
		{"postgres_snapshot", "1.1.83"},
	} {
		if err := requireIngestrParameterVersion(asset.Parameters, option.name, option.minimum, version); err != nil {
			return err
		}
	}
	return ValidateCDCSnapshotEngineVersion(asset, version)
}

// IngestrMetricsAddress preserves the CDC alias while giving the canonical
// parameter precedence when both are explicitly set.
func IngestrMetricsAddress(params pipeline.ParameterMap) string {
	if value, present := params.GetString("metrics_addr"); present {
		return value
	}
	cdc, _ := params.GetString("cdc")
	if cdc != "true" && !isStreamingParams(params) {
		return ""
	}
	value, _ := params.GetString("cdc_stream_metrics_addr")
	return value
}

// ValidateIngestrLoadParameters is shared with seed and Python materialization
// callers, which use the same ingestr argument builder.
func ValidateIngestrLoadParameters(params pipeline.ParameterMap, version string) error {
	for _, name := range []string{"batch_size", "destination_parallelism"} {
		if _, present := params[name]; !present {
			continue
		}
		value, _ := params.GetString(name)
		number, err := strconv.ParseInt(value, 10, 64)
		if err != nil || number <= 0 {
			return fmt.Errorf("invalid %s: must be a positive integer", name)
		}
		if name == "batch_size" && number > math.MaxInt64>>20 {
			return fmt.Errorf("invalid batch_size: MiB value exceeds the signed 64-bit byte range")
		}
	}
	for _, name := range []string{"no_load_timestamp", "no_run_id"} {
		if _, present := params[name]; !present {
			continue
		}
		value, _ := params.GetString(name)
		if value != "true" && value != "false" {
			return fmt.Errorf("invalid %s: must be true or false", name)
		}
	}
	for _, name := range []string{"metrics_addr", "cdc_stream_metrics_addr"} {
		if raw, present := params[name]; present {
			value, ok := raw.(string)
			if !ok || strings.TrimSpace(value) == "" {
				return fmt.Errorf("invalid %s: must be a nonempty listen address", name)
			}
		}
	}
	for _, option := range []struct{ name, minimum string }{
		{"batch_size", "1.1.30"},
		{"destination_parallelism", "1.1.85"},
		{"no_load_timestamp", "1.0.39"},
		{"no_run_id", "1.1.31"},
	} {
		if err := requireIngestrParameterVersion(params, option.name, option.minimum, version); err != nil {
			return err
		}
	}
	mode, _ := params.GetString("cdc_mode")
	cdc, _ := params.GetString("cdc")
	streaming := isStreamingParams(params) || (cdc == "true" && mode == "stream")
	if IngestrMetricsAddress(params) != "" {
		if !streaming && semver.Compare("v"+version, "v1.1.87") < 0 {
			return fmt.Errorf("batch metrics require ingestr 1.1.87 or newer, but parameters.version resolved to %s", version)
		}
		if semver.Compare("v"+version, "v1.0.73") < 0 {
			return fmt.Errorf("streaming metrics require ingestr 1.0.73 or newer, but parameters.version resolved to %s", version)
		}
	}
	return nil
}

func requireIngestrParameterVersion(params pipeline.ParameterMap, name, minimum, version string) error {
	if _, present := params[name]; present && semver.Compare("v"+version, "v"+minimum) < 0 {
		return fmt.Errorf("%s requires ingestr %s or newer, but parameters.version resolved to %s", name, minimum, version)
	}
	return nil
}
