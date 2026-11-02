package ingestr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"golang.org/x/mod/semver"
)

func appendCDCChunkedSnapshotParameters(args []string, engine resolvedEngine, asset *pipeline.Asset, sourceURI string) ([]string, error) {
	if !python.HasCDCChunkedSnapshotParameters(asset) {
		return args, nil
	}
	if err := python.ValidateCDCChunkedSnapshotParameters(asset); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(sourceURI, "postgres+cdc://") && !strings.HasPrefix(sourceURI, "postgresql+cdc://") {
		return nil, errors.New("chunked CDC snapshot parameters are only supported for PostgreSQL CDC sources")
	}
	if semver.Compare("v"+engine.ingestrVersion, "v1.1.81") < 0 {
		return nil, fmt.Errorf("chunked CDC snapshot parameters require ingestr 1.1.81 or newer, but parameters.version resolved to %s", engine.ingestrVersion)
	}
	if chunked, ok := asset.Parameters.GetString("cdc_snapshot_chunked"); ok {
		args = append(args, "--cdc-snapshot-chunked="+chunked)
	}
	for _, name := range []string{
		"cdc_snapshot_execution",
		"cdc_snapshot_chunk_size",
		"cdc_snapshot_chunk_timeout",
		"cdc_wal_staging",
		"cdc_wal_buffer_bytes",
	} {
		if value, ok := asset.Parameters.GetString(name); ok {
			args = append(args, "--"+strings.ReplaceAll(name, "_", "-"), value)
		}
	}
	return args, nil
}
