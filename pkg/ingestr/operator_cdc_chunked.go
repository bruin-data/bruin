package ingestr

import (
	"errors"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
)

func appendCDCChunkedSnapshotParameters(args []string, engine resolvedEngine, asset *pipeline.Asset, sourceURI string) ([]string, error) {
	if !python.HasCDCChunkedSnapshotParameters(asset) {
		return args, nil
	}
	if err := python.ValidateCDCChunkedSnapshotParameters(asset); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(sourceURI, "postgres+cdc://") && !strings.HasPrefix(sourceURI, "postgresql+cdc://") {
		return nil, errors.New("CDC snapshot parameters are only supported for PostgreSQL CDC sources")
	}
	if err := python.ValidateCDCSnapshotEngineVersion(asset, engine.ingestrVersion); err != nil {
		return nil, err
	}
	for _, name := range []string{"cdc_snapshot_chunked", "cdc_snapshot_spool"} {
		if value, ok := asset.Parameters.GetString(name); ok {
			args = append(args, "--"+strings.ReplaceAll(name, "_", "-")+"="+value)
		}
	}
	for _, name := range []string{
		"cdc_snapshot_execution",
		"cdc_snapshot_chunk_size",
		"cdc_snapshot_chunk_timeout",
		"cdc_wal_staging",
		"cdc_wal_spool_dir",
		"cdc_wal_buffer_bytes",
	} {
		if value, ok := asset.Parameters.GetString(name); ok {
			args = append(args, "--"+strings.ReplaceAll(name, "_", "-"), value)
		}
	}
	return args, nil
}
