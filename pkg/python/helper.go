package python

import (
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func ConsolidatedParameters(asset *pipeline.Asset, cmdArgs []string) []string {
	if format, exists := asset.Parameters["loader_file_format"]; exists && format != "" {
		cmdArgs = append(cmdArgs, "--loader-file-format", format)
	}
	return cmdArgs
}

func AddExtraPackages(destURI, sourceURI string, extraPackages []string) []string {
	if strings.HasPrefix(sourceURI, "mssql://") || strings.HasPrefix(destURI, "mssql://") {
		extraPackages = []string{"pyodbc==5.1.0"}
	}
	return extraPackages
}
