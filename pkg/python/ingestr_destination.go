package python

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// ConnectionTypeGetter returns the configured type of a named connection,
// e.g. "google_cloud_platform" for a BigQuery connection.
type ConnectionTypeGetter interface {
	GetConnectionType(name string) string
}

// ingestrDestinationsByConnectionType maps a connection type to the ingestr
// `destination` value that writes to it.
var ingestrDestinationsByConnectionType = buildIngestrDestinationsByConnectionType()

func buildIngestrDestinationsByConnectionType() map[string]string {
	destinations := make(map[string]string, len(pipeline.IngestrTypeConnectionMapping)+len(ReverseETLIngestrDestinations))
	for destination, assetType := range pipeline.IngestrTypeConnectionMapping {
		if connectionType, ok := pipeline.AssetTypeConnectionMapping[assetType]; ok {
			destinations[connectionType] = destination
		}
	}

	// Reverse-ETL destinations share their name with their connection type.
	for destination := range ReverseETLIngestrDestinations {
		destinations[destination] = destination
	}

	return destinations
}

// InferIngestrDestination fills the `destination` parameter of an ingestr
// asset that leaves it out, using the type of the connection the asset writes
// to. This lets `connection` or `destination_connection` alone pick the
// destination. An explicit `destination` is never overwritten.
func InferIngestrDestination(asset *pipeline.Asset, p *pipeline.Pipeline, connections ConnectionTypeGetter) {
	if asset.Type != pipeline.AssetTypeIngestr || connections == nil {
		return
	}

	if destination, _ := asset.Parameters.GetString("destination"); destination != "" {
		return
	}

	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil || connectionName == "" {
		return
	}

	destination, ok := ingestrDestinationsByConnectionType[connections.GetConnectionType(connectionName)]
	if !ok {
		return
	}

	if asset.Parameters == nil {
		asset.Parameters = pipeline.ParameterMap{}
	}
	asset.Parameters["destination"] = destination
}
