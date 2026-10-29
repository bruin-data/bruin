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

// IngestrDestinationForConnectionType returns the ingestr `destination` that
// writes to connections of the given type.
func IngestrDestinationForConnectionType(connectionType string) (string, bool) {
	destination, ok := ingestrDestinationsByConnectionType[connectionType]
	return destination, ok
}

// InferIngestrDestination fills the `destination` parameter of an ingestr
// asset that leaves it out, using the type of the connection the asset writes
// to. This lets `connection` or `destination_connection` alone pick the
// destination. An explicit `destination` is never overwritten.
//
// Inference assets publish through ingestr to their own `connection`, so their
// `destination` is always derived from it. Quality checks use it to pick the
// warehouse that runs them, the same way they do for ingestr assets.
func InferIngestrDestination(asset *pipeline.Asset, p *pipeline.Pipeline, connections ConnectionTypeGetter) {
	if connections == nil {
		return
	}

	switch asset.Type {
	case pipeline.AssetTypeIngestr:
		if destination, _ := asset.Parameters.GetString("destination"); destination != "" {
			return
		}
	case pipeline.AssetTypeInference:
		// Never route checks by a user-set value that disagrees with the connection.
		delete(asset.Parameters, "destination")
	default:
		return
	}

	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil || connectionName == "" {
		return
	}

	destination, ok := IngestrDestinationForConnectionType(connections.GetConnectionType(connectionName))
	if !ok {
		return
	}

	if asset.Parameters == nil {
		asset.Parameters = pipeline.ParameterMap{}
	}
	asset.Parameters["destination"] = destination
}

// InferIngestrDestinations runs InferIngestrDestination on every asset of a
// fully built pipeline. It must run after variant rendering, which resolves
// templated connections.
func InferIngestrDestinations(p *pipeline.Pipeline, connections ConnectionTypeGetter) {
	if connections == nil {
		return
	}

	// Some secrets backends fetch the secret on every lookup, so resolve each connection once.
	cached := &cachedConnectionTypes{inner: connections, types: map[string]string{}}
	for _, asset := range p.Assets {
		InferIngestrDestination(asset, p, cached)
	}
}

type cachedConnectionTypes struct {
	inner ConnectionTypeGetter
	types map[string]string
}

func (c *cachedConnectionTypes) GetConnectionType(name string) string {
	if connectionType, ok := c.types[name]; ok {
		return connectionType
	}

	connectionType := c.inner.GetConnectionType(name)
	c.types[name] = connectionType
	return connectionType
}
