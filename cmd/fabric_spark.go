package cmd

import (
	"context"
	"sync"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/fabric"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// assetConnectionResolver resolves the connection an asset executes against,
// including the asset types whose client is derived from another connection
// type. Derived clients are built once and shared, so a command that walks many
// assets reuses a single Fabric Spark session per connection instead of opening
// one per asset.
type assetConnectionResolver struct {
	connections config.ConnectionAndDetailsGetter

	fabricSparkOnce sync.Once
	fabricSpark     *fabric.SparkConnectionGetter
}

func newAssetConnectionResolver(connections config.ConnectionAndDetailsGetter) *assetConnectionResolver {
	return &assetConnectionResolver{connections: connections}
}

func (r *assetConnectionResolver) Resolve(
	ctx context.Context,
	assetType pipeline.AssetType,
	connectionName string,
) (any, error) {
	if assetType == pipeline.AssetTypeFabricSparkQuery {
		r.fabricSparkOnce.Do(func() {
			r.fabricSpark = fabric.NewSparkConnectionGetter(r.connections)
		})
		return r.fabricSpark.ResolveSparkClient(ctx, connectionName)
	}
	connection := r.connections.GetConnection(connectionName)
	if connection == nil {
		return nil, config.NewConnectionNotFoundError(ctx, "", connectionName)
	}
	return connection, nil
}

// resolveConnectionForAsset resolves a single asset's connection. Callers that
// resolve connections for more than one asset should build an
// assetConnectionResolver once and reuse it instead.
func resolveConnectionForAsset(
	ctx context.Context,
	connections config.ConnectionAndDetailsGetter,
	assetType pipeline.AssetType,
	connectionName string,
) (any, error) {
	return newAssetConnectionResolver(connections).Resolve(ctx, assetType, connectionName)
}
