package lint

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// materializationTypeValidator represents a connection capable of
// checking if the materialization type of an asset matches the
// existing object in the database.
type materializationTypeValidator interface {
	MaterializationTypeMatches(ctx context.Context, asset *pipeline.Asset) (bool, error)
}

// ValidateMaterializationTypeMatches validates that the materialization
// type defined for an asset matches the object type in the destination
// data platform. It returns an issue if a mismatch is detected.
func ValidateMaterializationTypeMatches(connections connectionManager) AssetValidator {
	return func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		issues := make([]*Issue, 0)

		if asset.Materialization.Type == pipeline.MaterializationTypeNone {
			return issues, nil
		}

		connName, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Cannot get connection for asset: %v", err),
			})
			return issues, nil
		}

		conn, err := connections.GetConnection(connName)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Cannot get connection for asset: %v", err),
			})
			return issues, nil
		}

		validator, ok := conn.(materializationTypeValidator)
		if !ok {
			return issues, nil
		}

		match, err := validator.MaterializationTypeMatches(ctx, asset)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Failed to validate materialization type: %v", err),
			})
			return issues, nil
		}
		if !match {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Materialization type mismatch between asset and destination", // simple message
			})
		}
		return issues, nil
	}
}
