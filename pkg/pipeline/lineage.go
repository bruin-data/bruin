package pipeline

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/sqlparser"
)

type Parser interface {
	ColumnLineage(foundPipeline *Pipeline, asset *Asset) error
}

type LineageExtractor struct{}

// ParseLineageRecursive processes the lineage of an asset and its upstream dependencies recursively.
func (p *LineageExtractor) ColumnLineage(foundPipeline *Pipeline, asset *Asset) error {
	if err := p.parseLineage(foundPipeline, asset); err != nil {
		return err
	}

	if len(asset.Columns) == 0 {
		for _, upstream := range asset.Upstreams {
			upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
			if upstreamAsset == nil {
				continue
			}

			if err := p.ColumnLineage(foundPipeline, upstreamAsset); err != nil {
				return err
			}
		}

		if err := p.parseLineage(foundPipeline, asset); err != nil {
			return err
		}
	}

	return nil
}

// ParseLineage analyzes the column lineage for a given asset within a
// It traces column relationships between the asset and its upstream dependencies.
func (p *LineageExtractor) parseLineage(pipe *Pipeline, asset *Asset) error {
	parser, err := sqlparser.NewSQLParser()
	if err != nil {
		return fmt.Errorf("failed to create SQL parser: %w", err)
	}

	if err := parser.Start(); err != nil {
		return fmt.Errorf("failed to start SQL parser: %w", err)
	}

	columnMetadata := make(sqlparser.Schema)
	for _, upstream := range asset.Upstreams {
		upstreamAsset := pipe.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			return fmt.Errorf("upstream asset not found: %s", upstream.Value)
		}
		if len(upstreamAsset.Columns) > 0 {
			columnMetadata[upstreamAsset.Name] = makeColumnMap(upstreamAsset.Columns)
		}
	}

	lineage, err := parser.ColumnLineage(asset.ExecutableFile.Content, "", columnMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse column lineage: %w", err)
	}
	for _, lineageCol := range lineage.Columns {
		for _, upstream := range lineageCol.Upstream {
			upstreamAsset := pipe.GetAssetByName(upstream.Table)

			if upstreamAsset == nil {
				continue
			}

			if upstream.Column != "*" {
				upstreamCol := upstreamAsset.GetColumnWithName(upstream.Column)
				if upstreamCol == nil {
					continue
				}

				if lineageCol.Name != "*" {
					newCol := *upstreamCol
					newCol.Name = lineageCol.Name
					newCol.Upstreams = append(newCol.Upstreams, UpstreamColumn{
						Asset:      upstreamAsset.Name,
						Column:     upstreamCol.Name,
						Table:      upstreamAsset.Name,
						AssetFound: true,
					})
					if col := asset.GetColumnWithName(lineageCol.Name); col == nil {
						asset.Columns = append(asset.Columns, newCol)
					}
				}
			}
		}
	}

	return nil
}

// makeColumnMap creates a map of column names to their types from a slice of columns.
func makeColumnMap(columns []Column) map[string]string {
	columnMap := make(map[string]string, len(columns))
	for _, col := range columns {
		columnMap[col.Name] = col.Type
	}
	return columnMap
}
