package pipeline

import (
	"errors"
	"fmt"

	"github.com/bruin-data/bruin/pkg/sqlparser"
)

// Define constants for known SQL dialects.
const (
	BigQueryDialect  = "bigquery"
	SnowflakeDialect = "snowflake"
	DuckDBDialect    = "duckdb"
)

// Make the map immutable and use constants.
var assetTypeDialectMap = map[AssetType]string{
	"bq.sql":     BigQueryDialect,
	"sf.sql":     SnowflakeDialect,
	"duckdb.sql": DuckDBDialect,
}

type LineageExtractor struct {
	Pipeline       *Pipeline
	columnMetadata sqlparser.Schema
}

// NewLineageExtractor creates a new LineageExtractor instance.
func NewLineageExtractor(pipeline *Pipeline) *LineageExtractor {
	return &LineageExtractor{
		Pipeline:       pipeline,
		columnMetadata: make(sqlparser.Schema),
	}
}

// validateDialect checks if the asset type has a valid SQL dialect.
func validateDialect(assetType AssetType) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type: %s", assetType)
	}
	return dialect, nil
}

// TableSchema extracts the table schema from the assets and stores it in the columnMetadata map.
func (p *LineageExtractor) TableSchema() error {
	for _, foundAsset := range p.Pipeline.Assets {
		if len(foundAsset.Columns) > 0 {
			p.columnMetadata[foundAsset.Name] = makeColumnMap(foundAsset.Columns)
		}
	}
	return nil
}

// ParseLineageRecursive processes the lineage of an asset and its upstream dependencies recursively.
func (p *LineageExtractor) ColumnLineage(asset *Asset) error {
	if asset == nil {
		return nil
	}

	if len(asset.Columns) > 0 {
		return nil
	}

	for _, upstream := range asset.Upstreams {
		upstreamAsset := p.Pipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			continue
		}
		if err := p.ColumnLineage(upstreamAsset); err != nil {
			return err
		}
	}

	if err := p.parseLineage(asset); err != nil {
		return err
	}

	return nil
}

// ParseLineage analyzes the column lineage for a given asset within a
// It traces column relationships between the asset and its upstream dependencies.
func (p *LineageExtractor) parseLineage(asset *Asset) error {
	if asset == nil {
		return errors.New("invalid arguments: asset and pipeline cannot be nil")
	}

	dialect, err := validateDialect(asset.Type)
	if err != nil {
		return err
	}

	parser, err := sqlparser.NewSQLParser()
	if err != nil {
		return fmt.Errorf("failed to create SQL parser: %w", err)
	}

	if err := parser.Start(); err != nil {
		return fmt.Errorf("failed to start SQL parser: %w", err)
	}

	for _, upstream := range asset.Upstreams {
		upstreamAsset := p.Pipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			return fmt.Errorf("upstream asset not found: %s", upstream.Value)
		}
	}

	lineage, err := parser.ColumnLineage(asset.ExecutableFile.Content, dialect, p.columnMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse column lineage: %w", err)
	}
	// fmt.Println("======")
	// gs, _ := json.MarshalIndent(lineage, "", "  ")
	// fmt.Println(string(gs))
	// ts, _ := json.MarshalIndent(p.columnMetadata, "", "  ")
	// fmt.Println(string(ts))
	// fmt.Println(asset.ExecutableFile.Content)
	// fmt.Println(asset.Name)
	// fmt.Println(dialect)
	// fmt.Println("======")
	return p.processLineageColumns(asset, lineage)
}

func (p *LineageExtractor) processLineageColumns(asset *Asset, lineage *sqlparser.Lineage) error {
	if lineage == nil {
		return nil
	}

	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	for _, lineageCol := range lineage.Columns {
		if lineageCol.Name == "*" {
			for _, upstream := range lineageCol.Upstream {
				upstreamAsset := p.Pipeline.GetAssetByName(upstream.Table)
				if upstreamAsset == nil {
					continue
				}

				// If upstream column is *, copy all columns from upstream asset
				if upstream.Column == "*" {
					for _, upstreamCol := range upstreamAsset.Columns {
						if err := p.addColumnToAsset(asset, upstreamCol.Name, upstreamAsset, &upstreamCol); err != nil {
							return err
						}
					}
					continue
				}
			}
			continue
		}

		for _, upstream := range lineageCol.Upstream {
			if upstream.Column == "*" {
				continue
			}

			upstreamAsset := p.Pipeline.GetAssetByName(upstream.Table)
			if upstreamAsset == nil {
				continue
			}
			upstreamCol := upstreamAsset.GetColumnWithName(upstream.Column)
			if upstreamCol == nil {
				upstreamCol = &Column{
					Name:        upstream.Column,
					Type:        upstream.Column,
					Checks:      []ColumnCheck{},
					Description: "function",
					Upstreams: []*UpstreamColumn{
						{
							Asset:  upstreamAsset.Name,
							Column: upstream.Column,
							Table:  upstreamAsset.Name,
						},
					},
				}
			}

			if err := p.addColumnToAsset(asset, lineageCol.Name, upstreamAsset, upstreamCol); err != nil {
				return err
			}
		}
	}
	return nil
}

// addColumnToAsset adds a new column to the asset based on upstream information.
func (p *LineageExtractor) addColumnToAsset(asset *Asset, colName string, upstreamAsset *Asset, upstreamCol *Column) error {
	if asset == nil || upstreamAsset == nil || upstreamCol == nil || colName == "" {
		return errors.New("invalid arguments: all parameters must be non-nil and colName must not be empty")
	}

	if colName == "*" {
		return nil
	}

	col := asset.GetColumnWithName(colName)

	if col != nil {
		newUpstream := &UpstreamColumn{
			Asset:  upstreamAsset.Name,
			Column: upstreamCol.Name,
			Table:  upstreamAsset.Name,
		}

		for i, existing := range asset.Columns {
			if existing.Name == colName {
				asset.Columns[i].Upstreams = append(asset.Columns[i].Upstreams, newUpstream)
				return nil
			}
		}
		return nil
	}

	newCol := Column{}
	newCol.Name = colName
	newCol.PrimaryKey = false
	newCol.Type = upstreamCol.Type
	newCol.Description = upstreamCol.Description
	newCol.UpdateOnMerge = upstreamCol.UpdateOnMerge
	newCol.Checks = []ColumnCheck{}

	newCol.EntityAttribute = upstreamCol.EntityAttribute
	newCol.Upstreams = []*UpstreamColumn{
		{
			Asset:  upstreamAsset.Name,
			Column: upstreamCol.Name,
			Table:  upstreamAsset.Name,
		},
	}

	asset.Columns = append(asset.Columns, newCol)
	return nil
}

// makeColumnMap creates a map of column names to their types from a slice of columns.
func makeColumnMap(columns []Column) map[string]string {
	if len(columns) == 0 {
		return make(map[string]string)
	}

	columnMap := make(map[string]string, len(columns))
	for _, col := range columns {
		if col.Name != "" {
			columnMap[col.Name] = col.Type
		}
	}
	return columnMap
}
