package pipeline

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/dialect"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

type sqlParser interface {
	ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error)
}

type LineageExtractor struct {
	sqlParser sqlParser
	renderer  *jinja.Renderer
	processed map[string]bool
}

// NewLineageExtractor creates a new LineageExtractor instance.
func NewLineageExtractor(parser sqlParser) *LineageExtractor {
	return &LineageExtractor{
		sqlParser: parser,
		renderer:  jinja.NewRendererWithYesterday("lineage-parser", "lineage-parser"),
		processed: make(map[string]bool),
	}
}

// TableSchema extracts the table schema from the assets and stores it in the columnMetadata map.
func (p *LineageExtractor) TableSchema(foundPipeline *Pipeline) sqlparser.Schema {
	columnMetadata := make(sqlparser.Schema)
	for _, foundAsset := range foundPipeline.Assets {
		if len(foundAsset.Columns) > 0 {
			columnMetadata[foundAsset.Name] = makeColumnMap(foundAsset.Columns)
		}
	}
	return columnMetadata
}

// ParseLineageRecursive processes the lineage of an asset and its upstream dependencies recursively.
func (p *LineageExtractor) ColumnLineage(foundPipeline *Pipeline, asset *Asset, metadata sqlparser.Schema) error {
	if asset == nil {
		return nil
	}
	key := fmt.Sprintf("%s-%s", foundPipeline.Name, asset.Name)
	if p.processed[key] {
		return nil
	}

	p.processed[asset.Name] = true

	for _, upstream := range asset.Upstreams {
		upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			continue
		}
		_ = p.ColumnLineage(foundPipeline, upstreamAsset, metadata)
	}

	_ = p.parseLineage(foundPipeline, asset, metadata)

	return nil
}

// ParseLineage analyzes the column lineage for a given asset within a
// It traces column relationships between the asset and its upstream dependencies.
func (p *LineageExtractor) parseLineage(foundPipeline *Pipeline, asset *Asset, metadata sqlparser.Schema) error {
	if asset == nil {
		return errors.New("invalid arguments: asset and pipeline cannot be nil")
	}

	dialect, err := dialect.GetDialectByAssetType(string(asset.Type))
	if err != nil {
		return nil //nolint:nilerr
	}

	for _, upstream := range asset.Upstreams {
		upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			return fmt.Errorf("upstream asset not found: %s", upstream.Value)
		}
	}

	filteredMetadata := make(sqlparser.Schema)
	for _, upstream := range asset.Upstreams {
		upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			continue
		}
		filteredMetadata[upstreamAsset.Name] = metadata[upstreamAsset.Name]
	}

	query, err := p.renderer.Render(asset.ExecutableFile.Content)
	if err != nil {
		return fmt.Errorf("failed to render the query: %w", err)
	}

	lineage, err := p.sqlParser.ColumnLineage(query, dialect, filteredMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse column lineage: %w", err)
	}

	return p.processLineageColumns(foundPipeline, asset, lineage)
}

func (p *LineageExtractor) processLineageColumns(foundPipeline *Pipeline, asset *Asset, lineage *sqlparser.Lineage) error {
	if lineage == nil {
		return nil
	}

	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	upstreams := make([]Upstream, 0)
	for _, up := range asset.Upstreams {
		upstream := up
		lineage.NonSelectedColumns = append(lineage.NonSelectedColumns, lineage.Columns...)
		dict := map[string]bool{}
		for _, lineageCol := range lineage.NonSelectedColumns {
			for _, lineageUpstream := range lineageCol.Upstream {
				key := fmt.Sprintf("%s-%s", strings.ToLower(lineageUpstream.Table), strings.ToLower(lineageCol.Name))
				if _, ok := dict[key]; !ok {
					if strings.EqualFold(lineageUpstream.Table, up.Value) {
						exists := false
						for _, col := range upstream.Columns {
							if strings.EqualFold(col.Name, lineageCol.Name) {
								exists = true
								break
							}
						}
						if !exists {
							upstream.Columns = append(upstream.Columns, DependsColumn{
								Name: lineageCol.Name,
							})
						}
						dict[key] = true
					}
				}
			}
		}
		upstreams = append(upstreams, upstream)
	}
	asset.Upstreams = upstreams

	for _, lineageCol := range lineage.Columns {
		if lineageCol.Name == "*" {
			for _, upstream := range lineageCol.Upstream {
				upstreamAsset := foundPipeline.GetAssetByName(upstream.Table)
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

		if len(lineageCol.Upstream) == 0 {
			if err := p.addColumnToAsset(asset, lineageCol.Name, nil, &Column{
				Name:      lineageCol.Name,
				Type:      lineageCol.Type,
				Checks:    []ColumnCheck{},
				Upstreams: []*UpstreamColumn{},
			}); err != nil {
				return err
			}
			continue
		}

		for _, upstream := range lineageCol.Upstream {
			if upstream.Column == "*" {
				continue
			}
			if upstream.Table == asset.Name {
				continue
			}
			upstreamAsset := foundPipeline.GetAssetByName(upstream.Table)
			if upstreamAsset == nil {
				if err := p.addColumnToAsset(asset, lineageCol.Name, nil, &Column{
					Name:   upstream.Column,
					Type:   lineageCol.Type,
					Checks: []ColumnCheck{},
					Upstreams: []*UpstreamColumn{
						{
							Column: upstream.Column,
							Table:  strings.ToLower(upstream.Table),
						},
					},
				}); err != nil {
					return err
				}
				continue
			}

			upstreamCol := upstreamAsset.GetColumnWithName(upstream.Column)
			if upstreamCol == nil {
				upstreamCol = &Column{
					Name:   upstream.Column,
					Type:   lineageCol.Type,
					Checks: []ColumnCheck{},
					Upstreams: []*UpstreamColumn{
						{
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
	if asset == nil || upstreamCol == nil || colName == "" {
		return errors.New("invalid arguments: all parameters must be non-nil and colName must not be empty")
	}

	if colName == "*" {
		return nil
	}

	newCol := Column{
		Name:       colName,
		PrimaryKey: false,
		Type:       upstreamCol.Type,
		Checks:     []ColumnCheck{},
		// Description:     upstreamCol.Description,
		EntityAttribute: upstreamCol.EntityAttribute,
		Upstreams:       []*UpstreamColumn{},
		UpdateOnMerge:   upstreamCol.UpdateOnMerge,
	}

	if upstreamAsset == nil {
		newCol = *upstreamCol
	}

	col := asset.GetColumnWithName(colName)

	if col != nil {
		if upstreamAsset != nil {
			newUpstream := UpstreamColumn{
				Column: upstreamCol.Name,
				Table:  upstreamAsset.Name,
			}
			for i, existing := range asset.Columns {
				if strings.EqualFold(existing.Name, colName) {
					exists := false
					for _, existingUpstream := range asset.Columns[i].Upstreams {
						if strings.EqualFold(existingUpstream.Column, newUpstream.Column) &&
							strings.EqualFold(existingUpstream.Table, newUpstream.Table) {
							exists = true
							break
						}
					}
					if !exists {
						asset.Columns[i].Upstreams = append(asset.Columns[i].Upstreams, &newUpstream)
					}
					return nil
				}
			}
		}
		return nil
	}

	if upstreamAsset != nil {
		newCol.Upstreams = append(newCol.Upstreams, &UpstreamColumn{
			Column: upstreamCol.Name,
			Table:  upstreamAsset.Name,
		})
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
