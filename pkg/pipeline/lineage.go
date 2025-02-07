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
}

// NewLineageExtractor creates a new LineageExtractor instance.
func NewLineageExtractor(parser sqlParser) *LineageExtractor {
	return &LineageExtractor{
		sqlParser: parser,
		renderer:  jinja.NewRendererWithYesterday("lineage-parser", "lineage-parser"),
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

// TableSchemaForUpstreams extracts the table schema for a single asset and returns a sqlparser schema only for its upstreams.
func (p *LineageExtractor) TableSchemaForUpstreams(foundPipeline *Pipeline, asset *Asset) sqlparser.Schema {
	columnMetadata := make(sqlparser.Schema)
	for _, upstream := range asset.Upstreams {
		if upstream.Type != "asset" {
			continue
		}

		upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
		if len(upstreamAsset.Columns) > 0 {
			columnMetadata[upstreamAsset.Name] = makeColumnMap(upstreamAsset.Columns)
		}
	}
	return columnMetadata
}

// ColumnLineage processes the lineage of an asset and its upstream dependencies recursively.
func (p *LineageExtractor) ColumnLineage(foundPipeline *Pipeline, asset *Asset, processedAssets map[string]bool) error {
	if asset == nil {
		return nil
	}
	if processedAssets[asset.Name] {
		return nil
	}

	processedAssets[asset.Name] = true

	for _, upstream := range asset.Upstreams {
		upstreamAsset := foundPipeline.GetAssetByName(upstream.Value)
		if upstreamAsset == nil {
			continue
		}
		_ = p.ColumnLineage(foundPipeline, upstreamAsset, processedAssets)
	}

	_ = p.parseLineage(foundPipeline, asset, p.TableSchemaForUpstreams(foundPipeline, asset))

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

	query, err := p.renderer.Render(asset.ExecutableFile.Content)
	if err != nil {
		return fmt.Errorf("failed to render the query: %w", err)
	}

	lineage, err := p.sqlParser.ColumnLineage(query, dialect, metadata)
	if err != nil {
		return fmt.Errorf("failed to parse column lineage: %w", err)
	}

	return p.processLineageColumns(foundPipeline, asset, lineage)
}

func (p *LineageExtractor) mergeAsteriskColumns(foundPipeline *Pipeline, asset *Asset, lineageCol sqlparser.ColumnLineage) error {
	for _, upstream := range lineageCol.Upstream {
		upstreamAsset := foundPipeline.GetAssetByName(strings.ToLower(upstream.Table))
		if upstreamAsset == nil {
			return nil
		}

		// If upstream column is *, copy all columns from upstream asset
		if upstream.Column == "*" {
			for _, upstreamCol := range upstreamAsset.Columns {
				if err := p.addColumnToAsset(asset, upstreamCol.Name, upstreamAsset, &upstreamCol); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return nil
}

func (p *LineageExtractor) mergeNonSelectedColumns(asset *Asset, lineage *sqlparser.Lineage) []Upstream {
	upstreams := make([]Upstream, 0)
	for _, up := range asset.Upstreams {
		processedColumns := make(map[string]bool)

		// Helper function to process columns
		processColumn := func(table, column string) {
			key := fmt.Sprintf("%s-%s", strings.ToLower(table), strings.ToLower(column))
			if !processedColumns[key] && strings.EqualFold(table, up.Value) {
				processedColumns[key] = true
				up.Columns = append(up.Columns, DependsColumn{
					Name: column,
				})
			}
		}

		for _, lineageCol := range lineage.NonSelectedColumns {
			for _, lineageUpstream := range lineageCol.Upstream {
				processColumn(lineageUpstream.Table, lineageCol.Name)
			}
		}

		for _, col := range lineage.Columns {
			for _, colUpstream := range col.Upstream {
				processColumn(colUpstream.Table, colUpstream.Column)
			}
		}

		upstreams = append(upstreams, up)
	}
	return upstreams
}

func (p *LineageExtractor) processLineageColumns(foundPipeline *Pipeline, asset *Asset, lineage *sqlparser.Lineage) error {
	if lineage == nil {
		return nil
	}

	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	asset.Upstreams = p.mergeNonSelectedColumns(asset, lineage)

	for _, lineageCol := range lineage.Columns {
		if lineageCol.Name == "*" {
			err := p.mergeAsteriskColumns(foundPipeline, asset, lineageCol)
			if err != nil {
				return err
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

			upstreamAsset := foundPipeline.GetAssetByName(strings.ToLower(upstream.Table))
			if upstreamAsset == nil {
				if err := p.addColumnToAsset(asset, lineageCol.Name, nil, &Column{
					Name:   lineageCol.Name,
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
					Name:   lineageCol.Name,
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

func (p *LineageExtractor) addColumnToAsset(asset *Asset, colName string, upstreamAsset *Asset, upstreamCol *Column) error {
	if err := validateInputs(asset, colName); err != nil {
		return err
	}

	existingCol := asset.GetColumnWithName(colName)

	// Handle upstream columns first
	if len(upstreamCol.Upstreams) > 0 {
		return p.handleUpstreamColumns(asset, colName, upstreamAsset, upstreamCol, existingCol)
	}

	// Handle direct column
	newUpstream := createUpstreamColumn(upstreamCol.Name, upstreamAsset)
	return p.handleDirectColumn(asset, colName, upstreamAsset, upstreamCol, existingCol, newUpstream)
}

func validateInputs(asset *Asset, colName string) error {
	if asset == nil || colName == "" {
		return errors.New("invalid arguments: all parameters must be non-nil and colName must not be empty")
	}
	if colName == "*" {
		return nil
	}
	return nil
}

func (p *LineageExtractor) handleUpstreamColumns(asset *Asset, colName string, upstreamAsset *Asset, upstreamCol *Column, existingCol *Column) error {
	for _, upstream := range upstreamCol.Upstreams {
		newUpstream := createUpstreamColumn(upstream.Column, &Asset{Name: upstream.Table})

		if upstreamAsset == nil {
			if err := p.handleNilUpstreamAsset(asset, existingCol, upstreamCol, newUpstream); err != nil {
				return err
			}
			continue
		}

		if err := p.handleExistingOrNewColumn(asset, colName, upstreamCol, existingCol, newUpstream); err != nil {
			return err
		}
	}
	return nil
}

func (p *LineageExtractor) handleDirectColumn(asset *Asset, colName string, upstreamAsset *Asset, upstreamCol *Column, existingCol *Column, newUpstream *UpstreamColumn) error {
	if upstreamAsset == nil {
		return p.handleNilUpstreamAsset(asset, existingCol, upstreamCol, newUpstream)
	}
	return p.handleExistingOrNewColumn(asset, colName, upstreamCol, existingCol, newUpstream)
}

func (p *LineageExtractor) handleNilUpstreamAsset(asset *Asset, existingCol *Column, upstreamCol *Column, newUpstream *UpstreamColumn) error {
	if existingCol == nil {
		upstreamCol.Upstreams = []*UpstreamColumn{newUpstream}
		asset.Columns = append(asset.Columns, *upstreamCol)
		return nil
	}
	existingCol.Upstreams = []*UpstreamColumn{newUpstream}
	p.updateAssetColumn(asset, existingCol)
	return nil
}

func (p *LineageExtractor) handleExistingOrNewColumn(asset *Asset, colName string, upstreamCol *Column, existingCol *Column, newUpstream *UpstreamColumn) error {
	if existingCol != nil {
		updateExistingColumn(existingCol, upstreamCol)
		if !upstreamExists(existingCol.Upstreams, *newUpstream) {
			existingCol.Upstreams = append(existingCol.Upstreams, newUpstream)
		}
		p.updateAssetColumn(asset, existingCol)
		return nil
	}

	newCol := createNewColumn(colName, upstreamCol, newUpstream)
	asset.Columns = append(asset.Columns, *newCol)
	return nil
}

func createUpstreamColumn(columnName string, sourceAsset *Asset) *UpstreamColumn {
	upstream := &UpstreamColumn{
		Column: columnName,
	}
	if sourceAsset != nil {
		upstream.Table = sourceAsset.Name
	}
	return upstream
}

func updateExistingColumn(existingCol *Column, upstreamCol *Column) {
	if len(existingCol.Description) == 0 {
		existingCol.Description = upstreamCol.Description
	}
	if len(existingCol.Type) == 0 {
		existingCol.Type = upstreamCol.Type
	}
	if existingCol.EntityAttribute == nil {
		existingCol.EntityAttribute = upstreamCol.EntityAttribute
	}
	existingCol.UpdateOnMerge = upstreamCol.UpdateOnMerge
}

func createNewColumn(colName string, upstreamCol *Column, newUpstream *UpstreamColumn) *Column {
	return &Column{
		Name:            colName,
		PrimaryKey:      false,
		Type:            upstreamCol.Type,
		Checks:          []ColumnCheck{},
		Description:     upstreamCol.Description,
		EntityAttribute: upstreamCol.EntityAttribute,
		UpdateOnMerge:   upstreamCol.UpdateOnMerge,
		Upstreams:       []*UpstreamColumn{newUpstream},
	}
}

func (p *LineageExtractor) updateAssetColumn(asset *Asset, col *Column) {
	for key, assetCol := range asset.Columns {
		if strings.EqualFold(assetCol.Name, col.Name) {
			asset.Columns[key] = *col
			break
		}
	}
}

func upstreamExists(upstreams []*UpstreamColumn, newUpstream UpstreamColumn) bool {
	for _, existing := range upstreams {
		if strings.EqualFold(existing.Column, newUpstream.Column) &&
			strings.EqualFold(existing.Table, newUpstream.Table) {
			return true
		}
	}
	return false
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
