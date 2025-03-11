package lineage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/dialect"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

type sqlParser interface {
	ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error)
}

type LineageError struct {
	Pipeline *pipeline.Pipeline
	Issues   []*LineageIssue
}

type LineageIssue struct {
	Task        *pipeline.Asset
	Description string
	Context     []string
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
func (p *LineageExtractor) TableSchema(foundPipeline *pipeline.Pipeline) sqlparser.Schema {
	columnMetadata := make(sqlparser.Schema)
	for _, foundAsset := range foundPipeline.Assets {
		if len(foundAsset.Columns) > 0 {
			columnMetadata[foundAsset.Name] = makeColumnMap(foundAsset.Columns)
		}
	}
	return columnMetadata
}

// TableSchemaForUpstreams extracts the table schema for a single asset and returns a sqlparser schema only for its upstreams.
func (p *LineageExtractor) TableSchemaForUpstreams(foundPipeline *pipeline.Pipeline, asset *pipeline.Asset) sqlparser.Schema {
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
func (p *LineageExtractor) ColumnLineage(foundPipeline *pipeline.Pipeline, asset *pipeline.Asset, processedAssets map[string]bool) *LineageError {
	issues := LineageError{
		Issues: []*LineageIssue{},
	}

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
		issues.Issues = append(issues.Issues, p.ColumnLineage(foundPipeline, upstreamAsset, processedAssets).Issues...)
	}

	err := p.parseLineage(foundPipeline, asset, p.TableSchemaForUpstreams(foundPipeline, asset))
	if err != nil {
		issues.Issues = append(issues.Issues, &LineageIssue{
			Task:        asset,
			Description: err.Error(),
			Context: []string{
				asset.ExecutableFile.Content,
			},
		})
	}

	return &issues
}

// ParseLineage analyzes the column lineage for a given asset within a
// It traces column relationships between the asset and its upstream dependencies.
func (p *LineageExtractor) parseLineage(foundPipeline *pipeline.Pipeline, asset *pipeline.Asset, metadata sqlparser.Schema) error {
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

	if len(lineage.Errors) > 0 {
		return fmt.Errorf("failed to parse column lineage: %s", strings.Join(lineage.Errors, ", "))
	}

	return p.processLineageColumns(foundPipeline, asset, lineage)
}

func (p *LineageExtractor) mergeAsteriskColumns(foundPipeline *pipeline.Pipeline, asset *pipeline.Asset, lineageCol sqlparser.ColumnLineage) error {
	for _, upstream := range lineageCol.Upstream {
		upstreamAsset := foundPipeline.GetAssetByNameCaseInsensitive(upstream.Table)
		if upstreamAsset == nil {
			return nil
		}

		// If upstream column is *, copy all columns from upstream asset
		if upstream.Column == "*" {
			for _, upstreamCol := range upstreamAsset.Columns {
				upstreamCol.PrimaryKey = false
				upstreamCol.Checks = []pipeline.ColumnCheck{}
				if err := p.addColumnToAsset(asset, upstreamCol.Name, &upstreamCol); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return nil
}

func (p *LineageExtractor) mergeNonSelectedColumns(asset *pipeline.Asset, lineage *sqlparser.Lineage) []pipeline.Upstream {
	upstreams := make([]pipeline.Upstream, 0)
	for _, up := range asset.Upstreams {
		processedColumns := make(map[string]bool)

		// Helper function to process columns
		processColumn := func(table, column string) {
			key := fmt.Sprintf("%s-%s", strings.ToLower(table), strings.ToLower(column))
			if !processedColumns[key] && strings.EqualFold(table, up.Value) {
				// Check if column already exists in up.Columns
				columnExists := false
				for _, existingCol := range up.Columns {
					if strings.EqualFold(existingCol.Name, column) {
						columnExists = true
						break
					}
				}

				if !columnExists {
					processedColumns[key] = true
					up.Columns = append(up.Columns, pipeline.DependsColumn{
						Name: column,
					})
				}
			}
		}

		for _, lineageCol := range lineage.NonSelectedColumns {
			if lineageCol.Name == "*" {
				continue
			}
			for _, lineageUpstream := range lineageCol.Upstream {
				processColumn(lineageUpstream.Table, lineageCol.Name)
			}
		}

		for _, col := range lineage.Columns {
			if col.Name != "*" {
				for _, colUpstream := range col.Upstream {
					processColumn(colUpstream.Table, colUpstream.Column)
				}
				continue
			}
			for _, colUpstream := range asset.Columns {
				processColumn(colUpstream.Name, colUpstream.Name)
			}
		}

		upstreams = append(upstreams, up)
	}
	return upstreams
}

func (p *LineageExtractor) processLineageColumns(foundPipeline *pipeline.Pipeline, asset *pipeline.Asset, lineage *sqlparser.Lineage) error {
	if lineage == nil {
		return nil
	}

	if asset == nil {
		return errors.New("asset cannot be nil")
	}

	for _, lineageCol := range lineage.Columns {
		if lineageCol.Name == "*" {
			err := p.mergeAsteriskColumns(foundPipeline, asset, lineageCol)
			if err != nil {
				return err
			}
			continue
		}

		if len(lineageCol.Upstream) == 0 {
			if err := p.addColumnToAsset(asset, lineageCol.Name, &pipeline.Column{
				Name:       lineageCol.Name,
				Type:       lineageCol.Type,
				PrimaryKey: false,
				Checks:     []pipeline.ColumnCheck{},
				Upstreams:  []*pipeline.UpstreamColumn{},
			}); err != nil {
				return err
			}
			continue
		}

		for _, upstream := range lineageCol.Upstream {
			if upstream.Column == "*" {
				continue
			}

			upstreamAsset := foundPipeline.GetAssetByNameCaseInsensitive(upstream.Table)
			if upstreamAsset == nil {
				if err := p.addColumnToAsset(asset, lineageCol.Name, &pipeline.Column{
					Name:       lineageCol.Name,
					Type:       lineageCol.Type,
					PrimaryKey: false,
					Checks:     []pipeline.ColumnCheck{},
					Upstreams: []*pipeline.UpstreamColumn{
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
				upstreamCol = &pipeline.Column{
					Name:       lineageCol.Name,
					Type:       lineageCol.Type,
					PrimaryKey: false,
					Checks:     []pipeline.ColumnCheck{},
					Upstreams: []*pipeline.UpstreamColumn{
						{
							Column: upstream.Column,
							Table:  upstreamAsset.Name,
						},
					},
				}
			} else {
				upstreamCol.Name = lineageCol.Name
				upstreamCol.PrimaryKey = false
				upstreamCol.Checks = []pipeline.ColumnCheck{}
				upstreamCol.Upstreams = []*pipeline.UpstreamColumn{
					{
						Column: upstream.Column,
						Table:  upstreamAsset.Name,
					},
				}
			}

			if err := p.addColumnToAsset(asset, lineageCol.Name, upstreamCol); err != nil {
				return err
			}
		}
	}

	asset.Upstreams = p.mergeNonSelectedColumns(asset, lineage)
	return nil
}

func (p *LineageExtractor) addColumnToAsset(asset *pipeline.Asset, colName string, upstreamCol *pipeline.Column) error {
	if err := validateInputs(asset, colName); err != nil {
		return err
	}

	return p.handleUpstreamColumns(asset, upstreamCol)
}

func validateInputs(asset *pipeline.Asset, colName string) error {
	if asset == nil || colName == "" {
		return errors.New("invalid arguments: all parameters must be non-nil and colName must not be empty")
	}
	if colName == "*" {
		return nil
	}
	return nil
}

func (p *LineageExtractor) handleUpstreamColumns(asset *pipeline.Asset, upstreamCol *pipeline.Column) error {
	existingCol := asset.GetColumnWithName(upstreamCol.Name)

	if existingCol == nil {
		asset.Columns = append(asset.Columns, *upstreamCol)
		return nil
	}

	if err := p.handleExistingOrNewColumn(asset, upstreamCol, existingCol); err != nil {
		return err
	}
	return nil
}

func (p *LineageExtractor) handleExistingOrNewColumn(asset *pipeline.Asset, upstreamCol *pipeline.Column, existingCol *pipeline.Column) error {
	updateExistingColumn(existingCol, upstreamCol)
	for _, upstream := range upstreamCol.Upstreams {
		if !upstreamExists(existingCol.Upstreams, upstream) {
			existingCol.Upstreams = append(existingCol.Upstreams, upstream)
		}
	}

	p.updateAssetColumn(asset, existingCol)
	return nil
}

func updateExistingColumn(existingCol *pipeline.Column, upstreamCol *pipeline.Column) {
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
	existingCol.PrimaryKey = false
}

func (p *LineageExtractor) updateAssetColumn(asset *pipeline.Asset, col *pipeline.Column) {
	for key, assetCol := range asset.Columns {
		if strings.EqualFold(assetCol.Name, col.Name) {
			asset.Columns[key] = *col
			break
		}
	}
}

func upstreamExists(upstreams []*pipeline.UpstreamColumn, newUpstream *pipeline.UpstreamColumn) bool {
	for _, existing := range upstreams {
		if strings.EqualFold(existing.Column, newUpstream.Column) &&
			strings.EqualFold(existing.Table, newUpstream.Table) {
			return true
		}
	}
	return false
}

// makeColumnMap creates a map of column names to their types from a slice of columns.
func makeColumnMap(columns []pipeline.Column) map[string]string {
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
