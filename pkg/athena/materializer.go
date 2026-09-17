package athena

import (
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// The other packages all use a materializer that renders the query to a single string. Due to the quirks of athena
// we need to create a different materializer that returns a slice of strings, since athena server requires us to send separate batches
// for certain things.
type Materializer struct {
	MaterializationMap AssetMaterializationMap
	fullRefresh        bool
	randomName         func() string
}

func (m *Materializer) Render(asset *pipeline.Asset, query, location string) ([]string, error) {
	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return []string{query}, nil
	}

	strategy := mat.Strategy
	if asset.FullRefreshEnabled(m.fullRefresh) && mat.Type == pipeline.MaterializationTypeTable {
		if mat.Strategy != pipeline.MaterializationStrategyDDL {
			strategy = pipeline.MaterializationStrategyCreateReplace
		}
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		return matFunc(asset, query, location)
	}

	return []string{}, fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

// RenderInitialTable builds the CTAS statement used when an incremental target
// is absent. Athena CTAS does not support IF NOT EXISTS, so the operator calls
// this only after checking the Glue catalog through information_schema.
func (m *Materializer) RenderInitialTable(asset *pipeline.Asset, query, location string) ([]string, error) {
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")

	partitionBy := ""
	if asset.Materialization.PartitionBy != "" {
		partitionBy = fmt.Sprintf(", partitioning = ARRAY['%s']", asset.Materialization.PartitionBy)
	}

	return []string{fmt.Sprintf(
		"CREATE TABLE %s WITH (table_type='ICEBERG', is_external=false, location='%s/%s'%s) AS %s WITH NO DATA",
		asset.Name,
		location,
		asset.Name,
		partitionBy,
		query,
	)}, nil
}

func NewMaterializer(fullRefresh bool) *Materializer {
	return &Materializer{
		MaterializationMap: matMap,
		fullRefresh:        fullRefresh,
		randomName:         helpers.PrefixGenerator,
	}
}

func (m *Materializer) IsFullRefresh() bool {
	return m.fullRefresh
}

type Renderer struct {
	mat      *Materializer
	location string
}

func NewRenderer(fullRefresh bool, location string) *Renderer {
	return &Renderer{
		mat:      NewMaterializer(fullRefresh),
		location: location,
	}
}

func (r *Renderer) Render(asset *pipeline.Asset, query string) (string, error) {
	queries, err := r.mat.Render(asset, query, r.location)
	if err != nil {
		return "", err
	}

	result := strings.Join(queries, ";")
	return result, nil
}

func (m *Materializer) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
	if !asset.FullRefreshEnabled(m.fullRefresh) {
		return nil
	}

	if asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
		return nil
	}
	if writer == nil {
		return errors.New("no writer found in context, please create an issue for this: https://github.com/bruin-data/bruin/issues")
	}
	message := "Full refresh detected, but DDL strategy is in use — table will NOT be dropped or recreated.\n"
	writerObj, ok := writer.(io.Writer)
	if !ok {
		return errors.New("writer is not an io.Writer")
	}
	_, _ = writerObj.Write([]byte(message))

	return nil
}
