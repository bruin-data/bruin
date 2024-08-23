package athena

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
	if m.fullRefresh && mat.Type == pipeline.MaterializationTypeTable {
		strategy = pipeline.MaterializationStrategyCreateReplace
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		return matFunc(asset, query, location)
	}

	return []string{}, fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func NewMaterializer(fullRefresh bool) *Materializer {
	return &Materializer{
		MaterializationMap: matMap,
		fullRefresh:        fullRefresh,
		randomName:         helpers.PrefixGenerator,
	}
}

type Renderer struct {
	mat *Materializer
}

func NewRenderer(fullRefresh bool) *Renderer {
	return &Renderer{
		mat: NewMaterializer(fullRefresh),
	}
}

func (r *Renderer) Render(asset *pipeline.Asset, query string) (string, error) {
	queries, err := r.mat.Render(asset, query, "s3://{output bucket}")
	if err != nil {
		return "", err
	}

	result := strings.Join(queries, ";")
	return result, nil
}
