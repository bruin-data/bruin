package clickhouse

import (
	"fmt"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"io"
	"strings"
)

// The other packages all use a materializer that renders the query to a single string. Due to the quirks of athena
// we need to create a different materializer that returns a slice of strings, since athena server requires us to send separate batches
// for certain things.
type Materializer struct {
	MaterializationMap AssetMaterializationMap
	fullRefresh        bool
	randomName         func() string
}

func (m *Materializer) Render(asset *pipeline.Asset, query string) ([]string, error) {
	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return []string{query}, nil
	}

	strategy := mat.Strategy
	if m.fullRefresh && mat.Type == pipeline.MaterializationTypeTable {
		if mat.Strategy != pipeline.MaterializationStrategyDDL {
			strategy = pipeline.MaterializationStrategyCreateReplace
		}
	}

	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		return matFunc(asset, query)
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
	queries, err := r.mat.Render(asset, query)
	if err != nil {
		return "", err
	}

	result := strings.Join(queries, ";")
	return result, nil
}

func (m *Materializer) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
	if !m.fullRefresh {
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
