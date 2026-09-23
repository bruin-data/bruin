package clickhouse

import (
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// Materializer returns separate statements because ClickHouse executes each
// step of a materialization independently.
type Materializer struct {
	MaterializationMap AssetMaterializationMap
	fullRefresh        bool
	randomName         func() string
	cluster            string
}

func (m *Materializer) Render(asset *pipeline.Asset, query string) ([]string, error) {
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
		if m.cluster != "" {
			return buildClusterQuery(asset, query, strategy, m.cluster, matFunc)
		}
		return matFunc(asset, query)
	}

	return []string{}, fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

// RenderWithCleanup returns the normal materialization statements together
// with idempotent cleanup statements that the operator should execute if a
// statement fails before the normal cleanup point is reached.
func (m *Materializer) RenderWithCleanup(asset *pipeline.Asset, query string) ([]string, []string, error) {
	queries, err := m.Render(asset, query)
	if err != nil {
		return nil, nil, err
	}

	strategy := asset.Materialization.Strategy
	if asset.FullRefreshEnabled(m.fullRefresh) && asset.Materialization.Type == pipeline.MaterializationTypeTable && strategy != pipeline.MaterializationStrategyDDL {
		strategy = pipeline.MaterializationStrategyCreateReplace
	}

	if strategy != pipeline.MaterializationStrategyMerge && strategy != pipeline.MaterializationStrategyDeleteInsert {
		return queries, nil, nil
	}

	if len(queries) == 0 {
		return queries, nil, nil
	}

	return queries, []string{queries[len(queries)-1]}, nil
}

func NewMaterializer(fullRefresh bool, cluster ...string) *Materializer {
	m := &Materializer{
		MaterializationMap: matMap,
		fullRefresh:        fullRefresh,
		randomName:         helpers.PrefixGenerator,
	}
	if len(cluster) > 0 {
		m.cluster = cluster[0]
	}
	return m
}

type Renderer struct {
	mat *Materializer
}

func NewRenderer(fullRefresh bool, cluster ...string) *Renderer {
	return &Renderer{
		mat: NewMaterializer(fullRefresh, cluster...),
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
