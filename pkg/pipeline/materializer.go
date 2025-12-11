package pipeline

import (
	"fmt"
	"io"
	"regexp"

	"github.com/pkg/errors"
)

var commentRegex = regexp.MustCompile(`/\* *@bruin[\s\w\S]*@bruin *\*/`)

type (
	MaterializerFunc        func(task *Asset, query string) (string, error)
	AssetMaterializationMap map[MaterializationType]map[MaterializationStrategy]MaterializerFunc
)

type Materializer struct {
	MaterializationMap AssetMaterializationMap
	FullRefresh        bool
}

func (m *Materializer) Render(asset *Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.Type == MaterializationTypeNone {
		return removeComments(query), nil
	}

	strategy := mat.Strategy
	if m.FullRefresh && mat.Type == MaterializationTypeTable {
		// Only override to CreateReplace if strategy is not explicitly set to DDL
		// This strategy should never be overridden, even with full refresh
		if mat.Strategy != MaterializationStrategyDDL {
			strategy = MaterializationStrategyCreateReplace
		}
	}

	if matFunc, ok := m.MaterializationMap[mat.Type][strategy]; ok {
		materializedQuery, err := matFunc(asset, query)
		if err != nil {
			return "", err
		}

		return removeComments(materializedQuery), nil
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func removeComments(query string) string {
	return commentRegex.ReplaceAllString(query, "")
}

func (m *Materializer) IsFullRefresh() bool {
	return m.FullRefresh
}

func (m *Materializer) LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error {
	if !m.FullRefresh {
		return nil
	}

	if asset.Materialization.Strategy != MaterializationStrategyDDL {
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
