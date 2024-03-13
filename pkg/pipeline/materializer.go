package pipeline

import (
	"fmt"
	"regexp"
)

type (
	MaterializerFunc        func(task *Asset, query string) (string, error)
	AssetMaterializationMap map[MaterializationType]map[MaterializationStrategy]MaterializerFunc
)

type Materializer struct {
	MaterializationMap AssetMaterializationMap
}

func (m *Materializer) Render(asset *Asset, query string) (string, error) {
	mat := asset.Materialization
	if mat.Type == MaterializationTypeNone {
		return query, nil
	}

	if matFunc, ok := m.MaterializationMap[mat.Type][mat.Strategy]; ok {
		materializedQuery, err := matFunc(asset, query)
		if err != nil {
			return "", err
		}

		return removeComments(materializedQuery), nil
	}

	return "", fmt.Errorf("unsupported materialization type - strategy combination: (`%s` - `%s`)", mat.Type, mat.Strategy)
}

func removeComments(query string) string {
	bytes := []byte(query)
	re := regexp.MustCompile("(?s)/\\*.*?\\*/")
	newBytes := re.ReplaceAll(bytes, nil)
	return string(newBytes)
}
