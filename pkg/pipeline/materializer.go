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
		// Also respect refresh_restricted flag - if true, don't drop/recreate the table
		if mat.Strategy != MaterializationStrategyDDL && (asset.RefreshRestricted == nil || !*asset.RefreshRestricted) {
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

// HookWrapperMaterializer decorates a string-based materializer by wrapping the rendered SQL
// with asset hooks, while forwarding optional behaviors when the base materializer supports them.
type HookWrapperMaterializer struct {
	Mat interface {
		Render(asset *Asset, query string) (string, error)
	}
}

func (m HookWrapperMaterializer) Render(asset *Asset, query string) (string, error) {
	if m.Mat == nil {
		return "", errors.New("hook wrapper materializer requires a base materializer")
	}

	materialized, err := m.Mat.Render(asset, query)
	if err != nil {
		return "", err
	}

	return WrapHooks(materialized, asset.Hooks), nil
}

func (m HookWrapperMaterializer) LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error {
	if m.Mat == nil {
		return errors.New("hook wrapper materializer requires a base materializer")
	}

	logger, ok := m.Mat.(interface {
		LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error
	})
	if !ok {
		return nil
	}

	return logger.LogIfFullRefreshAndDDL(writer, asset)
}

func (m HookWrapperMaterializer) IsFullRefresh() bool {
	if m.Mat == nil {
		return false
	}

	fullRefresh, ok := m.Mat.(interface {
		IsFullRefresh() bool
	})
	if !ok {
		return false
	}

	return fullRefresh.IsFullRefresh()
}

// HookWrapperMaterializerList decorates list-based materializers by injecting hook queries
// before and after the materialized statements.
type HookWrapperMaterializerList struct {
	Mat interface {
		Render(asset *Asset, query string) ([]string, error)
	}
}

func (m HookWrapperMaterializerList) Render(asset *Asset, query string) ([]string, error) {
	if m.Mat == nil {
		return nil, errors.New("hook wrapper materializer requires a base materializer")
	}

	materialized, err := m.Mat.Render(asset, query)
	if err != nil {
		return nil, err
	}

	return wrapHookQueriesList(materialized, asset.Hooks), nil
}

func (m HookWrapperMaterializerList) LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error {
	if m.Mat == nil {
		return errors.New("hook wrapper materializer requires a base materializer")
	}

	logger, ok := m.Mat.(interface {
		LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error
	})
	if !ok {
		return nil
	}

	return logger.LogIfFullRefreshAndDDL(writer, asset)
}

// HookWrapperMaterializerListWithLocation decorates list-based materializers that require a location
// parameter by injecting hook queries before and after the materialized statements.
type HookWrapperMaterializerListWithLocation struct {
	Mat interface {
		Render(asset *Asset, query, location string) ([]string, error)
	}
}

func (m HookWrapperMaterializerListWithLocation) Render(asset *Asset, query, location string) ([]string, error) {
	if m.Mat == nil {
		return nil, errors.New("hook wrapper materializer requires a base materializer")
	}

	materialized, err := m.Mat.Render(asset, query, location)
	if err != nil {
		return nil, err
	}

	return wrapHookQueriesList(materialized, asset.Hooks), nil
}

func (m HookWrapperMaterializerListWithLocation) LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error {
	if m.Mat == nil {
		return errors.New("hook wrapper materializer requires a base materializer")
	}

	logger, ok := m.Mat.(interface {
		LogIfFullRefreshAndDDL(writer interface{}, asset *Asset) error
	})
	if !ok {
		return nil
	}

	return logger.LogIfFullRefreshAndDDL(writer, asset)
}
