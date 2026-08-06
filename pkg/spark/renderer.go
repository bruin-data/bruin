package spark

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

// Renderer applies Spark's multi-statement execution semantics to an
// already-rendered script before materializing its main query.
type Renderer struct {
	materializer pipeline.HookWrapperMaterializer
}

func NewRenderer(fullRefresh bool) *Renderer {
	return &Renderer{
		materializer: pipeline.HookWrapperMaterializer{
			Mat: NewMaterializer(fullRefresh),
		},
	}
}

func (r *Renderer) Render(asset *pipeline.Asset, script string) (string, error) {
	queries := query.SplitQueriesPreservingSessionStatements(script)
	if len(queries) == 0 {
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
			return "", nil
		}
		queries = []*query.Query{{Query: ""}}
	}
	return renderSparkQueries(asset, queries, r.materializer)
}

// WrapsHooks tells command renderers that Spark has already placed hooks
// relative to session statements using the same ordering as runtime execution.
func (r *Renderer) WrapsHooks() bool {
	return true
}
