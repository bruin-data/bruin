package jinja

import "github.com/bruin-data/bruin/pkg/pipeline"

// VariantRendererFactory satisfies pipeline.VariantRendererFactory using a
// fresh Jinja renderer per call. Pass it into pipeline.NewBuilder so
// WithVariant / CreatePipelinesFromPath can materialize variants without
// callers having to construct a renderer.
//
// The renderer's context exposes only `var` (the merged variable values) and
// `variant` (the variant name). Run-time variables like start_date / end_date
// / this are deliberately absent — variant materialization is only meant to
// resolve identity and structural fields.
func VariantRendererFactory(vars map[string]any, variantName string) pipeline.RenderFunc {
	r := NewRenderer(Context{
		"var":     vars,
		"variant": variantName,
	})
	return r.Render
}
