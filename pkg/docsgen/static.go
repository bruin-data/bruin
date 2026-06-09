package docsgen

import "embed"

// The embedded files below are built from pkg/docsgen/web by esbuild. Regenerate
// them after editing the web sources with `make docs-app` (or `npm run
// docs:app:build`); CI verifies the committed bundle matches the sources.
//
//go:generate sh -c "cd ../.. && npm run docs:app:build"

//go:embed static/app.min.js static/style.min.css
var staticFiles embed.FS
