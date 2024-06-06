package python_src

import (
	"embed"
)

//go:embed all:*
var RendererSource embed.FS
