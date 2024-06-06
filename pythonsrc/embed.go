package pythonsrc

import (
	"embed"
)

//go:embed all:*
var RendererSource embed.FS
