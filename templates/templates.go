package templates

import (
	"embed"
	_ "embed"
)

//go:embed *
var Templates embed.FS
