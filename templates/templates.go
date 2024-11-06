package templates

import (
	"embed"
)

//go:embed *
//go:embed */.bruin.yml
var Templates embed.FS
