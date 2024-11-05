package templates

import (
	"embed"
)

//go:embed *
//go:embed duckdb/.bruin.yml
var Templates embed.FS
