package ingestion

import (
	"embed"
)

//go:embed *.md
var DocsFS embed.FS
