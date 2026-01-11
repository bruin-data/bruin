package docs

import "embed"

//go:embed all:getting-started all:commands all:quality all:secrets all:deployment all:cicd all:cloud all:vscode-extension all:ingestion all:platforms all:assets *.md
var DocsFS embed.FS
