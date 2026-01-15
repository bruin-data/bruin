package docs

import "embed"

// Only embed markdown files - media files (png, gif, jpg, mp4) are not needed
// and were causing ~34MB of unnecessary binary bloat.
// Patterns cover: root level, 1 deep, 2 deep, and 3 deep directories.
//
//go:embed *.md */*.md */*/*.md */*/*/*.md
var DocsFS embed.FS
