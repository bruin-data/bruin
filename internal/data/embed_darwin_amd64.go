
package data

import (
	"embed"
	"io/fs"
)

//go:embed all:darwin-amd64
var _data embed.FS
var Data, _ = fs.Sub(_data, "darwin-amd64")
