
package data

import (
	"embed"
	"io/fs"
)

//go:embed all:linux-amd64
var _data embed.FS
var Data, _ = fs.Sub(_data, "linux-amd64")
