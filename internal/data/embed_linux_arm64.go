
package data

import (
	"embed"
	"io/fs"
)

//go:embed all:linux-arm64
var _data embed.FS
var Data, _ = fs.Sub(_data, "linux-arm64")
