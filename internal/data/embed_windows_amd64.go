
package data

import (
	"embed"
	"io/fs"
)

//go:embed all:windows-amd64
var _data embed.FS
var Data, _ = fs.Sub(_data, "windows-amd64")
