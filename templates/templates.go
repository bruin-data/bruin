package templates

import (
	"embed"
)

//go:embed *
//go:embed */.bruin.yml
var Templates embed.FS

func TemplateNames() []string {
	dirs, err := Templates.ReadDir(".")
	if err != nil {
		return []string{}
	}

	dirsNames := []string{}
	for _, dir := range dirs {
		dirsNames = append(dirsNames, dir.Name())
	}
	return dirsNames
}
