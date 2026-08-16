package templates

import (
	"embed"
)

//go:embed *
//go:embed */.bruin.yml
//go:embed migration-fivetran/.gitignore
//go:embed stripe-bigquery/.gitignore
//go:embed google-web-analytics/.gitignore
//go:embed migration-fivetran/.agents/skills/bruin-fivetran-migrator/*
var Templates embed.FS
