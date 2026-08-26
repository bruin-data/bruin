package templates

import (
	"embed"
)

//go:embed *
//go:embed */.bruin.yml
//go:embed migration-fivetran/.gitignore
//go:embed stripe-bigquery/.gitignore
//go:embed google-web-analytics/.gitignore
//go:embed posthog-bigquery/.gitignore
//go:embed academy-sql-beginner/.gitignore
// Note: academy-sql-beginner/queries/audit-lab/_answer-key.md is deliberately NOT
// embedded. Go's embed excludes underscore-prefixed files, which is what keeps the
// audit-lab answers off a student's machine - a plain-text key inside the project
// is found by any repo-wide search and defeats the exercise. It stays in this repo
// as the acceptance-test fixture.
//go:embed migration-fivetran/.agents/skills/bruin-fivetran-migrator/*
var Templates embed.FS
