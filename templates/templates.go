package templates

import (
	"embed"
)

// Note: academy-sql-beginner/queries/audit-lab/_answer-key.md is deliberately NOT
// embedded. Go's embed excludes underscore-prefixed files, which is what keeps the
// audit-lab answers off a student's machine - a plain-text key inside the project
// is found by any repo-wide search and defeats the exercise. It stays in this repo
// as the acceptance-test fixture.
//
// academy-sql-intermediate/docs/_data-design.md is excluded the same way. It is
// the contributor-facing record of the nine defects that template injects on
// purpose, and finding those defects is the coursework.
// academy-sql-advanced/docs/_data-design.md and _known-defects.md are excluded
// for the same reason. The advanced template deliberately fails at runtime on
// one mart asset; these notes are for maintainers, not students.
//
//go:embed *
//go:embed */.bruin.yml
//go:embed migration-fivetran/.gitignore
//go:embed stripe-bigquery/.gitignore
//go:embed google-web-analytics/.gitignore
//go:embed posthog-bigquery/.gitignore
//go:embed academy-sql-beginner/.gitignore
//go:embed academy-sql-intermediate/.gitignore
//go:embed academy-sql-advanced/.bruin.yml
//go:embed academy-sql-advanced/.gitignore
//go:embed academy-sql-advanced/pipeline/assets/staging/.gitkeep
//go:embed academy-sql-advanced/pipeline/assets/core/.gitkeep
//go:embed academy-sql-advanced/pipeline/assets/mart/.gitkeep
//go:embed academy-sql-intermediate/pipeline/assets/staging/.gitkeep
//go:embed academy-sql-intermediate/pipeline/assets/core/.gitkeep
//go:embed academy-sql-intermediate/pipeline/assets/mart/.gitkeep
//go:embed migration-fivetran/.agents/skills/bruin-fivetran-migrator/*
var Templates embed.FS
