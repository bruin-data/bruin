package templates

import (
	"embed"
)

//go:embed *
//go:embed */.bruin.yml
//go:embed ai/agents-md/AGENTS.md
//go:embed ai/skill-self-heal/.agents/skills/*/SKILL.md
var Templates embed.FS
