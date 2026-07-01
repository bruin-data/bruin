// Standalone module so the semantic engine can be consumed by lightweight
// tools (e.g. dac) without pulling in the full bruin CLI dependency tree.
module github.com/bruin-data/bruin/semantic-engine

go 1.25.0

require (
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2
	github.com/spf13/afero v1.15.0
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/text v0.28.0 // indirect
