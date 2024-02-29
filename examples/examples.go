package examples

import _ "embed"

//go:embed init-pipeline/pipeline.yml
var InitPipeline []byte

//go:embed init-pipeline/.bruin.yml
var InitPipelineConfig []byte

//go:embed init-pipeline/assets/example.sql
var InitPipelineAsset []byte
