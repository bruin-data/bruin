package mssql

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          errorMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:          errorMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: errorMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
		pipeline.MaterializationStrategyMerge:         errorMaterializer,
	},
}

func NewMaterializer() *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s", asset.Materialization.Strategy, asset.Materialization.Type)
}
