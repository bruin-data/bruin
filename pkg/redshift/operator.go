package redshift

import (
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/query"
)

type TableSensor struct {
	connection config.ConnectionGetter
	sensorMode string
	extractor  query.QueryExtractor
}

func NewTableSensor(conn config.ConnectionGetter, sensorMode string, extractor query.QueryExtractor) *TableSensor {
	return &TableSensor{
		connection: conn,
		sensorMode: sensorMode,
		extractor:  extractor,
	}
}