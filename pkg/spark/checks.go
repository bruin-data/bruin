package spark

import (
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/sail"
)

// NewColumnCheckOperator uses Spark SQL-specific accepted-values and regex
// checks shared with Sail.
func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return sail.NewColumnCheckOperator(manager)
}
