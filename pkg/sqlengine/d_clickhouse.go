package sqlengine

import (
	"fmt"
	"strings"
)

func init() { registerCustomizer("clickhouse", customizeClickHouse) }

func customizeClickHouse(d *Dialect) {
	d.hooks.generateValuesAliases = clickhouseGenerateValuesAliases
	customizeClickHouseParser(d)
	customizeClickHouseGenerator(d)
}

// clickhouseGenerateValuesAliases returns the column aliases of a VALUES clause: the column names
// of a leading structure string when there is one, otherwise c1, c2 and so on.
func clickhouseGenerateValuesAliases(d *Dialect, e *Expr) []*Expr {
	// Clickhouse allows VALUES to have an embedded structure e.g:
	// VALUES('person String, place String', ('Noah', 'Paris'), ...)
	values := e.Expressions()[0].Expressions()

	var structure *Expr
	if len(values) > 1 && values[0].IsString() && values[1].IsA(KTuple) {
		structure = values[0]
	}

	var columnAliases []*Expr
	if structure != nil {
		for _, coldef := range strings.Split(structure.Name(), ",") {
			coldef = pyStrip(coldef)
			columnAliases = append(columnAliases, ToIdentifier(strings.Split(coldef, " ")[0], nil))
		}
	} else {
		for i := range values[0].Expressions() {
			columnAliases = append(columnAliases, ToIdentifier(fmt.Sprintf("c%d", i+1), nil))
		}
	}

	return columnAliases
}
