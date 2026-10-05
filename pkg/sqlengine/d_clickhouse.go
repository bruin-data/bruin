package sqlengine

import (
	"fmt"
	"strings"
)

// Port of sqlglot/dialects/clickhouse.py (ClickHouse dialect class body). The tokenizer and the
// data-only class attributes are generated (zz_*_settings.go).

func init() { registerCustomizer("clickhouse", customizeClickHouse) }

func customizeClickHouse(d *Dialect) {
	d.hooks.generateValuesAliases = clickhouseGenerateValuesAliases
	customizeClickHouseParser(d)
	customizeClickHouseGenerator(d)
}

// clickhouseGenerateValuesAliases mirrors ClickHouse.generate_values_aliases.
func clickhouseGenerateValuesAliases(d *Dialect, e *Expr) []*Expr {
	// Clickhouse allows VALUES to have an embedded structure e.g:
	// VALUES('person String, place String', ('Noah', 'Paris'), ...)
	// In this case, we don't want to qualify the columns
	values := e.Expressions()[0].Expressions()

	var structure *Expr
	if len(values) > 1 && values[0].IsString() && values[1].IsA(KTuple) {
		structure = values[0]
	}

	var columnAliases []*Expr
	if structure != nil {
		// Split each column definition into the column name e.g:
		// 'person String, place String' -> ['person', 'place']
		for _, coldef := range strings.Split(structure.Name(), ",") {
			coldef = pyStrip(coldef)
			columnAliases = append(columnAliases, ToIdentifier(strings.Split(coldef, " ")[0], nil))
		}
	} else {
		// Default column aliases in CH are "c1", "c2", etc.
		for i := range values[0].Expressions() {
			columnAliases = append(columnAliases, ToIdentifier(fmt.Sprintf("c%d", i+1), nil))
		}
	}

	return columnAliases
}
