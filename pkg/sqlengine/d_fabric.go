package sqlengine

// Port of sqlglot/dialects/fabric.py, sqlglot/parsers/fabric.py and sqlglot/generators/fabric.py.
//
// Microsoft Fabric Data Warehouse dialect that inherits from T-SQL.

import "strconv"

func init() { registerCustomizer("fabric", customizeFabric) }

func customizeFabric(d *Dialect) {
	// FabricParser
	d.P.h.parseCreate = fabricParseCreate

	// FabricGenerator
	d.G.TRANSFORMS[KCreate] = transformPreprocess([]func(*Expr) *Expr{fabricAddDefaultPrecisionToVarchar}, nil)
	d.G.h.datatypeSQL = fabricDatatypeSQL
	d.G.h.castSQL = fabricCastSQL
	d.G.h.attimezoneSQL = fabricAttimezoneSQL
	d.G.methods[KUnixToTime] = fabricUnixtotimeSQL
}

// fabricParseCreate mirrors FabricParser._parse_create.
func fabricParseCreate(p *Parser) *Expr {
	create := tsqlParseCreate(p)

	if create.IsA(KCreate) {
		// Transform VARCHAR/CHAR without precision to VARCHAR(1)/CHAR(1)
		if create.KindText() == "TABLE" && create.This().IsA(KSchema) {
			for _, column := range create.This().Expressions() {
				if column.IsA(KColumnDef) {
					columnType := column.ArgE("kind")
					if columnType.IsA(KDataType) &&
						(columnType.Arg("this") == DT_VARCHAR || columnType.Arg("this") == DT_CHAR) &&
						len(columnType.Expressions()) == 0 {
						// Add default precision of 1 to VARCHAR/CHAR without precision
						// When n isn't specified in a data definition or variable declaration statement, the default length is 1.
						// https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql?view=sql-server-ver17#remarks
						columnType.Set("expressions", []*Expr{LiteralNumber("1")})
					}
				}
			}
		}
	}

	return create
}

// fabricCapDataTypePrecision mirrors generators/fabric.py _cap_data_type_precision.
// Cap the precision of to a maximum of `max_precision` digits.
// If no precision is specified, default to `max_precision`.
func fabricCapDataTypePrecision(expression *Expr, maxPrecision int /*=6*/) *Expr {
	precisionParam := expression.Find(KDataTypeParam)

	var targetPrecision int
	if precisionParam != nil && precisionParam.This().IsInt() {
		currentPrecision := tsqlIntToPy(precisionParam.This())
		targetPrecision = min(currentPrecision, maxPrecision)
	} else {
		targetPrecision = maxPrecision
	}

	return New(
		KDataType,
		"this", expression.Arg("this"),
		"expressions", []*Expr{New(KDataTypeParam, "this", LiteralInt(targetPrecision))},
	)
}

// fabricAddDefaultPrecisionToVarchar mirrors generators/fabric.py _add_default_precision_to_varchar.
// Transform function to add VARCHAR(MAX) or CHAR(MAX) for cross-dialect conversion.
func fabricAddDefaultPrecisionToVarchar(expression *Expr) *Expr {
	if expression.IsA(KCreate) && expression.KindText() == "TABLE" && expression.This().IsA(KSchema) {
		for _, column := range expression.This().Expressions() {
			if column.IsA(KColumnDef) {
				columnType := column.ArgE("kind")
				if columnType.IsA(KDataType) &&
					(columnType.Arg("this") == DT_VARCHAR || columnType.Arg("this") == DT_CHAR) &&
					len(columnType.Expressions()) == 0 {
					// For transpilation, VARCHAR/CHAR without precision becomes VARCHAR(MAX)/CHAR(MAX)
					columnType.Set("expressions", []*Expr{VarChecked("MAX")})
				}
			}
		}
	}

	return expression
}

// fabricDatatypeSQL mirrors FabricGenerator.datatype_sql.
func fabricDatatypeSQL(g *Generator, e *Expr) string {
	// Check if this is a temporal type that needs precision handling. Fabric limits temporal
	// types to max 6 digits precision. When no precision is specified, we default to 6 digits.
	if DataTypeIsType(e, fabricTemporalTypesAny(), false) && e.Arg("this") != DT_DATE {
		// Create a new expression with the capped precision
		e = fabricCapDataTypePrecision(e, 6)
	}

	return g.baseDatatypeSQL(e)
}

// fabricTemporalTypesAny returns exp.DataType.TEMPORAL_TYPES as is_type arguments.
func fabricTemporalTypesAny() []any {
	items := DataType_TEMPORAL_TYPES.Items()
	out := make([]any, len(items))
	for i, t := range items {
		out[i] = t
	}
	return out
}

// fabricCastSQL mirrors FabricGenerator.cast_sql.
func fabricCastSQL(g *Generator, e *Expr, safePrefix string) string {
	// Cast to DATETIMEOFFSET if inside an AT TIME ZONE expression
	// https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
	if DataTypeIsType(e.ArgE("to"), []any{DT_TIMESTAMPTZ}, false) {
		atTimeZone := e.FindAncestor(KAtTimeZone, KSelect)

		// Return normal cast, if the expression is not in an AT TIME ZONE context
		if !atTimeZone.IsA(KAtTimeZone) {
			return g.baseCastSQL(e, safePrefix)
		}

		// Get the precision from the original TIMESTAMPTZ cast and cap it to 6
		cappedDataType := fabricCapDataTypePrecision(e.ArgE("to"), 6)
		precision := cappedDataType.Find(KDataTypeParam)
		precisionValue := 6
		if precision != nil && precision.This().IsInt() {
			precisionValue = tsqlIntToPy(precision.This())
		}

		// Do the cast explicitly to bypass sqlglot's default handling
		datetimeoffset := "CAST(" + dhPyStr(e.This()) + " AS DATETIMEOFFSET(" + strconv.Itoa(precisionValue) + "))"

		return g.sql(datetimeoffset)
	}

	return g.baseCastSQL(e, safePrefix)
}

// fabricAttimezoneSQL mirrors FabricGenerator.attimezone_sql.
func fabricAttimezoneSQL(g *Generator, e *Expr) string {
	// Wrap the AT TIME ZONE expression in a cast to DATETIME2 if it contains a TIMESTAMPTZ
	// https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
	timestamptzCast := e.Find(KCast)
	if timestamptzCast != nil && DataTypeIsType(timestamptzCast.ArgE("to"), []any{DT_TIMESTAMPTZ}, false) {
		// Get the precision from the original TIMESTAMPTZ cast and cap it to 6
		dataType := timestamptzCast.ArgE("to")
		cappedDataType := fabricCapDataTypePrecision(dataType, 6)
		precisionParam := cappedDataType.Find(KDataTypeParam)
		precision := "6"
		if precisionParam != nil {
			precision = tsqlPyNumStr(precisionParam.This())
		}

		// Generate the AT TIME ZONE expression (which will handle the inner cast conversion)
		atTimeZoneSQL := g.baseAttimezoneSQL(e)

		// Wrap it in an outer cast to DATETIME2
		return "CAST(" + atTimeZoneSQL + " AS DATETIME2(" + precision + "))"
	}

	return g.baseAttimezoneSQL(e)
}

// tsqlPyNumStr mirrors str(literal.to_py()) for a numeric literal.
func tsqlPyNumStr(e *Expr) string {
	if e.IsInt() {
		return strconv.Itoa(tsqlIntToPy(e))
	}
	return chunkDToPyNumber(e).String()
}

// fabricUnixToTimeSeconds mirrors exp.UnixToTime.SECONDS.
var fabricUnixToTimeSeconds = LiteralInt(0)

// fabricUnixtotimeSQL mirrors FabricGenerator.unixtotime_sql.
func fabricUnixtotimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.This()

	if scale != nil && !scale.Equal(fabricUnixToTimeSeconds) {
		g.unsupported("UnixToTime scale " + dhPyStr(scale) + " is not supported by Fabric")
		return ""
	}

	// Convert unix timestamp (seconds) to microseconds and round to avoid decimals
	microseconds := tsqlBinop(KMul, timestamp, LiteralNumber("1e6"))
	rounded := dhFunc("round", microseconds, 0)
	roundedMsAsBigint := CastExpr(rounded, DT_BIGINT, true, nil)

	// Create the base datetime as '1970-01-01' cast to DATETIME2(6)
	fabric := MustDialect("fabric")
	epochStart := CastExpr(MaybeParse("'1970-01-01'", KNone, "", fabric), "datetime2(6)", true, fabric)

	dateadd := New(
		KDateAdd,
		"this", epochStart,
		"expression", roundedMsAsBigint,
		"unit", LiteralString("MICROSECONDS"),
	)
	return g.sql(dateadd)
}

// tsqlBinop mirrors Expr._binop(klass, other) for expression operands (e.g. `a * b`).
func tsqlBinop(klass Kind, self *Expr, other *Expr) *Expr {
	this := self.Copy()
	o := other.Copy()
	if !this.IsA(klass) && !o.IsA(klass) {
		if this.IsA(KBinary) {
			this = New(KParen, "this", this)
		}
		if o.IsA(KBinary) {
			o = New(KParen, "this", o)
		}
	}
	return New(klass, "this", this, "expression", o)
}
