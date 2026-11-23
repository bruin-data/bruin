package sqlengine

import "strconv"

func init() { registerCustomizer("fabric", customizeFabric) }

func customizeFabric(d *Dialect) {
	d.P.h.parseCreate = fabricParseCreate

	d.G.TRANSFORMS[KCreate] = transformPreprocess([]func(*Expr) *Expr{fabricAddDefaultPrecisionToVarchar}, nil)
	d.G.h.datatypeSQL = fabricDatatypeSQL
	d.G.h.castSQL = fabricCastSQL
	d.G.h.attimezoneSQL = fabricAttimezoneSQL
	d.G.methods[KUnixToTime] = fabricUnixtotimeSQL
}

func fabricParseCreate(p *Parser) *Expr {
	create := tsqlParseCreate(p)

	if create.IsA(KCreate) {
		if create.KindText() == "TABLE" && create.This().IsA(KSchema) {
			for _, column := range create.This().Expressions() {
				if column.IsA(KColumnDef) {
					columnType := column.ArgE("kind")
					if columnType.IsA(KDataType) &&
						(columnType.Arg("this") == DT_VARCHAR || columnType.Arg("this") == DT_CHAR) &&
						len(columnType.Expressions()) == 0 {
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

func fabricDatatypeSQL(g *Generator, e *Expr) string {
	// Fabric limits temporal types to 6 digits of precision, the default when none is given.
	if DataTypeIsType(e, fabricTemporalTypesAny(), false) && e.Arg("this") != DT_DATE {
		e = fabricCapDataTypePrecision(e, 6)
	}

	return g.baseDatatypeSQL(e)
}

func fabricTemporalTypesAny() []any {
	items := DataType_TEMPORAL_TYPES.Items()
	out := make([]any, len(items))
	for i, t := range items {
		out[i] = t
	}
	return out
}

func fabricCastSQL(g *Generator, e *Expr, safePrefix string) string {
	// Cast to DATETIMEOFFSET if inside an AT TIME ZONE expression
	// https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
	if DataTypeIsType(e.ArgE("to"), []any{DT_TIMESTAMPTZ}, false) {
		atTimeZone := e.FindAncestor(KAtTimeZone, KSelect)

		if !atTimeZone.IsA(KAtTimeZone) {
			return g.baseCastSQL(e, safePrefix)
		}

		cappedDataType := fabricCapDataTypePrecision(e.ArgE("to"), 6)
		precision := cappedDataType.Find(KDataTypeParam)
		precisionValue := 6
		if precision != nil && precision.This().IsInt() {
			precisionValue = tsqlIntToPy(precision.This())
		}

		// Do the cast explicitly to bypass the default cast handling
		datetimeoffset := "CAST(" + dhPyStr(e.This()) + " AS DATETIMEOFFSET(" + strconv.Itoa(precisionValue) + "))"

		return g.sql(datetimeoffset)
	}

	return g.baseCastSQL(e, safePrefix)
}

func fabricAttimezoneSQL(g *Generator, e *Expr) string {
	// Wrap the AT TIME ZONE expression in a cast to DATETIME2 if it contains a TIMESTAMPTZ
	// https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetimeoffset-transact-sql#microsoft-fabric-support
	timestamptzCast := e.Find(KCast)
	if timestamptzCast != nil && DataTypeIsType(timestamptzCast.ArgE("to"), []any{DT_TIMESTAMPTZ}, false) {
		dataType := timestamptzCast.ArgE("to")
		cappedDataType := fabricCapDataTypePrecision(dataType, 6)
		precisionParam := cappedDataType.Find(KDataTypeParam)
		precision := "6"
		if precisionParam != nil {
			precision = tsqlPyNumStr(precisionParam.This())
		}

		atTimeZoneSQL := g.baseAttimezoneSQL(e)

		// Don't wrap again if it's already cast to DATETIME2, e.g. when re-parsing our own output
		if parent := e.Parent(); parent.IsA(KCast) && parent.ArgE("to").IsTypeOf(DT_DATETIME2) {
			return atTimeZoneSQL
		}

		return "CAST(" + atTimeZoneSQL + " AS DATETIME2(" + precision + "))"
	}

	return g.baseAttimezoneSQL(e)
}

func tsqlPyNumStr(e *Expr) string {
	if e.IsInt() {
		return strconv.Itoa(tsqlIntToPy(e))
	}
	return chunkDToPyNumber(e).String()
}

var fabricUnixToTimeSeconds = LiteralInt(0)

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
