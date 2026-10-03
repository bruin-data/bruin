package sqlengine

// Port of sqlglot/generator.py (generator chunk A, part 2): L2012-L2253
// properties / property locations and the *property_sql methods up to withsystemversioningproperty_sql.

import (
	"strings"
)

// chunkAPropertiesLocation mirrors self.PROPERTIES_LOCATION[p.__class__] (KeyError if missing).
func (g *Generator) chunkAPropertiesLocation(p *Expr) string {
	loc, ok := g.s.PROPERTIES_LOCATION[p.Kind()]
	if !ok {
		panic(&ValueError{Msg: kindClassRepr(p.Kind())})
	}
	return loc
}

// properties_sql (generator.py L2012)
func (g *Generator) propertiesSQL(expression *Expr) string {
	rootProperties := []*Expr{}
	withProperties := []*Expr{}

	for _, p := range expression.Expressions() {
		pLoc := g.chunkAPropertiesLocation(p)
		if pLoc == Loc_POST_WITH {
			withProperties = append(withProperties, p)
		} else if pLoc == Loc_POST_SCHEMA {
			rootProperties = append(rootProperties, p)
		}
	}

	rootPropsAST := New(KProperties, "expressions", rootProperties)
	rootPropsAST.parent = expression.parent

	withPropsAST := New(KProperties, "expressions", withProperties)
	withPropsAST.parent = expression.parent

	rootProps := g.rootProperties(rootPropsAST)
	withProps := g.withProperties(withPropsAST)

	if rootProps != "" && withProps != "" && !g.pretty {
		withProps = " " + withProps
	}

	return rootProps + withProps
}

// root_properties (generator.py L2037)
func (g *Generator) rootProperties(properties *Expr) string {
	if len(properties.Expressions()) > 0 {
		return g.expressions(properties, exprsOpts{noIndent: true, sep: strp2(" ")})
	}
	return ""
}

// properties (generator.py L2042)
func (g *Generator) properties(properties *Expr, prefix string, sep string, suffix string, wrapped bool) string {
	if len(properties.Expressions()) > 0 {
		expressions := g.expressions(properties, exprsOpts{sep: strp2(sep), noIndent: true})
		if expressions != "" {
			if wrapped {
				expressions = g.wrap(expressions)
			}
			space := ""
			if pyStrip(prefix) != "" {
				space = " "
			}
			return prefix + space + expressions + suffix
		}
	}
	return ""
}

// with_properties (generator.py L2057)
func (g *Generator) baseWithProperties(properties *Expr) string {
	return g.properties(properties, g.segSep(g.s.WITH_PROPERTIES_PREFIX, ""), ", ", "", true)
}

// locate_properties (generator.py L2060)
func (g *Generator) baseLocateProperties(properties *Expr) propLocations {
	propertiesLocs := propLocations{}
	for _, p := range properties.Expressions() {
		pLoc := g.chunkAPropertiesLocation(p)
		if pLoc != Loc_UNSUPPORTED {
			propertiesLocs[pLoc] = append(propertiesLocs[pLoc], p)
		} else {
			g.unsupported("Unsupported property " + p.Key())
		}
	}

	return propertiesLocs
}

// property_name (generator.py L2071)
func (g *Generator) propertyName(expression *Expr, stringKey bool) string {
	if expression.This().IsA(KDot) {
		return g.sqlKey(expression, "this")
	}
	if stringKey {
		return "'" + expression.Name() + "'"
	}
	return expression.Name()
}

// property_sql (generator.py L2076)
func (g *Generator) propertySQL(expression *Expr) string {
	if expression.Is(KProperty) {
		return g.propertyName(expression, false) + "=" + g.sqlKey(expression, "value")
	}

	propertyName, ok := PROPERTY_TO_NAME[expression.Kind()]
	if propertyName == "" {
		g.unsupported("Unsupported property " + expression.Key())
		if !ok {
			propertyName = "None"
		}
	}

	return propertyName + "=" + g.sqlKey(expression, "this")
}

// uuidproperty_sql (generator.py L2087)
func (g *Generator) uuidpropertySQL(expression *Expr) string {
	return "UUID " + g.sqlKey(expression, "this")
}

// likeproperty_sql (generator.py L2090)
func (g *Generator) baseLikepropertySQL(expression *Expr) string {
	if g.s.SUPPORTS_CREATE_TABLE_LIKE {
		var opts []string
		for _, e := range expression.Expressions() {
			opts = append(opts, e.Name()+" "+g.sqlKey(e, "value"))
		}
		options := strings.Join(opts, " ")
		if options != "" {
			options = " " + options
		}

		like := "LIKE " + g.sqlKey(expression, "this") + options
		if g.s.LIKE_PROPERTY_INSIDE_SCHEMA && !expression.Parent().IsA(KSchema) {
			like = "(" + like + ")"
		}

		return like
	}

	if len(expression.Expressions()) > 0 {
		g.unsupported("Transpilation of LIKE property options is unsupported")
	}

	sel := chunkASelectStarFromLimit0(expression.This())
	return "AS " + g.sql(sel)
}

// chunkASelectStarFromLimit0 mirrors exp.select("*").from_(from_).limit(0).
func chunkASelectStarFromLimit0(from_ *Expr) *Expr {
	sel := New(KSelect, "expressions", []*Expr{Star()})

	// .from_(from_): wraps non-From expressions into From (re-parenting them), copies the instance.
	if from_ == nil {
		panic(genPanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	if !from_.IsA(KFrom) {
		from_ = New(KFrom, "this", from_)
	}
	sel = sel.Copy()
	sel.Set("from_", from_)

	// .limit(0): parse_one("LIMIT 0", into=Limit), copies the instance.
	sel = sel.Copy()
	sel.Set("limit", New(
		KLimit,
		"this", nil,
		"expression", LiteralNumber("0"),
		"offset", nil,
		"limit_options", nil,
		"expressions", nil,
	))
	return sel
}

// fallbackproperty_sql (generator.py L2107)
func (g *Generator) fallbackpropertySQL(expression *Expr) string {
	no := ""
	if expression.ArgB("no") {
		no = "NO "
	}
	protection := ""
	if expression.ArgB("protection") {
		protection = " PROTECTION"
	}
	return no + "FALLBACK" + protection
}

// journalproperty_sql (generator.py L2112)
func (g *Generator) journalpropertySQL(expression *Expr) string {
	no := ""
	if expression.ArgB("no") {
		no = "NO "
	}
	local := ""
	if v := expression.Arg("local"); truthy(v) {
		local = chunkAPyStr(v) + " "
	}
	dual := ""
	if expression.ArgB("dual") {
		dual = "DUAL "
	}
	before := ""
	if expression.ArgB("before") {
		before = "BEFORE "
	}
	after := ""
	if expression.ArgB("after") {
		after = "AFTER "
	}
	return no + local + dual + before + after + "JOURNAL"
}

// freespaceproperty_sql (generator.py L2121)
func (g *Generator) freespacepropertySQL(expression *Expr) string {
	freespace := g.sqlKey(expression, "this")
	percent := ""
	if expression.ArgB("percent") {
		percent = " PERCENT"
	}
	return "FREESPACE=" + freespace + percent
}

// checksumproperty_sql (generator.py L2126)
func (g *Generator) checksumpropertySQL(expression *Expr) string {
	var property string
	if expression.ArgB("default") {
		property = "DEFAULT"
	} else if expression.ArgB("on") {
		property = "ON"
	} else {
		property = "OFF"
	}
	return "CHECKSUM=" + property
}

// mergeblockratioproperty_sql (generator.py L2135)
func (g *Generator) mergeblockratiopropertySQL(expression *Expr) string {
	if expression.ArgB("no") {
		return "NO MERGEBLOCKRATIO"
	}
	if expression.ArgB("default") {
		return "DEFAULT MERGEBLOCKRATIO"
	}

	percent := ""
	if expression.ArgB("percent") {
		percent = " PERCENT"
	}
	return "MERGEBLOCKRATIO=" + g.sqlKey(expression, "this") + percent
}

// moduleproperty_sql (generator.py L2144)
func (g *Generator) modulepropertySQL(expression *Expr) string {
	expressions := g.expressions(expression, exprsOpts{flat: true})
	if expressions != "" {
		expressions = "(" + expressions + ")"
	}
	return "USING " + g.sqlKey(expression, "this") + expressions
}

// datablocksizeproperty_sql (generator.py L2149)
func (g *Generator) datablocksizepropertySQL(expression *Expr) string {
	def := expression.ArgB("default")
	minimum := expression.ArgB("minimum")
	maximum := expression.ArgB("maximum")
	if def || minimum || maximum {
		var prop string
		if def {
			prop = "DEFAULT"
		} else if minimum {
			prop = "MINIMUM"
		} else {
			prop = "MAXIMUM"
		}
		return prop + " DATABLOCKSIZE"
	}
	units := ""
	if v := expression.Arg("units"); truthy(v) {
		units = " " + chunkAPyStr(v)
	}
	return "DATABLOCKSIZE=" + g.sqlKey(expression, "size") + units
}

// blockcompressionproperty_sql (generator.py L2165)
func (g *Generator) blockcompressionpropertySQL(expression *Expr) string {
	autotemp := expression.Arg("autotemp")
	always := expression.ArgB("always")
	def := expression.ArgB("default")
	manual := expression.ArgB("manual")
	never := expression.ArgB("never")

	var prop string
	if autotemp != nil {
		at, _ := autotemp.(*Expr)
		prop = "AUTOTEMP(" + g.expressions(at, exprsOpts{}) + ")"
	} else if always {
		prop = "ALWAYS"
	} else if def {
		prop = "DEFAULT"
	} else if manual {
		prop = "MANUAL"
	} else if never {
		prop = "NEVER"
	} else {
		// Python raises UnboundLocalError here.
		panic(&ValueError{Msg: "cannot access local variable 'prop' where it is not associated with a value"})
	}
	return "BLOCKCOMPRESSION=" + prop
}

// isolatedloadingproperty_sql (generator.py L2184)
func (g *Generator) isolatedloadingpropertySQL(expression *Expr) string {
	no := ""
	if expression.ArgB("no") {
		no = " NO"
	}
	concurrent := ""
	if expression.ArgB("concurrent") {
		concurrent = " CONCURRENT"
	}
	target := g.sqlKey(expression, "target")
	if target != "" {
		target = " " + target
	}
	return "WITH" + no + concurrent + " ISOLATED LOADING" + target
}

// partitionboundspec_sql (generator.py L2193)
func (g *Generator) partitionboundspecSQL(expression *Expr) string {
	if _, ok := expression.Arg("this").([]*Expr); ok {
		return "IN (" + g.expressions(expression, exprsOpts{key: "this", flat: true}) + ")"
	}
	if expression.ArgB("this") {
		modulus := g.sqlKey(expression, "this")
		remainder := g.sqlKey(expression, "expression")
		return "WITH (MODULUS " + modulus + ", REMAINDER " + remainder + ")"
	}

	fromExpressions := g.expressions(expression, exprsOpts{key: "from_expressions", flat: true})
	toExpressions := g.expressions(expression, exprsOpts{key: "to_expressions", flat: true})
	return "FROM (" + fromExpressions + ") TO (" + toExpressions + ")"
}

// partitionedofproperty_sql (generator.py L2205)
func (g *Generator) partitionedofpropertySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")

	forValuesOrDefault := expression.Expression()
	var forValuesOrDefaultSQL string
	if forValuesOrDefault.IsA(KPartitionBoundSpec) {
		forValuesOrDefaultSQL = " FOR VALUES " + g.sql(forValuesOrDefault)
	} else {
		forValuesOrDefaultSQL = " DEFAULT"
	}

	return "PARTITION OF " + this + forValuesOrDefaultSQL
}

// lockingproperty_sql (generator.py L2216)
func (g *Generator) lockingpropertySQL(expression *Expr) string {
	kind := expression.Arg("kind")
	this := ""
	if expression.ArgB("this") {
		this = " " + g.sqlKey(expression, "this")
	}
	forOrIn := ""
	if v := expression.Arg("for_or_in"); truthy(v) {
		forOrIn = " " + chunkAPyStr(v)
	}
	lockType := expression.Arg("lock_type")
	override := ""
	if expression.ArgB("override") {
		override = " OVERRIDE"
	}
	return "LOCKING " + chunkAPyStr(kind) + this + forOrIn + " " + chunkAPyStr(lockType) + override
}

// withdataproperty_sql (generator.py L2225)
func (g *Generator) withdatapropertySQL(expression *Expr) string {
	no := ""
	if expression.ArgB("no") {
		no = "NO "
	}
	dataSQL := "WITH " + no + "DATA"
	statistics := expression.Arg("statistics")
	statisticsSQL := ""
	if statistics != nil {
		noStats := ""
		if !truthy(statistics) {
			noStats = "NO "
		}
		statisticsSQL = " AND " + noStats + "STATISTICS"
	}
	return dataSQL + statisticsSQL
}

// withsystemversioningproperty_sql (generator.py L2233)
func (g *Generator) withsystemversioningpropertySQL(expression *Expr) string {
	this := g.sqlKey(expression, "this")
	if this != "" {
		this = "HISTORY_TABLE=" + this
	}
	// None when empty: func() skips None arguments.
	var dataConsistency any
	if s := g.sqlKey(expression, "data_consistency"); s != "" {
		dataConsistency = "DATA_CONSISTENCY_CHECK=" + s
	}
	var retentionPeriod any
	if s := g.sqlKey(expression, "retention_period"); s != "" {
		retentionPeriod = "HISTORY_RETENTION_PERIOD=" + s
	}

	var onSQL string
	if this != "" {
		onSQL = g.fn("ON", this, dataConsistency, retentionPeriod)
	} else if expression.ArgB("on") {
		onSQL = "ON"
	} else {
		onSQL = "OFF"
	}

	sql := "SYSTEM_VERSIONING=" + onSQL

	if expression.ArgB("with_") {
		return "WITH(" + sql + ")"
	}
	return sql
}
