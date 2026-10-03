package sqlengine

import (
	"reflect"
	"strings"
	"sync"
)

// UNSUPPORTED_TEMPLATE mirrors sqlglot.generator.UNSUPPORTED_TEMPLATE.
const UNSUPPORTED_TEMPLATE = "Argument '%s' is not supported for expression '%s' when targeting %s."

// baseAfterHavingModifierTransforms mirrors Generator.AFTER_HAVING_MODIFIER_TRANSFORMS, which is
// {cluster, distribute, sort, **generator.AFTER_HAVING_MODIFIER_TRANSFORMS (windows, qualify)}.
func baseAfterHavingModifierTransforms() (map[string]GenFunc, []string) {
	return map[string]GenFunc{
			"cluster":    func(g *Generator, e *Expr) string { return g.sqlKey(e, "cluster") },
			"distribute": func(g *Generator, e *Expr) string { return g.sqlKey(e, "distribute") },
			"sort":       func(g *Generator, e *Expr) string { return g.sqlKey(e, "sort") },
			"windows": func(g *Generator, e *Expr) string {
				if e.ArgB("windows") {
					return g.seg("WINDOW ") + g.expressions(e, exprsOpts{key: "windows", flat: true})
				}
				return ""
			},
			"qualify": func(g *Generator, e *Expr) string { return g.sqlKey(e, "qualify") },
		},
		[]string{"cluster", "distribute", "sort", "windows", "qualify"}
}

// baseGeneratorTransforms mirrors Generator.TRANSFORMS (before the Dialect metaclass prunes the
// unsupported JSONPathPart entries).
func baseGeneratorTransforms() map[Kind]GenFunc {
	this := func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	exprList := func(es []*Expr) []any {
		out := make([]any, len(es))
		for i, x := range es {
			out[i] = x
		}
		return out
	}

	t := map[Kind]GenFunc{}
	for k, f := range JSON_PATH_PART_TRANSFORMS {
		t[k] = f
	}
	for k, f := range map[Kind]GenFunc{
		KAdjacent: func(g *Generator, e *Expr) string { return g.binary(e, "-|-") },
		KAllowedValuesProperty: func(g *Generator, e *Expr) string {
			return "ALLOWED_VALUES " + g.expressions(e, exprsOpts{flat: true})
		},
		KAnalyzeColumns: func(g *Generator, e *Expr) string { return this(g, e) },
		KAnalyzeWith: func(g *Generator, e *Expr) string {
			return g.expressions(e, exprsOpts{prefix: "WITH ", sep: strp2(" ")})
		},
		KArrayContainedBy:       func(g *Generator, e *Expr) string { return g.binary(e, "<@") },
		KArrayContainsAll:       func(g *Generator, e *Expr) string { return g.binary(e, "@>") },
		KArrayOverlaps:          func(g *Generator, e *Expr) string { return g.binary(e, "&&") },
		KAssumeColumnConstraint: func(g *Generator, e *Expr) string { return "ASSUME (" + this(g, e) + ")" },
		KAutoRefreshProperty:    func(g *Generator, e *Expr) string { return "AUTO REFRESH " + this(g, e) },
		KBackupProperty:         func(g *Generator, e *Expr) string { return "BACKUP " + this(g, e) },
		KCaseSpecificColumnConstraint: func(g *Generator, e *Expr) string {
			not := ""
			if e.ArgB("not_") {
				not = "NOT "
			}
			return not + "CASESPECIFIC"
		},
		KCalledOnNullInputProperty:    func(g *Generator, e *Expr) string { return "CALLED ON NULL INPUT" },
		KCeil:                         func(g *Generator, e *Expr) string { return g.ceilFloor(e) },
		KCharacterSetColumnConstraint: func(g *Generator, e *Expr) string { return "CHARACTER SET " + this(g, e) },
		KCharacterSetProperty: func(g *Generator, e *Expr) string {
			def := ""
			if e.ArgB("default") {
				def = "DEFAULT "
			}
			return def + "CHARACTER SET=" + this(g, e)
		},
		KClusteredColumnConstraint: func(g *Generator, e *Expr) string {
			return "CLUSTERED (" + g.expressions(e, exprsOpts{key: "this", noIndent: true}) + ")"
		},
		KCollateColumnConstraint: func(g *Generator, e *Expr) string { return "COLLATE " + this(g, e) },
		KCommentColumnConstraint: func(g *Generator, e *Expr) string { return "COMMENT " + this(g, e) },
		KConnectByRoot:           func(g *Generator, e *Expr) string { return "CONNECT_BY_ROOT " + this(g, e) },
		KConvertToCharset: func(g *Generator, e *Expr) string {
			return g.fn("CONVERT", e.Arg("this"), e.Arg("dest"), e.Arg("source"))
		},
		KCopyGrantsProperty: func(g *Generator, e *Expr) string { return "COPY GRANTS" },
		KCredentialsProperty: func(g *Generator, e *Expr) string {
			return "CREDENTIALS=(" + g.expressions(e, exprsOpts{key: "expressions", sep: strp2(" ")}) + ")"
		},
		KCurrentCatalog:             func(g *Generator, e *Expr) string { return "CURRENT_CATALOG" },
		KSessionUser:                func(g *Generator, e *Expr) string { return "SESSION_USER" },
		KDateFormatColumnConstraint: func(g *Generator, e *Expr) string { return "FORMAT " + this(g, e) },
		KDefaultColumnConstraint:    func(g *Generator, e *Expr) string { return "DEFAULT " + this(g, e) },
		KApiProperty:                func(g *Generator, e *Expr) string { return "API" },
		KApplicationProperty:        func(g *Generator, e *Expr) string { return "APPLICATION" },
		KCatalogProperty:            func(g *Generator, e *Expr) string { return "CATALOG" },
		KComputeProperty:            func(g *Generator, e *Expr) string { return "COMPUTE" },
		KDatabaseProperty:           func(g *Generator, e *Expr) string { return "DATABASE" },
		KDynamicProperty:            func(g *Generator, e *Expr) string { return "DYNAMIC" },
		KEmptyProperty:              func(g *Generator, e *Expr) string { return "EMPTY" },
		KEncodeColumnConstraint:     func(g *Generator, e *Expr) string { return "ENCODE " + this(g, e) },
		KEndStatement:               func(g *Generator, e *Expr) string { return "END" },
		KEnviromentProperty: func(g *Generator, e *Expr) string {
			return "ENVIRONMENT (" + g.expressions(e, exprsOpts{flat: true}) + ")"
		},
		KHandlerProperty:        func(g *Generator, e *Expr) string { return "HANDLER " + this(g, e) },
		KParameterStyleProperty: func(g *Generator, e *Expr) string { return "PARAMETER STYLE " + this(g, e) },
		KEphemeralColumnConstraint: func(g *Generator, e *Expr) string {
			s := ""
			if e.ArgB("this") {
				s = " " + this(g, e)
			}
			return "EPHEMERAL" + s
		},
		KExcludeColumnConstraint: func(g *Generator, e *Expr) string {
			return "EXCLUDE " + strings.TrimLeftFunc(this(g, e), pyIsSpaceRune)
		},
		KExecuteAsProperty: func(g *Generator, e *Expr) string { return g.nakedProperty(e) },
		KExcept:            func(g *Generator, e *Expr) string { return g.setOperations(e) },
		KExternalProperty:  func(g *Generator, e *Expr) string { return "EXTERNAL" },
		KFloor:             func(g *Generator, e *Expr) string { return g.ceilFloor(e) },
		KGet:               func(g *Generator, e *Expr) string { return g.getPutSQL(e) },
		KGlobalProperty:    func(g *Generator, e *Expr) string { return "GLOBAL" },
		KHeapProperty:      func(g *Generator, e *Expr) string { return "HEAP" },
		KHybridProperty:    func(g *Generator, e *Expr) string { return "HYBRID" },
		KIcebergProperty:   func(g *Generator, e *Expr) string { return "ICEBERG" },
		KInheritsProperty: func(g *Generator, e *Expr) string {
			return "INHERITS (" + g.expressions(e, exprsOpts{flat: true}) + ")"
		},
		KInlineLengthColumnConstraint: func(g *Generator, e *Expr) string { return "INLINE LENGTH " + this(g, e) },
		KInputModelProperty:           func(g *Generator, e *Expr) string { return "INPUT" + this(g, e) },
		KIntersect:                    func(g *Generator, e *Expr) string { return g.setOperations(e) },
		KIntervalSpan: func(g *Generator, e *Expr) string {
			return this(g, e) + " TO " + g.sqlKey(e, "expression")
		},
		KInt64: func(g *Generator, e *Expr) string {
			return g.sql(genTablesCast(e.Arg("this"), DT_BIGINT))
		},
		KJSONBContainsAnyTopKeys: func(g *Generator, e *Expr) string { return g.binary(e, "?|") },
		KJSONBContainsAllTopKeys: func(g *Generator, e *Expr) string { return g.binary(e, "?&") },
		KJSONBDeleteAtPath:       func(g *Generator, e *Expr) string { return g.binary(e, "#-") },
		KJSONBPathExists:         func(g *Generator, e *Expr) string { return g.binary(e, "@?") },
		KJSONObject:              func(g *Generator, e *Expr) string { return g.jsonobjectSQL(e, "") },
		KJSONObjectAgg:           func(g *Generator, e *Expr) string { return g.jsonobjectSQL(e, "") },
		KLanguageProperty:        func(g *Generator, e *Expr) string { return g.nakedProperty(e) },
		KLocationProperty:        func(g *Generator, e *Expr) string { return g.nakedProperty(e) },
		KLogProperty: func(g *Generator, e *Expr) string {
			no := ""
			if e.ArgB("no") {
				no = "NO "
			}
			return no + "LOG"
		},
		KMaskingProperty:      func(g *Generator, e *Expr) string { return "MASKING" },
		KMaterializedProperty: func(g *Generator, e *Expr) string { return "MATERIALIZED" },
		KNetFunc:              func(g *Generator, e *Expr) string { return "NET." + this(g, e) },
		KNetworkProperty:      func(g *Generator, e *Expr) string { return "NETWORK" },
		KNonClusteredColumnConstraint: func(g *Generator, e *Expr) string {
			return "NONCLUSTERED (" + g.expressions(e, exprsOpts{key: "this", noIndent: true}) + ")"
		},
		KNoPrimaryIndexProperty:            func(g *Generator, e *Expr) string { return "NO PRIMARY INDEX" },
		KNotForReplicationColumnConstraint: func(g *Generator, e *Expr) string { return "NOT FOR REPLICATION" },
		KOnCommitProperty: func(g *Generator, e *Expr) string {
			action := "PRESERVE"
			if e.ArgB("delete") {
				action = "DELETE"
			}
			return "ON COMMIT " + action + " ROWS"
		},
		KOnProperty:               func(g *Generator, e *Expr) string { return "ON " + this(g, e) },
		KOnUpdateColumnConstraint: func(g *Generator, e *Expr) string { return "ON UPDATE " + this(g, e) },
		// The operator is produced in `binary`
		KOperator:                          func(g *Generator, e *Expr) string { return g.binary(e, "") },
		KOutputModelProperty:               func(g *Generator, e *Expr) string { return "OUTPUT" + this(g, e) },
		KExtendsLeft:                       func(g *Generator, e *Expr) string { return g.binary(e, "&<") },
		KExtendsRight:                      func(g *Generator, e *Expr) string { return g.binary(e, "&>") },
		KPathColumnConstraint:              func(g *Generator, e *Expr) string { return "PATH " + this(g, e) },
		KPartitionedByBucket:               func(g *Generator, e *Expr) string { return g.fn("BUCKET", e.Arg("this"), e.Arg("expression")) },
		KPartitionByTruncate:               func(g *Generator, e *Expr) string { return g.fn("TRUNCATE", e.Arg("this"), e.Arg("expression")) },
		KPivotAny:                          func(g *Generator, e *Expr) string { return "ANY" + this(g, e) },
		KPositionalColumn:                  func(g *Generator, e *Expr) string { return "#" + this(g, e) },
		KProjectionPolicyColumnConstraint:  func(g *Generator, e *Expr) string { return "PROJECTION POLICY " + this(g, e) },
		KInvisibleColumnConstraint:         func(g *Generator, e *Expr) string { return "INVISIBLE" },
		KZeroFillColumnConstraint:          func(g *Generator, e *Expr) string { return "ZEROFILL" },
		KPut:                               func(g *Generator, e *Expr) string { return g.getPutSQL(e) },
		KRemoteWithConnectionModelProperty: func(g *Generator, e *Expr) string { return "REMOTE WITH CONNECTION " + this(g, e) },
		KReturnsProperty: func(g *Generator, e *Expr) string {
			if e.ArgB("null") {
				return "RETURNS NULL ON NULL INPUT"
			}
			return g.nakedProperty(e)
		},
		KRowAccessProperty:           func(g *Generator, e *Expr) string { return "ROW ACCESS" },
		KSafeFunc:                    func(g *Generator, e *Expr) string { return "SAFE." + this(g, e) },
		KSampleProperty:              func(g *Generator, e *Expr) string { return "SAMPLE BY " + this(g, e) },
		KSecureProperty:              func(g *Generator, e *Expr) string { return "SECURE" },
		KSecurityIntegrationProperty: func(g *Generator, e *Expr) string { return "SECURITY" },
		KSetConfigProperty:           func(g *Generator, e *Expr) string { return this(g, e) },
		KSetProperty: func(g *Generator, e *Expr) string {
			multi := ""
			if e.ArgB("multi") {
				multi = "MULTI"
			}
			return multi + "SET"
		},
		KSettingsProperty: func(g *Generator, e *Expr) string {
			return "SETTINGS" + g.seg("") + g.expressions(e, exprsOpts{})
		},
		KSharingProperty:        func(g *Generator, e *Expr) string { return "SHARING=" + this(g, e) },
		KSqlReadWriteProperty:   func(g *Generator, e *Expr) string { return e.Name() },
		KSqlSecurityProperty:    func(g *Generator, e *Expr) string { return "SQL SECURITY " + this(g, e) },
		KStabilityProperty:      func(g *Generator, e *Expr) string { return e.Name() },
		KStream:                 func(g *Generator, e *Expr) string { return "STREAM " + this(g, e) },
		KStreamingTableProperty: func(g *Generator, e *Expr) string { return "STREAMING" },
		KStrictProperty:         func(g *Generator, e *Expr) string { return "STRICT" },
		KSwapTable:              func(g *Generator, e *Expr) string { return "SWAP WITH " + this(g, e) },
		KTableColumn:            func(g *Generator, e *Expr) string { return g.sql(e.Arg("this")) },
		KTags: func(g *Generator, e *Expr) string {
			return "TAG (" + g.expressions(e, exprsOpts{flat: true}) + ")"
		},
		KTemporaryProperty:     func(g *Generator, e *Expr) string { return "TEMPORARY" },
		KTitleColumnConstraint: func(g *Generator, e *Expr) string { return "TITLE " + this(g, e) },
		KToMap:                 func(g *Generator, e *Expr) string { return "MAP " + this(g, e) },
		KToTableProperty:       func(g *Generator, e *Expr) string { return "TO " + g.sql(e.Arg("this")) },
		KTransformModelProperty: func(g *Generator, e *Expr) string {
			return g.fn("TRANSFORM", exprList(e.Expressions())...)
		},
		KTransientProperty:         func(g *Generator, e *Expr) string { return "TRANSIENT" },
		KVirtualProperty:           func(g *Generator, e *Expr) string { return "VIRTUAL" },
		KTriggerExecute:            func(g *Generator, e *Expr) string { return "EXECUTE FUNCTION " + this(g, e) },
		KUnion:                     func(g *Generator, e *Expr) string { return g.setOperations(e) },
		KUnloggedProperty:          func(g *Generator, e *Expr) string { return "UNLOGGED" },
		KUsingTemplateProperty:     func(g *Generator, e *Expr) string { return "USING TEMPLATE " + this(g, e) },
		KUsingData:                 func(g *Generator, e *Expr) string { return "USING DATA " + this(g, e) },
		KUppercaseColumnConstraint: func(g *Generator, e *Expr) string { return "UPPERCASE" },
		KUtcDate: func(g *Generator, e *Expr) string {
			return g.sql(New(KCurrentDate, "this", LiteralString("UTC")))
		},
		KUtcTime: func(g *Generator, e *Expr) string {
			return g.sql(New(KCurrentTime, "this", LiteralString("UTC")))
		},
		KUtcTimestamp: func(g *Generator, e *Expr) string {
			return g.sql(New(KCurrentTimestamp, "this", LiteralString("UTC")))
		},
		KVariadic:                 func(g *Generator, e *Expr) string { return "VARIADIC " + this(g, e) },
		KVarMap:                   func(g *Generator, e *Expr) string { return g.fn("MAP", e.Arg("keys"), e.Arg("values")) },
		KViewAttributeProperty:    func(g *Generator, e *Expr) string { return "WITH " + this(g, e) },
		KVolatileProperty:         func(g *Generator, e *Expr) string { return "VOLATILE" },
		KWithJournalTableProperty: func(g *Generator, e *Expr) string { return "WITH JOURNAL TABLE=" + this(g, e) },
		KWithProcedureOptions: func(g *Generator, e *Expr) string {
			return "WITH " + g.expressions(e, exprsOpts{flat: true})
		},
		KWithSchemaBindingProperty: func(g *Generator, e *Expr) string { return "WITH SCHEMA " + this(g, e) },
		KWithOperator: func(g *Generator, e *Expr) string {
			return this(g, e) + " WITH " + g.sqlKey(e, "op")
		},
		KForceProperty: func(g *Generator, e *Expr) string { return "FORCE" },
	} {
		t[k] = f
	}
	return t
}

// newBaseGeneratorSettings returns the resolved settings of sqlglot.generator.Generator as seen
// through the base Dialect (i.e. after the Dialect metaclass pruned the JSONPath transforms).
func newBaseGeneratorSettings() *GeneratorSettings {
	ahm, ahmKeys := baseAfterHavingModifierTransforms()
	s := &GeneratorSettings{
		GeneratorData:                         generatorSettings_base(),
		TRANSFORMS:                            baseGeneratorTransforms(),
		methods:                               baseGeneratorMethods(),
		AFTER_HAVING_MODIFIER_TRANSFORMS:      ahm,
		AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS: ahmKeys,
		h:                                     defaultGeneratorHooks(),
	}
	pruneJSONPathTransforms(s)
	s.buildDispatch()
	return s
}

// pruneJSONPathTransforms mirrors the Dialect metaclass step that removes the TRANSFORMS
// entries of JSONPathPart expressions that are not in SUPPORTED_JSON_PATH_PARTS.
func pruneJSONPathTransforms(s *GeneratorSettings) {
	for _, part := range allJSONPathPartKinds {
		if !s.SUPPORTED_JSON_PATH_PARTS.Has(part) {
			delete(s.TRANSFORMS, part)
		}
	}
}

// cloneGeneratorSettings returns a copy of s whose maps and slices can be mutated without
// affecting s (mirrors `{**Parent.TRANSFORMS, ...}`-style subclassing). Hooks are copied by value.
// The dispatch table is not copied: the caller must call buildDispatch after customizing.
func cloneGeneratorSettings(s *GeneratorSettings) *GeneratorSettings {
	c := &GeneratorSettings{
		GeneratorData:      cloneGeneratorData(s.GeneratorData),
		UNICODE_SUBSTITUTE: s.UNICODE_SUBSTITUTE,
		h:                  s.h,
	}
	if s.TRANSFORMS != nil {
		c.TRANSFORMS = make(map[Kind]GenFunc, len(s.TRANSFORMS))
		for k, f := range s.TRANSFORMS {
			c.TRANSFORMS[k] = f
		}
	}
	if s.methods != nil {
		c.methods = make(map[Kind]GenFunc, len(s.methods))
		for k, f := range s.methods {
			c.methods[k] = f
		}
	}
	if s.AFTER_HAVING_MODIFIER_TRANSFORMS != nil {
		c.AFTER_HAVING_MODIFIER_TRANSFORMS = make(map[string]GenFunc, len(s.AFTER_HAVING_MODIFIER_TRANSFORMS))
		for k, f := range s.AFTER_HAVING_MODIFIER_TRANSFORMS {
			c.AFTER_HAVING_MODIFIER_TRANSFORMS[k] = f
		}
	}
	if s.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS != nil {
		c.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = append([]string{}, s.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS...)
	}
	return c
}

// cloneGeneratorData copies the data struct, duplicating its top-level map and slice fields.
func cloneGeneratorData(d *GeneratorData) *GeneratorData {
	if d == nil {
		return nil
	}
	c := *d
	v := reflect.ValueOf(&c).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if !f.CanSet() {
			continue
		}
		switch f.Kind() {
		case reflect.Map:
			if f.IsNil() {
				continue
			}
			m := reflect.MakeMapWithSize(f.Type(), f.Len())
			iter := f.MapRange()
			for iter.Next() {
				m.SetMapIndex(iter.Key(), iter.Value())
			}
			f.Set(m)
		case reflect.Slice:
			if f.IsNil() {
				continue
			}
			sl := reflect.MakeSlice(f.Type(), f.Len(), f.Len())
			reflect.Copy(sl, f)
			f.Set(sl)
		}
	}
	return &c
}

var (
	genTablesBaseTypeMappingOnce sync.Once
	genTablesBaseTypeMapping     map[DType]string
)

// genTablesCast mirrors exp.cast(expression, to) with copy=True and dialect=None, for a DType
// target (DataType.build(DType) == DataType(this=dtype)).
func genTablesCast(expression any, to DType) *Expr {
	expr := genTablesMaybeParse(expression)
	dataType := NewDataType(to)

	// dont re-cast if the expression is already a cast to the correct type
	if expr.IsA(KCast) {
		genTablesBaseTypeMappingOnce.Do(func() {
			genTablesBaseTypeMapping = generatorSettings_base().TYPE_MAPPING
		})
		typeMapping := genTablesBaseTypeMapping
		lookup := func(d DType) string {
			if v, ok := typeMapping[d]; ok {
				return v
			}
			return dtypeValues[d]
		}
		existingTo := expr.ArgE("to")
		existingCastType := existingTo.DTypeOf()
		newCastType := dataType.DTypeOf()
		typesAreEquivalent := lookup(existingCastType) == lookup(newCastType)

		if genTablesIsType(existingTo, dataType) || typesAreEquivalent {
			return expr
		}
	}

	c := New(KCast, "this", expr, "to", dataType)
	c.SetType(dataType)
	return c
}

// genTablesMaybeParse mirrors exp.maybe_parse(sql_or_expression, copy=True) without a dialect.
func genTablesMaybeParse(x any) *Expr {
	switch v := x.(type) {
	case *Expr:
		if v != nil {
			return v.Copy()
		}
	case string:
		e, err := prototype("").ParseOne(v, nil)
		if err != nil {
			if pe, ok := err.(*ParseError); ok {
				panic(parsePanic{pe})
			}
			panic(err)
		}
		return e
	}
	panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
}

// genTablesIsType mirrors DataType.is_type(other) for a single DataType `other` without
// expressions or nullability (check_nullable=False).
func genTablesIsType(dataType, other *Expr) bool {
	if dataType == nil {
		return false
	}
	if dataType.DTypeOf() == DT_USERDEFINED || other.DTypeOf() == DT_USERDEFINED {
		return dataType.Equal(other)
	}
	return dataType.Arg("this") == other.Arg("this")
}
