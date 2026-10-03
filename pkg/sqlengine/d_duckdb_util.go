package sqlengine

// Local ports of sqlglot expression builders / helpers used by the DuckDB generator
// (prefixed `duckdb`), plus the lazily parsed class-level templates of
// sqlglot/generators/duckdb.py.

import (
	"fmt"
	"math/big"
	"strings"
	"sync"
)

// duckdbFunc mirrors exp.func(name, *args) with the default dialect (copy=True). Args may be
// *Expr, string (parsed as SQL) or int (parsed from its string form).
func duckdbFunc(name string, args ...any) *Expr {
	d := MustDialect("")

	converted := make([]*Expr, 0, len(args))
	for _, a := range args {
		switch x := a.(type) {
		case *Expr:
			converted = append(converted, maybeParseExpr(x, true))
		case string:
			converted = append(converted, MaybeParse(x, KNone, "", d))
		case int:
			converted = append(converted, MaybeParse(fmt.Sprint(x), KNone, "", d))
		case nil:
			panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
		default:
			converted = append(converted, MaybeParse(fmt.Sprint(x), KNone, "", d))
		}
	}

	var function *Expr
	constructor := d.P.FUNCTIONS[pyUpper(name)]
	if constructor != nil {
		if len(converted) > 0 {
			function, converted = callFuncBuilder(constructor, converted, d)
		} else if kind, ok := FUNCTION_BY_NAME[pyUpper(name)]; ok {
			function = New(kind)
		} else {
			panic(&ValueError{Msg: fmt.Sprintf("Unable to convert '%s' into a Func. Either manually construct the Func expression of interest or parse the function call.", name)})
		}
	} else {
		function = New(KAnonymous, "this", name, "expressions", converted)
	}

	for _, msg := range function.ErrorMessages(converted) {
		panic(&ValueError{Msg: msg})
	}

	return function
}

// duckdbConvert mirrors exp.convert(value, copy).
func duckdbConvert(v any, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		if x == nil {
			return Null()
		}
		if copy {
			return x.Copy()
		}
		return x
	case string:
		return LiteralString(x)
	case bool:
		return Boolean(x)
	case nil:
		return Null()
	case int:
		return LiteralInt(x)
	}
	panic(&ValueError{Msg: fmt.Sprintf("Cannot convert %v", v)})
}

// duckdbBinop mirrors Expr._binop(klass, other, reverse) (the Python operator overloads).
func duckdbBinop(klass Kind, self *Expr, other any, reverse bool) *Expr {
	this := self.Copy()
	o := duckdbConvert(other, true)
	if !this.IsA(klass) && !o.IsA(klass) {
		this = wrapIfKind(this, KBinary)
		o = wrapIfKind(o, KBinary)
	}
	if reverse {
		return New(klass, "this", o, "expression", this)
	}
	return New(klass, "this", this, "expression", o)
}

func duckdbAdd(a *Expr, b any) *Expr    { return duckdbBinop(KAdd, a, b, false) }
func duckdbSub(a *Expr, b any) *Expr    { return duckdbBinop(KSub, a, b, false) }
func duckdbMul(a *Expr, b any) *Expr    { return duckdbBinop(KMul, a, b, false) }
func duckdbDiv(a *Expr, b any) *Expr    { return duckdbBinop(KDiv, a, b, false) }
func duckdbIntDiv(a *Expr, b any) *Expr { return duckdbBinop(KIntDiv, a, b, false) }
func duckdbMod(a *Expr, b any) *Expr    { return duckdbBinop(KMod, a, b, false) }
func duckdbLT(a *Expr, b any) *Expr     { return duckdbBinop(KLT, a, b, false) }
func duckdbEQ(a *Expr, b any) *Expr     { return duckdbBinop(KEQ, a, b, false) }
func duckdbIs(a *Expr, b any) *Expr     { return duckdbBinop(KIs, a, b, false) }

// duckdbGetItem mirrors Expr.__getitem__ (a[index]).
func duckdbGetItem(a *Expr, index any) *Expr {
	return New(KBracket, "this", a.Copy(), "expressions", []*Expr{duckdbConvert(index, true)})
}

// duckdbMaybeParse mirrors exp.maybe_parse(value, copy=copy) for Case builder inputs.
func duckdbMaybeParse(v any, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		return maybeParseExpr(x, copy)
	case string:
		return MaybeParse(x, KNone, "", nil)
	case int:
		return MaybeParse(fmt.Sprint(x), KNone, "", nil)
	case nil:
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	return MaybeParse(fmt.Sprint(v), KNone, "", nil)
}

// duckdbCase mirrors exp.case().
func duckdbCase() *Expr { return CaseExpr(nil, true) }

// duckdbWhen mirrors Case.when(condition, then, copy=copy).
func duckdbWhen(c *Expr, condition, then any, copy bool) *Expr {
	instance := maybeCopy(c, copy)
	instance.Append("ifs", New(
		KIf,
		"this", duckdbMaybeParse(condition, copy),
		"true", duckdbMaybeParse(then, copy),
	))
	return instance
}

// duckdbElse mirrors Case.else_(condition, copy=copy).
func duckdbElse(c *Expr, condition any, copy bool) *Expr {
	instance := maybeCopy(c, copy)
	instance.Set("default", duckdbMaybeParse(condition, copy))
	return instance
}

// duckdbReplacePlaceholders mirrors exp.replace_placeholders(expression, **kwargs).
func duckdbReplacePlaceholders(expression *Expr, kwargs map[string]*Expr) *Expr {
	return expression.Transform(func(node *Expr) *Expr {
		if node.IsA(KPlaceholder) {
			if name := node.ThisS(); name != "" {
				if newName, ok := kwargs[name]; ok && newName != nil {
					return newName
				}
			}
		}
		return node
	}, true)
}

// duckdbIsType mirrors Expr.is_type(*dtypes).
func duckdbIsType(e *Expr, dtypes ...DType) bool {
	if e == nil {
		return false
	}
	args := make([]any, len(dtypes))
	for i, d := range dtypes {
		args[i] = d
	}
	switch propOwner_is_type[e.kind] {
	case KDataType:
		return DataTypeIsType(e, args, false)
	case KCast:
		to := e.ArgE("to")
		return to != nil && DataTypeIsType(to, args, false)
	}
	t := e.RawType()
	return t != nil && DataTypeIsType(t, args, false)
}

// duckdbTypes concatenates data type sets (mirrors `*exp.DataType.X_TYPES, ...`).
func duckdbTypes(sets []DTypeSet, extra ...DType) []DType {
	var out []DType
	for _, s := range sets {
		out = append(out, s.Items()...)
	}
	return append(out, extra...)
}

// duckdbToPyInt mirrors int(expr.to_py()) for an integer literal; ok=false otherwise.
func duckdbToPyInt(e *Expr) (int, bool) {
	v, isInt := e.toPyNumber()
	if v == nil || !isInt {
		return 0, false
	}
	i, _ := v.Int64()
	return int(i), true
}

// duckdbArgInt returns an int-valued arg (e.g. Bracket/ArrayInsert "offset"), or def when unset.
func duckdbArgInt(e *Expr, key string, def int) int {
	switch v := e.Arg(key).(type) {
	case int:
		return v
	case bool:
		// Python bools are ints (True == 1).
		if v {
			return 1
		}
		return 0
	case *Expr:
		if i, ok := duckdbToPyInt(v); ok {
			return i
		}
	}
	return def
}

// duckdbDataTypeFromStr mirrors exp.DataType.from_str(s, dialect=d) (d nil = default dialect).
func duckdbDataTypeFromStr(s string, d *Dialect) *Expr { return dataTypeFromStr(s, d, false) }

// duckdbNewTypeExpr mirrors exp.DType.X.into_expr() / exp.DataType(this=X).
func duckdbNewTypeExpr(dt DType) *Expr { return New(KDataType, "this", dt) }

// duckdbPow2 returns str(2**bits).
func duckdbPow2(bits int) string {
	return new(big.Int).Lsh(big.NewInt(1), uint(bits)).String()
}

// duckdbDecimalDivStr mirrors str(Decimal(i) / Decimal(n)) (default context, 28 digits).
func duckdbDecimalDivStr(i, n int) string {
	if i == 0 {
		return "0"
	}
	neg := (i < 0) != (n < 0)
	if i < 0 {
		i = -i
	}
	if n < 0 {
		n = -n
	}
	num := big.NewInt(int64(i))
	den := big.NewInt(int64(n))

	// Exact result if the reduced denominator only has factors 2 and 5 (and fits in 28 digits).
	r := new(big.Rat).SetFrac(num, den)
	d := new(big.Int).Set(r.Denom())
	two, five := big.NewInt(2), big.NewInt(5)
	scale := 0
	mod := new(big.Int)
	twos, fives := 0, 0
	for {
		if mod.Mod(d, two).Sign() == 0 {
			d.Div(d, two)
			twos++
			continue
		}
		break
	}
	for {
		if mod.Mod(d, five).Sign() == 0 {
			d.Div(d, five)
			fives++
			continue
		}
		break
	}
	var digits string
	exact := d.Cmp(big.NewInt(1)) == 0
	if exact {
		scale = twos
		if fives > scale {
			scale = fives
		}
		coef := new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
		coef.Div(coef, r.Denom())
		digits = coef.String()
		if len(digits) > 28 {
			exact = false
		}
	}
	if !exact {
		// 28 significant digits, ROUND_HALF_EVEN.
		// Find exponent such that num/den * 10^scale has 28 integer digits.
		scale = 0
		q := new(big.Int)
		for {
			p := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
			q.Div(p, den)
			if len(q.String()) >= 28 {
				break
			}
			scale++
		}
		p := new(big.Int).Mul(num, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
		q, rem := new(big.Int).QuoRem(p, den, new(big.Int))
		// rounding
		twice := new(big.Int).Mul(rem, big.NewInt(2))
		cmp := twice.Cmp(den)
		if cmp > 0 || (cmp == 0 && q.Bit(0) == 1) {
			q.Add(q, big.NewInt(1))
		}
		digits = q.String()
		if len(digits) > 28 {
			digits = digits[:28]
			scale--
		}
	}
	// Python Decimal.__str__ (to-scientific-string)
	exp := -scale
	adjusted := exp + len(digits) - 1
	var s string
	if exp <= 0 && adjusted >= -6 {
		if exp == 0 {
			s = digits
		} else if len(digits) > -exp {
			s = digits[:len(digits)+exp] + "." + digits[len(digits)+exp:]
		} else {
			s = "0." + strings.Repeat("0", -exp-len(digits)) + digits
		}
	} else {
		s = digits[:1]
		if len(digits) > 1 {
			s += "." + digits[1:]
		}
		s += "E"
		if adjusted >= 0 {
			s += "+"
		}
		s += fmt.Sprint(adjusted)
	}
	if neg {
		s = "-" + s
	}
	return s
}

// ---------------------------------------------------------------------------------------------
// Class-level templates (exp.maybe_parse(...) at import time), parsed lazily once with the
// default dialect.
// ---------------------------------------------------------------------------------------------

type duckdbLazyTemplate struct {
	once sync.Once
	sql  string
	e    *Expr
}

func (t *duckdbLazyTemplate) get() *Expr {
	t.once.Do(func() { t.e = MaybeParse(t.sql, KNone, "", nil) })
	return t.e
}

func duckdbTemplate(sql string) *duckdbLazyTemplate { return &duckdbLazyTemplate{sql: sql} }

// RANDSTR transpilation constants
const (
	duckdbRandstrCharPool = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	duckdbRandstrSeed     = 123456
)

var (
	// SEQ function constants
	duckdbSeqBase = duckdbTemplate("(ROW_NUMBER() OVER (ORDER BY 1) - 1)")
	// Template for generating signed and unsigned SEQ values within a specified range
	duckdbSeqUnsigned = duckdbTemplate(":base % :max_val")
	duckdbSeqSigned   = duckdbTemplate(
		"(CASE WHEN :base % :max_val >= :half " +
			"THEN :base % :max_val - :max_val " +
			"ELSE :base % :max_val END)",
	)

	// Template for ZIPF transpilation - placeholders get replaced with actual parameters
	duckdbZipfTemplate = duckdbTemplate(`
        WITH rand AS (SELECT :random_expr AS r),
        weights AS (
            SELECT i, 1.0 / POWER(i, :s) AS w
            FROM RANGE(1, :n + 1) AS t(i)
        ),
        cdf AS (
            SELECT i, SUM(w) OVER (ORDER BY i) / SUM(w) OVER () AS p
            FROM weights
        )
        SELECT MIN(i)
        FROM cdf
        WHERE p >= (SELECT r FROM rand)
        `)

	// Template for NORMAL transpilation using Box-Muller transform
	// mean + (stddev * sqrt(-2 * ln(u1)) * cos(2 * pi * u2))
	duckdbNormalTemplate = duckdbTemplate(
		":mean + (:stddev * SQRT(-2 * LN(GREATEST(:u1, 1e-10))) * COS(2 * PI() * :u2))",
	)

	// Template for generating a seeded pseudo-random value in [0, 1) from a hash
	duckdbSeededRandomTemplate = duckdbTemplate("(ABS(HASH(:seed)) % 1000000) / 1000000.0")

	// Template for MAP_CAT transpilation - Snowflake semantics:
	// 1. Returns NULL if either input is NULL
	// 2. For duplicate keys, prefers non-NULL value (COALESCE(m2[k], m1[k]))
	// 3. Filters out entries with NULL values from the result
	duckdbMapcatTemplate = duckdbTemplate(`
        CASE
            WHEN :map1 IS NULL OR :map2 IS NULL THEN NULL
            ELSE MAP_FROM_ENTRIES(LIST_FILTER(LIST_TRANSFORM(
                LIST_DISTINCT(LIST_CONCAT(MAP_KEYS(:map1), MAP_KEYS(:map2))),
                __k -> STRUCT_PACK(key := __k, value := COALESCE(:map2[__k], :map1[__k]))
            ), __x -> __x.value IS NOT NULL))
        END
        `)

	// Template for BITMAP_CONSTRUCT_AGG transpilation (see generators/duckdb.py for the format)
	duckdbBitmapConstructAggTemplate = duckdbTemplate(`
        SELECT CASE
            WHEN l IS NULL OR LENGTH(l) = 0 THEN NULL
            WHEN LENGTH(l) != LENGTH(LIST_FILTER(l, __v -> __v BETWEEN 0 AND 32767)) THEN NULL
            WHEN LENGTH(l) < 5 THEN UNHEX(PRINTF('%04X', LENGTH(l)) || h || REPEAT('00', GREATEST(0, 4 - LENGTH(l)) * 2))
            ELSE UNHEX('08000000000000000000' || h)
        END
        FROM (
            SELECT l, COALESCE(LIST_REDUCE(
                LIST_TRANSFORM(l, __x -> PRINTF('%02X%02X', CAST(__x AS INT) & 255, (CAST(__x AS INT) >> 8) & 255)),
                (__a, __b) -> __a || __b, ''
            ), '') AS h
            FROM (SELECT LIST_SORT(LIST_DISTINCT(LIST(:arg) FILTER(NOT :arg IS NULL))) AS l)
        )
        `)

	// Template for RANDSTR transpilation - placeholders get replaced with actual parameters
	duckdbRandstrTemplate = duckdbTemplate(`
        SELECT LISTAGG(
            SUBSTRING(
                '` + duckdbRandstrCharPool + `',
                1 + CAST(FLOOR(random_value * 62) AS INT),
                1
            ),
            ''
        )
        FROM (
            SELECT (ABS(HASH(i + :seed)) % 1000) / 1000.0 AS random_value
            FROM RANGE(:length) AS t(i)
        )
        `)

	// Template for MINHASH transpilation
	duckdbMinhashTemplate = duckdbTemplate(`
        SELECT JSON_OBJECT('state', LIST(min_h ORDER BY seed), 'type', 'minhash', 'version', 1)
        FROM (
            SELECT seed, LIST_MIN(LIST_TRANSFORM(vals, __v -> HASH(CAST(__v AS VARCHAR) || CAST(seed AS VARCHAR)))) AS min_h
            FROM (SELECT LIST(:expr) AS vals), RANGE(0, :k) AS t(seed)
        )
        `)

	// Template for MINHASH_COMBINE transpilation
	duckdbMinhashCombineTemplate = duckdbTemplate(`
        SELECT JSON_OBJECT('state', LIST(min_h ORDER BY idx), 'type', 'minhash', 'version', 1)
        FROM (
            SELECT
                pos AS idx,
                MIN(val) AS min_h
            FROM
                UNNEST(LIST(:expr)) AS _(sig),
                UNNEST(CAST(sig -> 'state' AS UBIGINT[])) WITH ORDINALITY AS t(val, pos)
            GROUP BY pos
        )
        `)

	// Template for APPROXIMATE_SIMILARITY transpilation
	duckdbApproximateSimilarityTemplate = duckdbTemplate(`
        SELECT CAST(SUM(CASE WHEN num_distinct = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*)
        FROM (
            SELECT pos, COUNT(DISTINCT h) AS num_distinct
            FROM (
                SELECT h, pos
                FROM UNNEST(LIST(:expr)) AS _(sig),
                     UNNEST(CAST(sig -> 'state' AS UBIGINT[])) WITH ORDINALITY AS s(h, pos)
            )
            GROUP BY pos
        )
        `)

	// Template for ARRAYS_ZIP transpilation
	duckdbArraysZipTemplate = duckdbTemplate(`
        CASE WHEN :null_check THEN NULL
        WHEN :all_empty_check THEN [:empty_struct]
        ELSE LIST_TRANSFORM(RANGE(0, :max_len), __i -> :transform_struct)
        END
        `)

	duckdbUUIDV5Template = duckdbTemplate(`
        (SELECT
            LOWER(
                SUBSTR(h, 1, 8) || '-' ||
                SUBSTR(h, 9, 4) || '-' ||
                '5' || SUBSTR(h, 14, 3) || '-' ||
                FORMAT('{:02x}', CAST('0x' || SUBSTR(h, 17, 2) AS INT) & 63 | 128) || SUBSTR(h, 19, 2) || '-' ||
                SUBSTR(h, 21, 12)
            )
        FROM (
            SELECT SUBSTR(SHA1(UNHEX(REPLACE(:namespace, '-', '')) || ENCODE(:name, 'utf8')), 1, 32) AS h
        ))
        `)

	// Shared bag semantics outer frame for ARRAY_EXCEPT and ARRAY_INTERSECTION.
	duckdbArrayBagTemplate = duckdbTemplate(`
        CASE
            WHEN :arr1 IS NULL OR :arr2 IS NULL THEN NULL
            ELSE LIST_TRANSFORM(
                LIST_FILTER(
                    LIST_ZIP(:arr1, GENERATE_SERIES(1, LEN(:arr1))),
                    pair -> :cond
                ),
                pair -> pair[0]
            )
        END
        `)

	duckdbArrayExceptCondition = duckdbTemplate(
		"LEN(LIST_FILTER(:arr1[1:pair[1]], e -> e IS NOT DISTINCT FROM pair[0]))" +
			" > LEN(LIST_FILTER(:arr2, e -> e IS NOT DISTINCT FROM pair[0]))",
	)

	duckdbArrayIntersectionCondition = duckdbTemplate(
		"LEN(LIST_FILTER(:arr1[1:pair[1]], e -> e IS NOT DISTINCT FROM pair[0]))" +
			" <= LEN(LIST_FILTER(:arr2, e -> e IS NOT DISTINCT FROM pair[0]))",
	)

	// Set semantics for ARRAY_EXCEPT.
	duckdbArrayExceptSetTemplate = duckdbTemplate(`
        CASE
            WHEN :arr1 IS NULL OR :arr2 IS NULL THEN NULL
            ELSE LIST_FILTER(
                LIST_DISTINCT(:arr1),
                e -> LEN(LIST_FILTER(:arr2, x -> x IS NOT DISTINCT FROM e)) = 0
            )
        END
        `)

	duckdbStrtokToArrayTemplate = duckdbTemplate(`
        CASE WHEN :delimiter IS NULL THEN NULL
        ELSE LIST_FILTER(
            REGEXP_SPLIT_TO_ARRAY(:string, CASE WHEN :delimiter = '' THEN '.^' ELSE CONCAT('[', :escaped, ']') END),
            x -> NOT x = ''
        ) END
        `)

	// Template for STRTOK function transpilation
	duckdbStrtokTemplate = duckdbTemplate(`
        CASE
            WHEN :delimiter = '' AND :string = '' THEN NULL
            WHEN :delimiter = '' AND :part_index = 1 THEN :string
            WHEN :delimiter = '' THEN NULL
            WHEN :part_index < 0 THEN NULL
            WHEN :string IS NULL OR :delimiter IS NULL OR :part_index IS NULL THEN NULL
            ELSE :base_func
        END
        `)
)
