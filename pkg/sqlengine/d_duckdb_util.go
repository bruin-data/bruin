package sqlengine

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync"
)

// duckdbFunc builds a function call by name with the default dialect, using the registered
// function builder when there is one and an Anonymous call otherwise. Args may be *Expr (copied),
// string (parsed as SQL) or int (parsed from its string form); nil panics with a ParseError.
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
			converted = append(converted, MaybeParse(strconv.Itoa(x), KNone, "", d))
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

// duckdbConvert turns a Go value into an expression: nil as NULL, and strings, bools and ints as
// literals; expressions are copied when copy is set.
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

// duckdbBinop builds the binary expression `self klass other` (operands swapped when reverse is
// set) over a copy of self and other converted with duckdbConvert. Binary operands are
// parenthesized unless either side already has kind klass.
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

func duckdbGetItem(a *Expr, index any) *Expr {
	return New(KBracket, "this", a.Copy(), "expressions", []*Expr{duckdbConvert(index, true)})
}

// duckdbMaybeParse turns a CASE builder input into an expression: expressions as is (copied when
// copy is set), strings parsed as SQL, ints parsed from their string form; nil panics.
func duckdbMaybeParse(v any, copy bool) *Expr {
	switch x := v.(type) {
	case *Expr:
		return maybeParseExpr(x, copy)
	case string:
		return MaybeParse(x, KNone, "", nil)
	case int:
		return MaybeParse(strconv.Itoa(x), KNone, "", nil)
	case nil:
		panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
	}
	return MaybeParse(fmt.Sprint(v), KNone, "", nil)
}

func duckdbCase() *Expr { return CaseExpr(nil, true) }

func duckdbWhen(c *Expr, condition, then any, copy bool) *Expr {
	instance := maybeCopy(c, copy)
	instance.Append("ifs", New(
		KIf,
		"this", duckdbMaybeParse(condition, copy),
		"true", duckdbMaybeParse(then, copy),
	))
	return instance
}

func duckdbElse(c *Expr, condition any, copy bool) *Expr {
	instance := maybeCopy(c, copy)
	instance.Set("default", duckdbMaybeParse(condition, copy))
	return instance
}

// duckdbReplacePlaceholders returns a copy of expression with each named placeholder (:name)
// replaced by kwargs[name], when present.
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

// duckdbIsType reports whether e is one of dtypes: a DataType node is compared structurally, a
// cast by its target type, and any other expression by its annotated type.
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

func duckdbTypes(sets []DTypeSet, extra ...DType) []DType {
	var out []DType
	for _, s := range sets {
		out = append(out, s.Items()...)
	}
	return append(out, extra...)
}

func duckdbToPyInt(e *Expr) (int, bool) {
	v, isInt := e.toPyNumber()
	if v == nil || !isInt {
		return 0, false
	}
	i, _ := v.Int64()
	return int(i), true
}

func duckdbArgInt(e *Expr, key string, def int) int {
	switch v := e.Arg(key).(type) {
	case int:
		return v
	case bool:
		// A boolean counts as 0 or 1.
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

// duckdbDataTypeFromStr parses the type string s in dialect d (nil for the default dialect).
func duckdbDataTypeFromStr(s string, d *Dialect) *Expr { return dataTypeFromStr(s, d, false) }

func duckdbNewTypeExpr(dt DType) *Expr { return New(KDataType, "this", dt) }

func duckdbPow2(bits int) string {
	return new(big.Int).Lsh(big.NewInt(1), uint(bits)).String()
}

// duckdbDecimalDivStr formats i/n as a decimal number with at most 28 significant digits
// (rounded half to even): exact results keep their natural scale, e.g. "0.25".
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
	// Decimal to-scientific-string formatting: plain notation unless the exponent is positive or
	// the adjusted exponent is below -6
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
		s += strconv.Itoa(adjusted)
	}
	if neg {
		s = "-" + s
	}
	return s
}

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

const (
	duckdbRandstrCharPool = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	duckdbRandstrSeed     = 123456
)

var (
	duckdbSeqBase     = duckdbTemplate("(ROW_NUMBER() OVER (ORDER BY 1) - 1)")
	duckdbSeqUnsigned = duckdbTemplate(":base % :max_val")
	duckdbSeqSigned   = duckdbTemplate(
		"(CASE WHEN :base % :max_val >= :half " +
			"THEN :base % :max_val - :max_val " +
			"ELSE :base % :max_val END)",
	)

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
	// mean + (stddev * sqrt(-2 * ln(u1)) * cos(2 * pi * u2)).
	duckdbNormalTemplate = duckdbTemplate(
		":mean + (:stddev * SQRT(-2 * LN(GREATEST(:u1, 1e-10))) * COS(2 * PI() * :u2))",
	)

	duckdbSeededRandomTemplate = duckdbTemplate("(ABS(HASH(:seed)) % 1000000) / 1000000.0")

	// Template for MAP_CAT transpilation - Snowflake semantics:
	// 1. Returns NULL if either input is NULL
	// 2. For duplicate keys, prefers non-NULL value (COALESCE(m2[k], m1[k]))
	// 3. Filters out entries with NULL values from the result.
	duckdbMapcatTemplate = duckdbTemplate(`
        CASE
            WHEN :map1 IS NULL OR :map2 IS NULL THEN NULL
            ELSE MAP_FROM_ENTRIES(LIST_FILTER(LIST_TRANSFORM(
                LIST_DISTINCT(LIST_CONCAT(MAP_KEYS(:map1), MAP_KEYS(:map2))),
                __k -> STRUCT_PACK(key := __k, value := COALESCE(:map2[__k], :map1[__k]))
            ), __x -> __x.value IS NOT NULL))
        END
        `)

	// Template for BITMAP_CONSTRUCT_AGG transpilation: a binary bitmap of the sorted distinct
	// values (each 0-32767, as little-endian 16-bit words), or NULL for an empty input or
	// out-of-range values.
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

	duckdbMinhashTemplate = duckdbTemplate(`
        SELECT JSON_OBJECT('state', LIST(min_h ORDER BY seed), 'type', 'minhash', 'version', 1)
        FROM (
            SELECT seed, LIST_MIN(LIST_TRANSFORM(vals, __v -> HASH(CAST(__v AS VARCHAR) || CAST(seed AS VARCHAR)))) AS min_h
            FROM (SELECT LIST(:expr) AS vals), RANGE(0, :k) AS t(seed)
        )
        `)

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
                PRINTF('%02x', CAST('0x' || SUBSTR(h, 17, 2) AS INT) & 63 | 128) || SUBSTR(h, 19, 2) || '-' ||
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
