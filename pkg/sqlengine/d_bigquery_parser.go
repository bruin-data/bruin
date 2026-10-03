package sqlengine

import (
	"strings"
)

// Port of sqlglot/parsers/bigquery.py (module-level builders and BigQueryParser).

// _build_contains_substring
func bigqueryBuildContainsSubstring(args []*Expr, _ *Dialect) *Expr {
	this := New(KLower, "this", seqGet(args, 0))
	expr := New(KLower, "this", seqGet(args, 1))
	return New(KContains, "this", this, "expression", expr, "json_scope", seqGet(args, 2))
}

// _build_date
func bigqueryBuildDate(args []*Expr, _ *Dialect) *Expr {
	exprType := KDate
	if len(args) == 3 {
		exprType = KDateFromParts
	}
	return FromArgList(exprType, args)
}

// build_date_diff
func bigqueryBuildDateDiff(exprType Kind) FuncBuilder {
	return func(args []*Expr, _ *Dialect) *Expr {
		expr := New(
			exprType,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"unit", seqGet(args, 2),
			"date_part_boundary", true,
		)

		unit := expr.ArgE("unit")
		if unit.IsA(KVar) && pyUpper(unit.Name()) == "WEEK" {
			expr.Set("unit", New(KWeekStart, "this", VarExpr("SUNDAY")))
		}

		return expr
	}
}

// _build_datetime
func bigqueryBuildDatetime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return FromArgList(KTsOrDsToDatetime, args)
	}
	if len(args) == 2 {
		return FromArgList(KDatetime, args)
	}
	return FromArgList(KTimestampFromParts, args)
}

// _build_extract_json_with_default_path
func bigqueryBuildExtractJSONWithDefaultPath(exprType Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		if len(args) == 1 {
			// args.append(...) mutates the caller's list: report it for arity validation
			args = append(args, LiteralString("$"))
			return WithValidateArgs(buildExtractJSONWithPath(exprType)(args, d), args)
		}
		return buildExtractJSONWithPath(exprType)(args, d)
	}
}

// _build_format_time
func bigqueryBuildFormatTime(exprType Kind) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		formattedTime := buildFormattedTime(KTimeToStr, "", nil)(
			[]*Expr{New(exprType, "this", seqGet(args, 1)), seqGet(args, 0)}, d,
		)
		formattedTime.Set("zone", seqGet(args, 2))
		return formattedTime
	}
}

// _build_json_strip_nulls
func bigqueryBuildJSONStripNulls(args []*Expr, _ *Dialect) *Expr {
	expression := New(KJSONStripNulls, "this", seqGet(args, 0))
	for _, a := range argsFrom(args, 1) {
		if a.IsA(KKwarg) {
			expression.Set(pyLower(a.This().Name()), a)
		} else {
			expression.Set("expression", a)
		}
	}
	return expression
}

// _build_levenshtein
func bigqueryBuildLevenshtein(args []*Expr, _ *Dialect) *Expr {
	maxDist := seqGet(args, 2)
	var md *Expr
	if maxDist != nil {
		md = maxDist.Expression()
	}
	return New(
		KLevenshtein,
		"this", seqGet(args, 0),
		"expression", seqGet(args, 1),
		"max_dist", md,
	)
}

// _build_parse_date
func bigqueryBuildParseDate(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KStrToDate, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("default_year", LiteralInt(1970))
	return this
}

// _build_parse_timestamp
func bigqueryBuildParseTimestamp(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KStrToTime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("zone", seqGet(args, 2))
	this.Set("default_year", LiteralInt(1970))
	return this
}

// _build_parse_datetime
func bigqueryBuildParseDatetime(args []*Expr, d *Dialect) *Expr {
	this := buildFormattedTime(KParseDatetime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	this.Set("default_year", LiteralInt(1970))
	return this
}

// _build_regexp_extract. defaultGroup nil means None.
func bigqueryBuildRegexpExtract(exprType Kind, defaultGroup func() *Expr /*=None*/) FuncBuilder {
	return func(args []*Expr, d *Dialect) *Expr {
		// group = re.compile(args[1].name).groups == 1 (False on re.error)
		n, ok := bigqueryPyRegexGroups(args[1].Name())
		group := ok && n == 1

		var groupArg *Expr
		if group {
			groupArg = LiteralInt(1)
		} else if defaultGroup != nil {
			groupArg = defaultGroup()
		}

		kv := []any{
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"position", seqGet(args, 2),
			"occurrence", seqGet(args, 3),
			"group", groupArg,
		}
		if exprType == KRegexpExtract {
			kv = append(kv, "null_if_pos_overflow", d.S.REGEXP_EXTRACT_POSITION_OVERFLOW_RETURNS_NULL)
		}
		return New(exprType, kv...)
	}
}

// bigqueryPyRegexGroups approximates `re.compile(pattern).groups` of Python's re module: it
// returns the number of capturing groups, or ok=false where Python raises re.error (bad escapes,
// unbalanced parentheses, unterminated sets, unknown (?...) extensions, misplaced global flags,
// invalid group references and nothing-to-repeat/multiple-repeat errors).
func bigqueryPyRegexGroups(pattern string) (int, bool) {
	rs := []rune(pattern)
	n := len(rs)
	groups := 0       // capturing groups opened so far
	closedGroups := 0 // capturing groups closed so far (for backreference validation)
	names := map[string]bool{}
	var stack []bool // per open paren: is it a capturing group?
	isASCIILetter := func(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }
	isDigit := func(r rune) bool { return r >= '0' && r <= '9' }
	const validEscapes = "abBdDfnrsStvwWZAxuUN"
	const validClassEscapes = "abdDfnrsStvwWxuUN"
	const flagChars = "aiLmsux-"
	// canRepeat reports whether a repeat operator is allowed at this point (i.e. there is an item).
	canRepeat := false
	lastWasRepeat := false
	for i := 0; i < n; i++ {
		c := rs[i]
		wasRepeat := lastWasRepeat
		lastWasRepeat = false
		switch c {
		case '\\':
			if i+1 >= n {
				return 0, false // bad escape (end of pattern)
			}
			i++
			e := rs[i]
			if isASCIILetter(e) && !strings.ContainsRune(validEscapes, e) {
				return 0, false // bad escape
			}
			if e >= '1' && e <= '9' {
				num := int(e - '0')
				if i+1 < n && isDigit(rs[i+1]) {
					num = num*10 + int(rs[i+1]-'0')
					i++
				}
				if num > closedGroups {
					return 0, false // invalid group reference
				}
			}
			canRepeat = !(e == 'b' || e == 'B' || e == 'A' || e == 'Z')
		case '[':
			i++
			if i < n && rs[i] == '^' {
				i++
			}
			if i < n && rs[i] == ']' {
				i++
			}
			closed := false
			for ; i < n; i++ {
				if rs[i] == '\\' {
					if i+1 >= n {
						return 0, false
					}
					i++
					if isASCIILetter(rs[i]) && !strings.ContainsRune(validClassEscapes, rs[i]) {
						return 0, false
					}
					continue
				}
				if rs[i] == ']' {
					closed = true
					break
				}
			}
			if !closed {
				return 0, false // unterminated character set
			}
			canRepeat = true
		case '(':
			capturing := true
			if i+1 < n && rs[i+1] == '?' {
				capturing = false
				if i+2 >= n {
					return 0, false // unexpected end of pattern
				}
				switch x := rs[i+2]; {
				case x == ':' || x == '=' || x == '!' || x == '>':
					i += 2
				case x == '(':
					// conditional (?(id)yes|no): skip the condition
					j := i + 3
					for j < n && rs[j] != ')' {
						j++
					}
					if j >= n {
						return 0, false
					}
					i = j
				case x == '#':
					// comment group: skip to the closing parenthesis
					j := i + 3
					for j < n && rs[j] != ')' {
						j++
					}
					if j >= n {
						return 0, false // missing ), unterminated comment
					}
					i = j
					continue
				case x == '<':
					if i+3 >= n || (rs[i+3] != '=' && rs[i+3] != '!') {
						return 0, false // unknown extension ?<x
					}
					i += 3
				case x == 'P':
					if i+3 < n && rs[i+3] == '<' {
						j := i + 4
						for j < n && rs[j] != '>' {
							j++
						}
						if j >= n || j == i+4 {
							return 0, false // missing >, unterminated name / missing group name
						}
						names[string(rs[i+4:j])] = true
						capturing = true
						i = j
					} else if i+3 < n && rs[i+3] == '=' {
						j := i + 4
						for j < n && rs[j] != ')' {
							j++
						}
						if j >= n || !names[string(rs[i+4:j])] {
							return 0, false // unknown group name
						}
						i = j
						canRepeat = true
						continue
					} else {
						return 0, false // unknown extension ?P
					}
				case strings.ContainsRune(flagChars, x):
					j := i + 2
					for j < n && strings.ContainsRune(flagChars, rs[j]) {
						j++
					}
					if j >= n {
						return 0, false // missing -, : or )
					}
					if rs[j] == ')' {
						// global flags must be at the start of the expression
						if i != 0 {
							return 0, false
						}
						i = j
						canRepeat = false
						continue
					}
					if rs[j] != ':' {
						return 0, false // missing :
					}
					i = j
				default:
					return 0, false // unknown extension
				}
			}
			if capturing {
				groups++
			}
			stack = append(stack, capturing)
			canRepeat = false
		case ')':
			if len(stack) == 0 {
				return 0, false // unbalanced parenthesis
			}
			if stack[len(stack)-1] {
				closedGroups++
			}
			stack = stack[:len(stack)-1]
			canRepeat = true
		case '|':
			canRepeat = false
		case '*', '+', '?':
			if wasRepeat {
				if c == '*' {
					return 0, false // multiple repeat
				}
				// lazy (`*?`) / possessive (`*+`) modifiers
				canRepeat = false
				continue
			}
			if !canRepeat {
				return 0, false // nothing to repeat
			}
			lastWasRepeat = true
			canRepeat = false
		case '{':
			// {m}, {m,}, {,n}, {m,n} is a repeat; anything else is a literal "{"
			j := i + 1
			for j < n && (isDigit(rs[j]) || rs[j] == ',') {
				j++
			}
			body := string(rs[i+1 : min(j, n)])
			if j < n && rs[j] == '}' && body != "" && body != "," && strings.Count(body, ",") <= 1 {
				if wasRepeat {
					return 0, false // multiple repeat
				}
				if !canRepeat {
					return 0, false // nothing to repeat
				}
				i = j
				lastWasRepeat = true
				canRepeat = false
				continue
			}
			canRepeat = true
		case '^', '$':
			canRepeat = false
		default:
			canRepeat = true
		}
	}
	if len(stack) != 0 {
		return 0, false // missing ), unterminated subpattern
	}
	return groups, true
}

// _build_time
func bigqueryBuildTime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 1 {
		return New(KTsOrDsToTime, "this", args[0])
	}
	if len(args) == 2 {
		return FromArgList(KTime, args)
	}
	return FromArgList(KTimeFromParts, args)
}

// _build_timestamp
func bigqueryBuildTimestamp(args []*Expr, _ *Dialect) *Expr {
	timestamp := FromArgList(KTimestamp, args)
	timestamp.Set("with_tz", true)
	return timestamp
}

// _build_to_hex
func bigqueryBuildToHex(args []*Expr, _ *Dialect) *Expr {
	arg := seqGet(args, 0)
	if arg.IsA(KMD5Digest) {
		return New(KMD5, "this", arg.This())
	}
	return New(KLowerHex, "this", arg)
}

// _DOMAIN_DOT: placeholder; cannot occur in a SQL identifier
const bigqueryDomainDot = "\x00"

// bigquerySplitNumWords mirrors helper.split_num_words(value, sep, min_num_words) (fill_from_start=True).
// nil entries are Python None.
func bigquerySplitNumWords(value, sep string, minNumWords int) []*string {
	words := strings.Split(value, sep)
	var out []*string
	for i := 0; i < minNumWords-len(words); i++ {
		out = append(out, nil)
	}
	for _, w := range words {
		w := w
		out = append(out, &w)
	}
	return out
}

// _split_qualified_name
func bigquerySplitQualifiedName(name string, minNumWords int) []*string {
	// A dotted reference (e.g. `project.dataset.table`) is split into a fixed number of parts,
	// the first of which is the project. Domain-scoped (legacy) project IDs have the form
	// `domain.com:project-id`, where the dots belong to the domain, not the path - and a project
	// ID itself can't contain dots, so every such dot precedes the colon. Mask those, then let
	// `split_num_words` split and pad as usual, to avoid corrupting the project ID.
	colon := strings.Index(name, ":")
	if colon != -1 && strings.Contains(name[:colon], ".") {
		name = strings.ReplaceAll(name[:colon], ".", bigqueryDomainDot) + name[colon:]
		parts := bigquerySplitNumWords(name, ".", minNumWords)
		for i, p := range parts {
			if p != nil && *p != "" {
				s := strings.ReplaceAll(*p, bigqueryDomainDot, ".")
				parts[i] = &s
			}
		}
		return parts
	}

	return bigquerySplitNumWords(name, ".", minNumWords)
}

// bigqueryQuotedIdentifiers mirrors `(exp.to_identifier(p, quoted=True) for p in parts)`.
func bigqueryQuotedIdentifiers(parts []*string) []*Expr {
	out := make([]*Expr, len(parts))
	for i, p := range parts {
		if p != nil {
			out[i] = ToIdentifier(*p, boolp(true))
		}
	}
	return out
}

func bigqueryJoinPartNames(parts []*Expr) string {
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.Name()
	}
	return strings.Join(names, ".")
}

var bigqueryMakeIntervalKwargs = []string{"year", "month", "day", "hour", "minute", "second"}

// BRACKET_OFFSETS
var bigqueryBracketOffsets = map[string]struct {
	offset int
	safe   bool
}{
	"OFFSET":       {0, false},
	"ORDINAL":      {1, false},
	"SAFE_OFFSET":  {0, true},
	"SAFE_ORDINAL": {1, true},
}

// bigqueryNoKwargs wraps a PROPERTY_PARSERS entry defined as `lambda self: ...` in BigQueryParser.
func bigqueryNoKwargs(fn func(p *Parser) any) propertyParseFn {
	return func(p *Parser, kw propKwargs) any {
		checkPropKwargs(kw, "BigQueryParser.<lambda>")
		return fn(p)
	}
}

func customizeBigQueryParser(d *Dialect) {
	P := d.P

	// FUNCTIONS
	delete(P.FUNCTIONS, "SEARCH")
	P.FUNCTIONS["APPROX_TOP_COUNT"] = fromArgList(KApproxTopK)
	P.FUNCTIONS["BIT_AND"] = fromArgList(KBitwiseAndAgg)
	P.FUNCTIONS["BIT_OR"] = fromArgList(KBitwiseOrAgg)
	P.FUNCTIONS["BIT_XOR"] = fromArgList(KBitwiseXorAgg)
	P.FUNCTIONS["BIT_COUNT"] = fromArgList(KBitwiseCount)
	P.FUNCTIONS["BOOL"] = fromArgList(KJSONBool)
	P.FUNCTIONS["CONTAINS_SUBSTR"] = bigqueryBuildContainsSubstring
	P.FUNCTIONS["DATE"] = bigqueryBuildDate
	P.FUNCTIONS["DATE_ADD"] = buildDateDeltaWithInterval(KDateAdd, "")
	P.FUNCTIONS["DATE_DIFF"] = bigqueryBuildDateDiff(KDateDiff)
	P.FUNCTIONS["DATE_SUB"] = buildDateDeltaWithInterval(KDateSub, "")
	P.FUNCTIONS["DATE_TRUNC"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KDateTrunc,
			"unit", seqGet(args, 1),
			"this", seqGet(args, 0),
			"zone", seqGet(args, 2),
		)
	}
	P.FUNCTIONS["DATETIME"] = bigqueryBuildDatetime
	P.FUNCTIONS["DATETIME_ADD"] = buildDateDeltaWithInterval(KDatetimeAdd, "")
	P.FUNCTIONS["DATETIME_DIFF"] = bigqueryBuildDateDiff(KDatetimeDiff)
	P.FUNCTIONS["DATETIME_SUB"] = buildDateDeltaWithInterval(KDatetimeSub, "")
	P.FUNCTIONS["DIV"] = binaryFromFunction(KIntDiv)
	P.FUNCTIONS["EDIT_DISTANCE"] = bigqueryBuildLevenshtein
	P.FUNCTIONS["EMBED"] = fromArgList(KAIEmbed)
	P.FUNCTIONS["FORMAT_DATE"] = bigqueryBuildFormatTime(KTsOrDsToDate)
	P.FUNCTIONS["GENERATE"] = fromArgList(KAIGenerate)
	P.FUNCTIONS["GENERATE_ARRAY"] = fromArgList(KGenerateSeries)
	P.FUNCTIONS["JSON_EXTRACT_SCALAR"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractScalar)
	P.FUNCTIONS["JSON_EXTRACT_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractArray)
	P.FUNCTIONS["JSON_EXTRACT_STRING_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONValueArray)
	P.FUNCTIONS["JSON_KEYS"] = fromArgList(KJSONKeysAtDepth)
	P.FUNCTIONS["JSON_QUERY"] = buildExtractJSONWithPath(KJSONExtract)
	P.FUNCTIONS["JSON_QUERY_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractArray)
	P.FUNCTIONS["JSON_STRIP_NULLS"] = bigqueryBuildJSONStripNulls
	P.FUNCTIONS["JSON_VALUE"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONExtractScalar)
	P.FUNCTIONS["JSON_VALUE_ARRAY"] = bigqueryBuildExtractJSONWithDefaultPath(KJSONValueArray)
	P.FUNCTIONS["LENGTH"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KLength, "this", seqGet(args, 0), "binary", true)
	}
	P.FUNCTIONS["MD5"] = fromArgList(KMD5Digest)
	P.FUNCTIONS["SHA1"] = fromArgList(KSHA1Digest)
	P.FUNCTIONS["NORMALIZE_AND_CASEFOLD"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KNormalize, "this", seqGet(args, 0), "form", seqGet(args, 1), "is_casefold", true)
	}
	P.FUNCTIONS["OCTET_LENGTH"] = fromArgList(KByteLength)
	P.FUNCTIONS["TO_HEX"] = bigqueryBuildToHex
	P.FUNCTIONS["PARSE_DATE"] = bigqueryBuildParseDate
	P.FUNCTIONS["PARSE_TIME"] = func(args []*Expr, d *Dialect) *Expr {
		return buildFormattedTime(KParseTime, "", nil)([]*Expr{seqGet(args, 1), seqGet(args, 0)}, d)
	}
	P.FUNCTIONS["PARSE_TIMESTAMP"] = bigqueryBuildParseTimestamp
	P.FUNCTIONS["PARSE_DATETIME"] = bigqueryBuildParseDatetime
	P.FUNCTIONS["REGEXP_CONTAINS"] = fromArgList(KRegexpLike)
	P.FUNCTIONS["REGEXP_EXTRACT"] = bigqueryBuildRegexpExtract(KRegexpExtract, nil)
	P.FUNCTIONS["REGEXP_SUBSTR"] = bigqueryBuildRegexpExtract(KRegexpExtract, nil)
	P.FUNCTIONS["REGEXP_EXTRACT_ALL"] = bigqueryBuildRegexpExtract(KRegexpExtractAll, func() *Expr { return LiteralInt(0) })
	P.FUNCTIONS["SHA256"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(256))
	}
	P.FUNCTIONS["SHA512"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(512))
	}
	P.FUNCTIONS["SIMILARITY"] = fromArgList(KAISimilarity)
	P.FUNCTIONS["SPLIT"] = func(args []*Expr, _ *Dialect) *Expr {
		// https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#split
		expression := seqGet(args, 1)
		if expression == nil {
			expression = LiteralString(",")
		}
		return New(KSplit, "this", seqGet(args, 0), "expression", expression)
	}
	P.FUNCTIONS["STRPOS"] = fromArgList(KStrPosition)
	P.FUNCTIONS["TIME"] = bigqueryBuildTime
	P.FUNCTIONS["TIME_ADD"] = buildDateDeltaWithInterval(KTimeAdd, "")
	P.FUNCTIONS["TIME_SUB"] = buildDateDeltaWithInterval(KTimeSub, "")
	P.FUNCTIONS["TIMESTAMP"] = bigqueryBuildTimestamp
	P.FUNCTIONS["TIMESTAMP_ADD"] = buildDateDeltaWithInterval(KTimestampAdd, "")
	P.FUNCTIONS["TIMESTAMP_SUB"] = buildDateDeltaWithInterval(KTimestampSub, "")
	P.FUNCTIONS["TIMESTAMP_MICROS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", LiteralInt(6)) // UnixToTime.MICROS
	}
	P.FUNCTIONS["TIMESTAMP_MILLIS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0), "scale", LiteralInt(3)) // UnixToTime.MILLIS
	}
	P.FUNCTIONS["TIMESTAMP_SECONDS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KUnixToTime, "this", seqGet(args, 0))
	}
	P.FUNCTIONS["TO_JSON"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONFormat, "this", seqGet(args, 0), "options", seqGet(args, 1), "to_json", true)
	}
	P.FUNCTIONS["TO_JSON_STRING"] = fromArgList(KJSONFormat)
	P.FUNCTIONS["FORMAT_DATETIME"] = bigqueryBuildFormatTime(KTsOrDsToDatetime)
	P.FUNCTIONS["FORMAT_TIMESTAMP"] = bigqueryBuildFormatTime(KTsOrDsToTimestamp)
	P.FUNCTIONS["FORMAT_TIME"] = bigqueryBuildFormatTime(KTsOrDsToTime)
	P.FUNCTIONS["FROM_HEX"] = fromArgList(KUnhex)
	P.FUNCTIONS["WEEK"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KWeekStart, "this", VarOf(seqGet(args, 0)))
	}

	// FUNCTION_PARSERS
	delete(P.FUNCTION_PARSERS, "TRIM")
	P.FUNCTION_PARSERS["ARRAY"] = func(p *Parser) *Expr {
		return p.expression(New(KArray, "expressions", []*Expr{p.parseStatement()}, "struct_name_inheritance", true))
	}
	P.FUNCTION_PARSERS["JSON_ARRAY"] = func(p *Parser) *Expr {
		return p.expression(New(KJSONArray, "expressions", p.parseCSV(p.parseBitwise, TK_COMMA)))
	}
	P.FUNCTION_PARSERS["MAKE_INTERVAL"] = func(p *Parser) *Expr { return bigqueryParseMakeInterval(p) }
	P.FUNCTION_PARSERS["PREDICT"] = func(p *Parser) *Expr { return bigqueryParseML(p, KPredict) }
	P.FUNCTION_PARSERS["TRANSLATE"] = func(p *Parser) *Expr { return bigqueryParseTranslate(p) }
	P.FUNCTION_PARSERS["FEATURES_AT_TIME"] = func(p *Parser) *Expr { return bigqueryParseFeaturesAtTime(p) }
	P.FUNCTION_PARSERS["GENERATE_EMBEDDING"] = func(p *Parser) *Expr { return bigqueryParseML(p, KGenerateEmbedding) }
	P.FUNCTION_PARSERS["GENERATE_TEXT_EMBEDDING"] = func(p *Parser) *Expr {
		return bigqueryParseML(p, KGenerateEmbedding, "is_text", true)
	}
	P.FUNCTION_PARSERS["GENERATE_TEXT"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateText) }
	P.FUNCTION_PARSERS["GENERATE_TABLE"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateTable) }
	P.FUNCTION_PARSERS["GENERATE_BOOL"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateBool) }
	P.FUNCTION_PARSERS["GENERATE_INT"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateInt) }
	P.FUNCTION_PARSERS["GENERATE_DOUBLE"] = func(p *Parser) *Expr { return bigqueryParseGenerate(p, KGenerateDouble) }
	P.FUNCTION_PARSERS["VECTOR_SEARCH"] = func(p *Parser) *Expr { return bigqueryParseVectorSearch(p) }
	P.FUNCTION_PARSERS["FORECAST"] = func(p *Parser) *Expr { return bigqueryParseForecast(p) }

	// PROPERTY_PARSERS
	P.PROPERTY_PARSERS["NOT DETERMINISTIC"] = bigqueryNoKwargs(func(p *Parser) any {
		return anyExpr(p.expression(New(KStabilityProperty, "this", LiteralString("VOLATILE"))))
	})
	P.PROPERTY_PARSERS["OPTIONS"] = bigqueryNoKwargs(func(p *Parser) any { return p.parseWithProperty() })

	// CONSTRAINT_PARSERS
	P.CONSTRAINT_PARSERS["OPTIONS"] = func(p *Parser) *Expr {
		return New(KProperties, "expressions", p.parseWithProperty())
	}

	// RANGE_PARSERS
	delete(P.RANGE_PARSERS, TK_OVERLAPS)

	// STATEMENT_PARSERS
	P.STATEMENT_PARSERS[TK_ELSE] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }
	P.STATEMENT_PARSERS[TK_END] = func(p *Parser) *Expr { return p.parseAsCommand(p.prev) }
	P.STATEMENT_PARSERS[TK_FOR] = func(p *Parser) *Expr { return bigqueryParseForIn(p) }
	P.STATEMENT_PARSERS[TK_EXPORT] = func(p *Parser) *Expr { return bigqueryParseExportData(p) }
	P.STATEMENT_PARSERS[TK_DECLARE] = func(p *Parser) *Expr { return p.parseDeclare() }

	// Method overrides
	P.h.parseTablePart = bigqueryParseTablePart
	P.h.parseTableParts = bigqueryParseTableParts
	P.h.parseColumn = bigqueryParseColumn
	P.h.parseClusterProperty = bigqueryParseClusterProperty
	P.h.parseJsonObject = bigqueryParseJsonObject
	P.h.parseBracket = bigqueryParseBracket
	P.h.parseUnnest = bigqueryParseUnnest
	P.h.parseColumnOps = bigqueryParseColumnOps
}

// _parse_for_in
func bigqueryParseForIn(p *Parser) *Expr {
	index := p.index
	this := p.parseRange(nil)
	p.matchTextSeq("DO")
	if p.match(TK_COMMAND) {
		p.retreat(index)
		return p.parseAsCommand(p.prev)
	}
	return p.expression(New(KForIn, "this", this, "expression", p.parseStatement()))
}

// _parse_table_part
func bigqueryParseTablePart(p *Parser, schema bool) *Expr {
	this := p.baseParseTablePart(schema)
	if this == nil {
		this = p.parseNumber()
	}

	// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#table_names
	if this.IsA(KIdentifier) {
		tableName := this.Name()
		for p.matchNoAdvance(TK_DASH) && p.next.ok() {
			start := p.curr
			for p.isConnected() && !p.matchSetNoAdvance(p.s.DASHED_TABLE_PART_FOLLOW_TOKENS) {
				p.advance(1)
			}

			if start == p.curr {
				break
			}

			tableName += p.findSQL(start, p.prev)
		}

		this = New(KIdentifier, "this", tableName, "quoted", this.Arg("quoted")).updatePositionsFrom(this)
	} else if this.IsA(KLiteral) {
		tableName := this.Name()

		if p.isConnected() && p.parseVar(true, nil, false) != nil {
			tableName += p.prev.Text
		}

		this = New(KIdentifier, "this", tableName, "quoted", true).updatePositionsFrom(this)
	}

	return this
}

// _parse_table_parts
func bigqueryParseTableParts(p *Parser, schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	table := p.baseParseTableParts(schema, isDbReference, true, fast)

	if !table.IsA(KTable) {
		return table
	}

	// proj-1.db.tbl -- `1.` is tokenized as a float so we need to unravel it here
	if table.CatalogName() == "" {
		if table.DbName() != "" {
			previousDb := table.ArgE("db")
			parts := strings.Split(table.DbName(), ".")
			if len(parts) == 2 && !previousDb.ArgB("quoted") {
				table.Set("catalog", New(KIdentifier, "this", parts[0]).updatePositionsFrom(previousDb))
				table.Set("db", New(KIdentifier, "this", parts[1]).updatePositionsFrom(previousDb))
			}
		} else {
			previousThis := table.This()
			parts := strings.Split(table.Name(), ".")
			if len(parts) == 2 && !previousThis.ArgB("quoted") {
				table.Set("db", New(KIdentifier, "this", parts[0]).updatePositionsFrom(previousThis))
				table.Set("this", New(KIdentifier, "this", parts[1]).updatePositionsFrom(previousThis))
			}
		}
	}

	var alias *Expr
	anyDot := false
	if table.This().IsA(KIdentifier) {
		for _, part := range table.Parts() {
			if strings.Contains(part.Name(), ".") {
				anyDot = true
				break
			}
		}
	}
	if anyDot {
		alias = table.This()
		ids := bigqueryQuotedIdentifiers(bigquerySplitQualifiedName(bigqueryJoinPartNames(table.Parts()), 3))
		catalog, db, thisID, rest := ids[0], ids[1], ids[2], ids[3:]

		for _, part := range []*Expr{catalog, db, thisID} {
			if part != nil {
				part.updatePositionsFrom(table.This())
			}
		}

		this := thisID
		if len(rest) > 0 && this != nil {
			this = DotBuild(append([]*Expr{this}, rest...))
		}

		table = New(KTable, "this", this, "db", db, "catalog", catalog, "pivots", table.Arg("pivots"))
		table.Meta()["quoted_table"] = true
	}

	// The `INFORMATION_SCHEMA` views in BigQuery need to be qualified by a region or
	// dataset, so if the project identifier is omitted we need to fix the ast so that
	// the `INFORMATION_SCHEMA.X` bit is represented as a single (quoted) Identifier.
	// Otherwise, we wouldn't correctly qualify a `Table` node that references these
	// views, because it would seem like the "catalog" part is set, when it'd actually
	// be the region/dataset. Merging the two identifiers into a single one is done to
	// avoid producing a 4-part Table reference, which would cause issues in the schema
	// module, when there are 3-part table names mixed with information schema views.
	//
	// See: https://cloud.google.com/bigquery/docs/information-schema-intro#syntax
	tableParts := table.Parts()
	if len(tableParts) > 1 && pyUpper(tableParts[len(tableParts)-2].Name()) == "INFORMATION_SCHEMA" {
		// We need to alias the table here to avoid breaking existing qualified columns.
		// This is expected to be safe, because if there's an actual alias coming up in
		// the token stream, it will overwrite this one. If there isn't one, we are only
		// exposing the name that can be used to reference the view explicitly (a no-op).
		aliasID := alias
		if aliasID == nil {
			aliasID = tableParts[len(tableParts)-1]
		}
		AliasTableExpr(table, aliasID, nil, nil, false)

		second := tableParts[len(tableParts)-2]
		last := tableParts[len(tableParts)-1]
		infoSchemaView := second.Name() + "." + last.Name()
		newThis := New(KIdentifier, "this", infoSchemaView, "quoted", true)
		m := newThis.Meta()
		m["line"] = second.MetaGet("line")
		m["col"] = last.MetaGet("col")
		m["start"] = second.MetaGet("start")
		m["end"] = last.MetaGet("end")
		table.Set("this", newThis)
		table.Set("db", seqGet(tableParts, -3))
		table.Set("catalog", seqGet(tableParts, -4))
	}

	return table
}

// _parse_column
func bigqueryParseColumn(p *Parser) *Expr {
	column := p.baseParseColumn()
	if column.IsA(KColumn) {
		parts := column.Parts()
		anyDot := false
		for _, part := range parts {
			if strings.Contains(part.Name(), ".") {
				anyDot = true
				break
			}
		}
		if anyDot {
			ids := bigqueryQuotedIdentifiers(bigquerySplitQualifiedName(bigqueryJoinPartNames(parts), 4))
			catalog, db, table, thisID, rest := ids[0], ids[1], ids[2], ids[3], ids[4:]

			this := thisID
			if len(rest) > 0 && this != nil {
				this = DotBuild(append([]*Expr{this}, rest...))
			}

			column = New(KColumn, "this", this, "table", table, "db", db, "catalog", catalog)
			column.Meta()["quoted_column"] = true
		}
	}

	return column
}

// _parse_cluster_property
func bigqueryParseClusterProperty(p *Parser) *Expr {
	return p.expression(New(
		KClusterProperty,
		"expressions", p.parseCSV(p.parseColumn, TK_COMMA),
	))
}

// _parse_json_object
func bigqueryParseJsonObject(p *Parser, agg bool) *Expr {
	jsonObject := p.baseParseJsonObject(false)
	arrayKVPair := seqGet(jsonObject.Expressions(), 0)

	// Converts BQ's "signature 2" of JSON_OBJECT into SQLGlot's canonical representation
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_object_signature2
	if arrayKVPair != nil &&
		arrayKVPair.This().IsA(KArray) &&
		arrayKVPair.Expression().IsA(KArray) {
		keys := arrayKVPair.This().Expressions()
		values := arrayKVPair.Expression().Expressions()

		n := min(len(keys), len(values))
		kvs := make([]*Expr, 0, n)
		for i := 0; i < n; i++ {
			kvs = append(kvs, New(KJSONKeyValue, "this", keys[i], "expression", values[i]))
		}
		jsonObject.Set("expressions", kvs)
	}

	return jsonObject
}

// _parse_bracket
func bigqueryParseBracket(p *Parser, this *Expr) *Expr {
	bracket := p.baseParseBracket(this)

	if bracket.IsA(KArray) {
		bracket.Set("struct_name_inheritance", true)
	}

	if this == bracket {
		return bracket
	}

	if bracket.IsA(KBracket) {
		for _, expression := range bracket.Expressions() {
			name := pyUpper(expression.Name())

			expressions := expression.Expressions()

			bo, ok := bigqueryBracketOffsets[name]
			if !ok || len(expressions) == 0 {
				break
			}

			bracket.Set("offset", bo.offset)
			bracket.Set("safe", bo.safe)
			expression.Replace(expressions[0])
		}
	}

	return bracket
}

// _parse_unnest
func bigqueryParseUnnest(p *Parser, withAlias bool) *Expr {
	unnest := p.baseParseUnnest(withAlias)

	if unnest == nil {
		return nil
	}

	unnestExpr := seqGet(unnest.Expressions(), 0)
	if unnestExpr != nil {
		unnestExpr = annotateTypes(unnestExpr, p.d)

		// Unnesting a nested array (i.e array of structs) explodes the top-level struct fields,
		// in contrast to other dialects such as DuckDB which flattens only the array by default
		if dhIsType(unnestExpr, DT_ARRAY) {
			typ := unnestExpr.RawType()
			if typ == nil {
				typ = unnestExpr.Type()
			}
			for _, arrayElem := range typ.Expressions() {
				if dhIsType(arrayElem, DT_STRUCT) {
					unnest.Set("explode_array", true)
					break
				}
			}
		}
	}

	return unnest
}

// _parse_make_interval
func bigqueryParseMakeInterval(p *Parser) *Expr {
	expr := New(KMakeInterval)

	for _, argKey := range bigqueryMakeIntervalKwargs {
		value := p.parseLambda(false)

		if value == nil {
			break
		}

		// Non-named arguments are filled sequentially, (optionally) followed by named arguments
		// that can appear in any order e.g MAKE_INTERVAL(1, minute => 5, day => 2)
		if value.IsA(KKwarg) {
			argKey = value.This().Name()
		}

		expr.Set(argKey, value)

		p.match(TK_COMMA)
	}

	return expr
}

// _parse_ml. kwargs are extra constructor keyword arguments.
func bigqueryParseML(p *Parser, exprType Kind, kwargs ...any) *Expr {
	p.matchTextSeq("MODEL")
	this := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)
	p.matchTextSeq("TABLE")

	// Certain functions like ML.FORECAST require a STRUCT argument but not a TABLE/SELECT one
	var expression *Expr
	if !p.matchNoAdvance(TK_STRUCT) {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	}

	p.match(TK_COMMA)

	kv := []any{"this", this, "expression", expression, "params_struct", p.parseBitwise()}
	kv = append(kv, kwargs...)
	return p.expression(New(exprType, kv...))
}

// _parse_generate
func bigqueryParseGenerate(p *Parser, exprType Kind, kwargs ...any) *Expr {
	p.matchTextSeq("MODEL")
	this := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)

	var expression *Expr
	if p.matchTextSeq("TABLE") {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	} else if p.matchNoAdvance(TK_L_PAREN) {
		expression = p.parseTable(false, false, nil, false, false, false, false)
	} else {
		expression = p.parseBitwise()
	}

	// self._match(TokenType.COMMA) and self._parse_bitwise() -> False when unmatched
	var paramsStruct any = false
	if p.match(TK_COMMA) {
		paramsStruct = p.parseBitwise()
	}

	kv := []any{"this", this, "expression", expression, "params_struct", paramsStruct}
	kv = append(kv, kwargs...)
	return p.expression(New(exprType, kv...))
}

// bigqueryTokenAt mirrors seq_get(self._tokens, i) (Python negative indexing included).
func bigqueryTokenAt(p *Parser, i int) *Token {
	if i < 0 {
		i += len(p.tokens)
	}
	if i < 0 || i >= len(p.tokens) {
		return nil
	}
	return p.tokens[i]
}

// _parse_translate
func bigqueryParseTranslate(p *Parser) *Expr {
	// Check if this is ML.TRANSLATE by looking at previous tokens
	token := bigqueryTokenAt(p, p.index-4)
	if token != nil && pyUpper(token.Text) == "ML" {
		return bigqueryParseML(p, KMLTranslate)
	}

	return FromArgList(KTranslate, p.parseFunctionArgs(false))
}

// _parse_forecast
func bigqueryParseForecast(p *Parser) *Expr {
	// Check if this is ML.FORECAST by looking at previous tokens.
	token := bigqueryTokenAt(p, p.index-4)
	if token != nil && pyUpper(token.Text) == "ML" {
		return bigqueryParseML(p, KMLForecast)
	}

	// AI.FORECAST is a TVF, where the first argument is either TABLE <table>
	// or a parenthesized query statement, followed by named arguments.
	p.match(TK_TABLE)
	this := p.parseTable(false, false, nil, false, false, false, false)
	if this == nil {
		p.raiseError("Expected table or query statement", nil)
	}

	expr := p.expression(New(KAIForecast, "this", this))
	for p.match(TK_COMMA) {
		a := p.parseLambda(false)
		if a.IsA(KKwarg) {
			expr.Set(a.This().Name(), a)
		} else {
			p.raiseError("Expected key => value syntax for AI.FORECAST, got "+bigqueryPyStrExpr(a), nil)
			break
		}
	}

	return expr
}

// bigqueryPyStrExpr mirrors f"{expr}" for an optional expression (None -> "None").
func bigqueryPyStrExpr(e *Expr) string {
	if e == nil {
		return "None"
	}
	return exprSQL(e)
}

// _parse_features_at_time
func bigqueryParseFeaturesAtTime(p *Parser) *Expr {
	p.match(TK_TABLE)
	this := p.parseTable(false, false, nil, false, false, false, false)

	expr := p.expression(New(KFeaturesAtTime, "this", this))

	for p.match(TK_COMMA) {
		a := p.parseLambda(false)

		// Get the LHS of the Kwarg and set the arg to that value, e.g
		// "num_rows => 1" sets the expr's `num_rows` arg
		if a != nil {
			expr.Set(a.This().Name(), a)
		}
	}

	return expr
}

// _parse_vector_search
func bigqueryParseVectorSearch(p *Parser) *Expr {
	p.match(TK_TABLE)
	baseTable := p.parseTable(false, false, nil, false, false, false, false)

	p.match(TK_COMMA)

	columnToSearch := p.parseBitwise()
	p.match(TK_COMMA)

	p.match(TK_TABLE)
	queryTable := p.parseTable(false, false, nil, false, false, false, false)

	expr := p.expression(New(
		KVectorSearch,
		"this", baseTable,
		"column_to_search", columnToSearch,
		"query_table", queryTable,
	))

	for p.match(TK_COMMA) {
		// query_column_to_search can be named argument or positional
		if p.matchNoAdvance(TK_STRING) {
			queryColumn := p.parseString()
			expr.Set("query_column_to_search", queryColumn)
		} else {
			a := p.parseLambda(false)
			if a != nil {
				expr.Set(a.This().Name(), a)
			}
		}
	}

	return expr
}

// _parse_export_data
func bigqueryParseExportData(p *Parser) *Expr {
	p.matchTextSeq("DATA")

	// self._match_text_seq(...) and self._parse_...() -> False when unmatched
	var connection any = false
	if p.matchTextSeq("WITH", "CONNECTION") {
		connection = p.parseTableParts(false, false, false, false)
	}
	options := p.parseProperties(false)
	var this any = false
	if p.matchTextSeq("AS") {
		this = p.parseSelect(true, false, true, true, true, nil)
	}

	return p.expression(New(
		KExport,
		"connection", connection,
		"options", options,
		"this", this,
	))
}

// _parse_column_ops
func bigqueryParseColumnOps(p *Parser, this *Expr) *Expr {
	funcIndex := p.index + 1
	this = p.baseParseColumnOps(this)

	if this.IsA(KDot) && this.Expression().IsA(KFunc) {
		prefix := pyUpper(this.This().Name())

		var fn Kind
		hasFn := false
		if prefix == "NET" {
			fn, hasFn = KNetFunc, true
		} else if prefix == "SAFE" {
			fn, hasFn = KSafeFunc, true
		}

		if hasFn {
			// Retreat to try and parse a known function instead of an anonymous one,
			// which is parsed by the base column ops parser due to anonymous_func=true
			p.retreat(funcIndex)
			this = New(fn, "this", p.parseFunction(nil, false, true, true))
		} else if prefix == "AI" || prefix == "ML" {
			// AI.* and ML.* function calls can use custom BigQuery signatures that rely on
			// function parsers, so re-parse the function in non-anonymous mode.
			p.retreat(funcIndex)
			parsed := p.parseFunction(nil, false, true, true)
			if parsed != nil {
				this = p.expression(New(KDot, "this", this.This(), "expression", parsed))
			}
		}
	}

	return this
}
