package sqlengine

import "sync"

// Port of sqlglot/dialects/presto.py and sqlglot/parsers/presto.py (sqlglot v30.13.0).
// The generator (sqlglot/generators/presto.py) lives in d_presto_generator.go.
// Data-only class attributes live in the generated zz_*_settings.go files.

func init() { registerCustomizer("presto", customizePresto) }

func customizePresto(d *Dialect) {
	customizePrestoParser(d)
	customizePrestoGenerator(d)
}

// ---------------------------------------------------------------------------------------------
// Module-level builders of sqlglot/parsers/presto.py
// ---------------------------------------------------------------------------------------------

// prestoBuildApproxPercentile mirrors parsers.presto._build_approx_percentile.
func prestoBuildApproxPercentile(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 4 {
		return New(
			KApproxQuantile,
			"this", seqGet(args, 0),
			"weight", seqGet(args, 1),
			"quantile", seqGet(args, 2),
			"accuracy", seqGet(args, 3),
		)
	}
	if len(args) == 3 {
		return New(
			KApproxQuantile,
			"this", seqGet(args, 0), "quantile", seqGet(args, 1), "accuracy", seqGet(args, 2),
		)
	}
	return FromArgList(KApproxQuantile, args)
}

// prestoBuildFromUnixtime mirrors parsers.presto._build_from_unixtime.
func prestoBuildFromUnixtime(args []*Expr, _ *Dialect) *Expr {
	if len(args) == 3 {
		return New(
			KUnixToTime,
			"this", seqGet(args, 0),
			"hours", seqGet(args, 1),
			"minutes", seqGet(args, 2),
		)
	}
	if len(args) == 2 {
		return New(KUnixToTime, "this", seqGet(args, 0), "zone", seqGet(args, 1))
	}

	return FromArgList(KUnixToTime, args)
}

// prestoTeradataTimeMapping mirrors Teradata.TIME_MAPPING (the Teradata dialect is not ported;
// _build_to_char formats with it on purpose).
var prestoTeradataTimeMapping = map[string]string{
	"YY": "%y", "Y4": "%Y", "YYYY": "%Y", "M4": "%B", "M3": "%b", "M": "%-M", "MI": "%M", "MM": "%m",
	"MMM": "%b", "MMMM": "%B", "D": "%-d", "DD": "%d", "D3": "%j", "DDD": "%j", "H": "%-H", "HH": "%H",
	"HH24": "%H", "S": "%-S", "SS": "%S", "SSSSSS": "%f", "E": "%a", "EE": "%a", "E3": "%a", "E4": "%A",
	"EEE": "%a", "EEEE": "%A",
}

var (
	prestoTeradataTimeTrieOnce sync.Once
	prestoTeradataTimeTrie     *trie
)

// prestoBuildToChar mirrors parsers.presto._build_to_char.
func prestoBuildToChar(args []*Expr, _ *Dialect) *Expr {
	fmt := seqGet(args, 1)
	if fmt.IsA(KLiteral) {
		// We uppercase this to match Teradata's format mapping keys
		fmt.Set("this", pyUpper(fmt.ThisS()))
	}

	// We use "teradata" on purpose here, because the time formats are different in Presto.
	// See https://prestodb.io/docs/current/functions/teradata.html?highlight=to_char#to_char
	// (build_formatted_time(exp.TimeToStr, "teradata") -> Teradata.format_time(fmt))
	prestoTeradataTimeTrieOnce.Do(func() {
		prestoTeradataTimeTrie = newTrieFromStrings(mapKeys(prestoTeradataTimeMapping)...)
	})
	var format *Expr
	if fmt != nil && fmt.IsString() {
		format = dhFormatTimeLiteral(formatTime(fmt.ThisS(), prestoTeradataTimeMapping, prestoTeradataTimeTrie))
	} else {
		format = fmt
	}
	return New(KTimeToStr, "this", seqGet(args, 0), "format", format)
}

// ---------------------------------------------------------------------------------------------
// PrestoParser
// ---------------------------------------------------------------------------------------------

func customizePrestoParser(d *Dialect) {
	P := d.P

	F := P.FUNCTIONS
	F["ARBITRARY"] = fromArgList(KAnyValue)
	F["APPROX_DISTINCT"] = fromArgList(KApproxDistinct)
	F["APPROX_PERCENTILE"] = prestoBuildApproxPercentile
	F["BITWISE_AND"] = binaryFromFunction(KBitwiseAnd)
	F["BITWISE_NOT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KBitwiseNot, "this", seqGet(args, 0))
	}
	F["BITWISE_OR"] = binaryFromFunction(KBitwiseOr)
	F["BITWISE_XOR"] = binaryFromFunction(KBitwiseXor)
	F["CARDINALITY"] = fromArgList(KArraySize)
	F["CONTAINS"] = fromArgList(KArrayContains)
	F["DATE_ADD"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KDateAdd, "this", seqGet(args, 2), "expression", seqGet(args, 1), "unit", seqGet(args, 0))
	}
	F["DATE_DIFF"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KDateDiff, "this", seqGet(args, 2), "expression", seqGet(args, 1), "unit", seqGet(args, 0))
	}
	F["DATE_FORMAT"] = buildFormattedTime(KTimeToStr, "", nil)
	F["DATE_PARSE"] = buildFormattedTime(KStrToTime, "", nil)
	F["DATE_TRUNC"] = dateTruncToTime
	F["DAY_OF_WEEK"] = fromArgList(KDayOfWeekIso)
	F["DOW"] = fromArgList(KDayOfWeekIso)
	F["DOY"] = fromArgList(KDayOfYear)
	F["ELEMENT_AT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KBracket,
			"this", seqGet(args, 0), "expressions", []*Expr{seqGet(args, 1)}, "offset", 1, "safe", true,
		)
	}
	F["FROM_HEX"] = fromArgList(KUnhex)
	F["FROM_UNIXTIME"] = prestoBuildFromUnixtime
	F["FROM_UTF8"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KDecode,
			"this", seqGet(args, 0), "replace", seqGet(args, 1), "charset", LiteralString("utf-8"),
		)
	}
	F["JSON_FORMAT"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KJSONFormat, "this", seqGet(args, 0), "options", seqGet(args, 1), "is_json", true)
	}
	F["LEVENSHTEIN_DISTANCE"] = fromArgList(KLevenshtein)
	F["NOW"] = fromArgList(KCurrentTimestamp)
	F["REGEXP_EXTRACT"] = buildRegexpExtract(KRegexpExtract)
	F["REGEXP_EXTRACT_ALL"] = buildRegexpExtract(KRegexpExtractAll)
	F["REGEXP_REPLACE"] = func(args []*Expr, _ *Dialect) *Expr {
		replacement := seqGet(args, 2)
		if replacement == nil {
			replacement = LiteralString("")
		}
		return New(
			KRegexpReplace,
			"this", seqGet(args, 0),
			"expression", seqGet(args, 1),
			"replacement", replacement,
		)
	}
	F["REPLACE"] = buildReplaceWithOptionalReplacement
	F["ROW"] = fromArgList(KStruct)
	F["SEQUENCE"] = fromArgList(KGenerateSeries)
	F["SET_AGG"] = fromArgList(KArrayUniqueAgg)
	F["SPLIT_TO_MAP"] = fromArgList(KStrToMap)
	F["STRPOS"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(
			KStrPosition,
			"this", seqGet(args, 0), "substr", seqGet(args, 1), "occurrence", seqGet(args, 2),
		)
	}
	F["SLICE"] = fromArgList(KArraySlice)
	F["TO_CHAR"] = prestoBuildToChar
	F["TO_UNIXTIME"] = fromArgList(KTimeToUnix)
	F["TO_UTF8"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KEncode, "this", seqGet(args, 0), "charset", LiteralString("utf-8"))
	}
	F["MD5"] = fromArgList(KMD5Digest)
	F["SHA256"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(256))
	}
	F["SHA512"] = func(args []*Expr, _ *Dialect) *Expr {
		return New(KSHA2Digest, "this", seqGet(args, 0), "length", LiteralInt(512))
	}
	F["WEEK"] = fromArgList(KWeekOfYear)

	// FUNCTION_PARSERS = {k: v for k, v in parser.Parser.FUNCTION_PARSERS.items() if k != "TRIM"}
	delete(P.FUNCTION_PARSERS, "TRIM")
}
