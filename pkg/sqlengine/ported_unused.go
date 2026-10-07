package sqlengine

// Faithful ports of public sqlglot helpers that none of the registered dialects reference yet
// (they are used by dialects that are not ported, e.g. Teradata, or by sqlglot's own callers).
// Kept so that adding a dialect stays a mechanical port.
var (
	_ = withStrictTimeInverse
	_ = toNumberWithNlsParam
	_ = buildTimestampFromParts
	_ = transformStructKvToAlias
	_ = transformEliminateJoinMarks
	_ = normalizationDistanceInf
)
