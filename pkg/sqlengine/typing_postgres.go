package sqlengine

// Port of sqlglot/typing/postgres.py.

func typingPostgresMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	// https://www.postgresql.org/docs/current/functions-window.html
	// NTILE returns integer; other ranking functions return bigint (base default).
	m.returns(DT_INT, KNtile)

	return m
}
