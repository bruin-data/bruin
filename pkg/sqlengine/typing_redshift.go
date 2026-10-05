package sqlengine

// Port of sqlglot/typing/redshift.py.

func typingRedshiftMetadata() ExprMetadataType {
	m := typingMetadata("postgres").copy()

	// Redshift's TO_TIMESTAMP returns TIMESTAMPTZ, not TIMESTAMP
	// https://docs.aws.amazon.com/redshift/latest/dg/r_TO_TIMESTAMP.html
	m.returns(DT_TIMESTAMPTZ, KStrToTime)
	// Redshift's RANK returns INTEGER; DENSE_RANK/NTILE/ROW_NUMBER return BIGINT (base default).
	// https://docs.aws.amazon.com/redshift/latest/dg/r_WF_RANK.html
	m.returns(DT_INT, KRank)
	// Postgres NTILE is INT, but Redshift's is BIGINT — restore the base default.
	// https://docs.aws.amazon.com/redshift/latest/dg/r_WF_NTILE.html
	m.returns(DT_BIGINT, KNtile)

	return m
}
