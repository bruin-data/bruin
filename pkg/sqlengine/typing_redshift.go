package sqlengine

import (
	"math/big"
	"strconv"
)

func typingRedshiftAnnotateAtTimeZone(a *TypeAnnotator, expression *Expr) {
	this := expression.This()
	switch {
	case this.IsTypeOf(DT_DATE, DT_TIMESTAMP, DT_TIMESTAMPNTZ):
		a.setType(expression, DT_TIMESTAMPTZ)
	case this.IsTypeOf(DT_TIMESTAMPTZ):
		a.setType(expression, DT_TIMESTAMP)
	case this.IsTypeOf(DT_TIMETZ):
		a.setType(expression, DT_TIMETZ)
	default:
		a.setType(expression, DT_UNKNOWN)
	}
}

// typingRedshiftAnnotateSHA2 sizes the VARCHAR to the hex digest.
func typingRedshiftAnnotateSHA2(a *TypeAnnotator, expression *Expr) {
	size := 128
	if length := expression.ArgE("length"); length != nil && length.IsInt() {
		size = 1
		if v, _ := length.toPyNumber(); v != nil {
			if bits, acc := v.Int64(); acc == big.Exact {
				switch bits {
				case 0:
					size = 64
				case 224, 256, 384, 512:
					size = int(bits) / 4
				}
			}
		}
	}
	a.setType(expression, DataTypeBuild("VARCHAR("+strconv.Itoa(size)+")", nil, false, true))
}

func typingRedshiftMetadata() ExprMetadataType {
	m := typingMetadata("postgres").copy()

	m.annotators(typingRedshiftAnnotateAtTimeZone, KAtTimeZone)
	m.annotators(typingRedshiftAnnotateSHA2, KSHA2)
	// Redshift's TO_TIMESTAMP returns TIMESTAMPTZ, not TIMESTAMP
	// https://docs.aws.amazon.com/redshift/latest/dg/r_TO_TIMESTAMP.html
	m.returns(DT_TIMESTAMPTZ, KStrToTime)
	// Redshift's RANK returns INTEGER; DENSE_RANK/NTILE/ROW_NUMBER return BIGINT (base default).
	// https://docs.aws.amazon.com/redshift/latest/dg/r_WF_RANK.html
	m.returns(DT_INT, KRank)
	// Postgres NTILE is INT, but Redshift's is BIGINT — restore the base default.
	// https://docs.aws.amazon.com/redshift/latest/dg/r_WF_NTILE.html
	m.returns(DT_BIGINT, KNtile)
	m.returns(DT_BIGINT, KFromBase)
	m.returns(DT_VARCHAR, KLowerHex)

	return m
}
