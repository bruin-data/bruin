package sqlengine

func typingPrestoMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_BIGINT,
		KBitwiseAnd,
		KBitwiseNot,
		KBitwiseOr,
		KBitwiseXor,
		KLength,
		KLevenshtein,
		KStrPosition,
		KWidthBucket,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KRound,
		KSign,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this", "expression") }, KMod)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.This() != nil {
			a.annotateByArgs(e, "this")
		} else {
			a.setType(e, DT_DOUBLE)
		}
	}, KRand)
	m.returns(DT_VARBINARY, KMD5Digest)

	return m
}
