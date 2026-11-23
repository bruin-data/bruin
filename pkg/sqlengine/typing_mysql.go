package sqlengine

func typingMySQLAnnotateReverse(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.This().IsTypeOf(DT_BINARY, DT_VARBINARY, DT_UNKNOWN) {
		a.annotateByArgs(expression, "this")
	} else {
		a.setType(expression, DT_VARCHAR)
	}

	return expression
}

func typingMySQLAnnotateTruncate(a *TypeAnnotator, expression *Expr) *Expr {
	if annIsTypeSet(expression.This(), DataType_TEXT_TYPES) {
		return a.setType(expression, DT_DOUBLE)
	}

	return a.annotateByArgs(expression, "this")
}

var typingMySQLBinaryTypes = newDTypeSet(DT_BINARY, DT_VARBINARY, DT_TINYBLOB, DT_BLOB, DT_MEDIUMBLOB, DT_LONGBLOB)

var typingMySQLCompressLongblobTypes = newDTypeSet(
	DT_TEXT, DT_MEDIUMTEXT, DT_LONGTEXT, DT_BLOB, DT_MEDIUMBLOB, DT_LONGBLOB, DT_JSON,
)

var typingMySQLCompressVarbinaryTypes = newDTypeSet(
	DT_CHAR, DT_VARCHAR, DT_BINARY, DT_VARBINARY, DT_TINYBLOB, DT_ENUM, DT_INT, DT_BIGINT, DT_DECIMAL,
	DT_DOUBLE, DT_DATE, DT_DATETIME,
)

// typingMySQLAnnotateRegexpReplace types REGEXP_REPLACE as LONGBLOB when the subject, pattern or
// replacement is a binary string and LONGTEXT otherwise (UNKNOWN when one of them is untyped).
func typingMySQLAnnotateRegexpReplace(a *TypeAnnotator, expression *Expr) *Expr {
	hasBinary := false
	for _, arg := range []*Expr{expression.This(), expression.Expression(), expression.ArgE("replacement")} {
		if arg == nil {
			continue
		}
		if arg.IsTypeOf(DT_UNKNOWN) {
			return a.setType(expression, DT_UNKNOWN)
		}
		hasBinary = hasBinary || annIsTypeSet(arg, typingMySQLBinaryTypes)
	}
	if hasBinary {
		return a.setType(expression, DT_LONGBLOB)
	}
	return a.setType(expression, DT_LONGTEXT)
}

func typingMySQLAnnotateCompress(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()
	switch {
	case annIsTypeSet(this, typingMySQLCompressVarbinaryTypes):
		return a.setType(expression, DT_VARBINARY)
	case annIsTypeSet(this, typingMySQLCompressLongblobTypes):
		return a.setType(expression, DT_LONGBLOB)
	case this.IsTypeOf(DT_TINYTEXT):
		return a.setType(expression, DT_BLOB)
	}
	return a.setType(expression, DT_UNKNOWN)
}

// typingMySQLAnnotateBitFunc types BIT_AND, BIT_OR and BIT_XOR: VARBINARY for a binary string
// argument, UBIGINT otherwise (UNKNOWN when the argument is untyped).
func typingMySQLAnnotateBitFunc(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()
	switch {
	case this.IsTypeOf(DT_UNKNOWN):
		return a.setType(expression, DT_UNKNOWN)
	case annIsTypeSet(this, typingMySQLBinaryTypes):
		return a.setType(expression, DT_VARBINARY)
	}
	return a.setType(expression, DT_UBIGINT)
}

func typingMySQLMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_DOUBLE,
		KAtan2,
		KMatchAgainst,
		KStDistance,
	)
	m.returns(
		DT_DATETIME,
		KCurrentTimestamp,
		KConvertTimezone,
		KLocaltime,
		KLocaltimestamp,
		KUtcTimestamp,
	)
	m.returns(DT_DATE, KUtcDate)
	m.returns(
		DT_VARCHAR,
		KElt,
		KHex,
		KNumberToStr, // format()
		KReplace,
		KStuff, // insert function
		KSubstringIndex,
		KRegexpSubstr,
		KCollation,
		KJSONType,
		KUuid,
	)
	m.returns(
		DT_INT,
		KMonth,
		KSecond,
		KWeek,
		KMinute,
	)
	m.returns(
		DT_BIGINT,
		KRegexpInstr,
		KGrouping,
	)
	m.returns(
		DT_TIME,
		KTimeFromParts,
		KUtcTime,
	)
	m.returns(DT_VARBINARY, KUnhex)
	m.returns(
		DT_JSON,
		KJSONObjectAgg,
		KJSONObject,
		KJSONExtract,
		KJSONKeys,
		KJSONArrayAppend,
		KJSONArrayInsert,
		KJSONRemove,
		KJSONSet,
	)
	m.returns(DT_LONGTEXT, KCurrentRole)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KPad,
		KLeft,
		KRight,
		KLead,
		KLag,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateBitFunc(a, e) },
		KBitwiseAndAgg,
		KBitwiseXorAgg,
		KBitwiseOrAgg,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateReverse(a, e) }, KReverse)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateTruncate(a, e) }, KTrunc)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateRegexpReplace(a, e) }, KRegexpReplace)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateCompress(a, e) }, KCompress)

	return m
}
