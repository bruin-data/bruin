package schemaevolution

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/sqlengine"
)

var signedIntRankByDType = map[sqlengine.DType]int{
	sqlengine.DT_TINYINT:  1,
	sqlengine.DT_SMALLINT: 2,
	sqlengine.DT_INT:      3,
	sqlengine.DT_BIGINT:   4,
	sqlengine.DT_INT128:   5,
}

var signedIntDTypeByRank = map[int]sqlengine.DType{
	1: sqlengine.DT_TINYINT,
	2: sqlengine.DT_SMALLINT,
	3: sqlengine.DT_INT,
	4: sqlengine.DT_BIGINT,
	5: sqlengine.DT_INT128,
}

var unsignedIntRankByDType = map[sqlengine.DType]int{
	sqlengine.DT_UTINYINT:  1,
	sqlengine.DT_USMALLINT: 2,
	sqlengine.DT_UINT:      3,
	sqlengine.DT_UBIGINT:   4,
	sqlengine.DT_UINT128:   5,
}

var intMaxDecimalDigitsByDType = map[sqlengine.DType]int{
	sqlengine.DT_TINYINT:   3,
	sqlengine.DT_UTINYINT:  3,
	sqlengine.DT_SMALLINT:  5,
	sqlengine.DT_USMALLINT: 5,
	sqlengine.DT_INT:       10,
	sqlengine.DT_UINT:      10,
	sqlengine.DT_BIGINT:    19,
	sqlengine.DT_UBIGINT:   20,
}

var signedIntGuaranteedDecimalDigitsByDType = map[sqlengine.DType]int{
	sqlengine.DT_TINYINT:  2,
	sqlengine.DT_SMALLINT: 4,
	sqlengine.DT_INT:      9,
	sqlengine.DT_BIGINT:   18,
	sqlengine.DT_INT128:   38,
}

var timestampRankByDType = map[sqlengine.DType]int{
	sqlengine.DT_DATE:         1,
	sqlengine.DT_TIMESTAMP_S:  2,
	sqlengine.DT_TIMESTAMP_MS: 3,
	sqlengine.DT_TIMESTAMP:    4,
}

// TypePolicy defines the column validation and backwards-compatible type widening
// rules of a target SQL engine.
type TypePolicy struct {
	Name                           string
	Format                         TypeFormatConfig
	NotNullAddedColumnReason       string
	RequireLiteralDefault          bool
	MaxSignedIntDType              sqlengine.DType
	MaxUnsignedIntDType            sqlengine.DType
	AllowUnsignedToSigned          bool
	AllowDecimalWidening           bool
	AllowIntToDecimal              bool
	MaxDecimalPrecision            int
	MaxDecimalIntDigits            int
	MaxDecimalScale                int
	AllowDecimalToBigDecimal       bool
	AllowLengthWidening            bool
	UnboundedVarcharAbsorbsAll     bool
	ImplicitVarcharTargetPredicate func(cur ParsedType) bool
	AllowDateToTimestamp           bool
	AllowTimestampToTimestampTZ    bool
}

// PolicyDialect implements Dialect using a declarative TypePolicy.
type PolicyDialect struct {
	Policy TypePolicy
}

func (d PolicyDialect) destName() string {
	if d.Policy.Name != "" {
		return d.Policy.Name
	}
	return "destination"
}

// ParseType parses sqlType using the policy's TypeFormatConfig.
func (d PolicyDialect) ParseType(sqlType string) ParsedType {
	return ParseType(d.Policy.Format, sqlType)
}

// FormatType formats pt using the policy's TypeFormatConfig.
func (d PolicyDialect) FormatType(pt ParsedType) string {
	return FormatType(d.Policy.Format, pt)
}

// ColumnKey returns the case-insensitive canonical column lookup key.
func (d PolicyDialect) ColumnKey(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// NormalizeType returns the canonical comparison/display string for sqlType.
func (d PolicyDialect) NormalizeType(sqlType string) string {
	return d.FormatType(d.ParseType(sqlType))
}

// ValidateNewColumn checks whether col can be added via ALTER TABLE under d.Policy.
func (d PolicyDialect) ValidateNewColumn(col Column) (Column, string, bool) {
	strExample := d.FormatType(ParsedType{DType: sqlengine.DT_VARCHAR})
	if col.UntypedNull {
		return Column{}, fmt.Sprintf("cannot infer concrete type for new column from untyped NULL; add an explicit CAST (e.g. CAST(NULL AS %s))", strExample), false
	}
	rawType := strings.TrimSpace(col.Type)
	if rawType == "" {
		return Column{}, "column must declare a non-empty SQL type", false
	}
	pt := d.ParseType(rawType)
	if pt.IsNull() {
		return Column{}, fmt.Sprintf("cannot infer concrete type for new column from NULL; add an explicit CAST (e.g. CAST(NULL AS %s))", strExample), false
	}
	if pt.HasNestedNull() {
		return Column{}, "cannot infer concrete type for new column from nested untyped NULL or empty collection; add an explicit CAST", false
	}
	if d.Policy.RequireLiteralDefault && col.Default != "" && !IsConstantLiteralExpr(d.Policy.Format.SQLDialect, col.Default) {
		return Column{}, d.destName() + " only supports literal DEFAULT values on ADD COLUMN", false
	}
	if col.PrimaryKey {
		return Column{}, "cannot add a new PRIMARY KEY column via ALTER TABLE in " + d.destName(), false
	}
	if col.HasForeignKey {
		return Column{}, "cannot add a new FOREIGN KEY column via ALTER TABLE in " + d.destName(), false
	}
	if col.NotNull && d.Policy.NotNullAddedColumnReason != "" {
		return Column{}, d.Policy.NotNullAddedColumnReason, false
	}

	out := col
	out.Type = d.FormatType(pt)
	return out, "", true
}

// ResolveTypeChange determines whether existing can keep its current type, widen safely
// in place, or must be rejected as incompatible under d.Policy.
func (d PolicyDialect) ResolveTypeChange(existing, incoming Column) (TypeResolution, string, string) {
	if incoming.UntypedNull {
		return TypeChangeNone, "", ""
	}

	cur := d.ParseType(existing.Type)
	inc := d.ParseType(incoming.Type)
	if incoming.UnreportedTypeParams {
		inc = d.inheritTypeParams(cur, inc)
	}

	res, target, reason := d.resolveParsedTypeChange(cur, inc)
	if res == TypeChangeWiden {
		if existing.PrimaryKey || existing.Unique {
			return TypeChangeIncompatible, "", "cannot change type of a PRIMARY KEY or UNIQUE column in " + d.destName()
		}
		return TypeChangeWiden, d.FormatType(target), ""
	}
	return res, "", reason
}

// inheritTypeParams gives each unparameterized STRING, BYTES, NUMERIC, or BIGNUMERIC in inc the
// parameters of the same type at the same position in cur, descending into ARRAY elements and
// STRUCT fields whose names line up.
func (d PolicyDialect) inheritTypeParams(cur, inc ParsedType) ParsedType {
	if cur.DType != inc.DType {
		return inc
	}
	switch inc.DType { //nolint:exhaustive
	case sqlengine.DT_VARCHAR, sqlengine.DT_BINARY, sqlengine.DT_DECIMAL, sqlengine.DT_BIGDECIMAL:
		if d.FormatType(inc) == d.FormatType(ParsedType{DType: inc.DType}) {
			inc.Length, inc.Precision, inc.Scale = cur.Length, cur.Precision, cur.Scale
		}
	case sqlengine.DT_ARRAY:
		if cur.Elem != nil && inc.Elem != nil {
			elem := d.inheritTypeParams(*cur.Elem, *inc.Elem)
			inc.Elem = &elem
		}
	case sqlengine.DT_STRUCT:
		if len(cur.Fields) != len(inc.Fields) {
			return inc
		}
		fields := make([]StructField, len(inc.Fields))
		for i, f := range inc.Fields {
			if d.ColumnKey(f.Name) == d.ColumnKey(cur.Fields[i].Name) {
				f.Type = d.inheritTypeParams(cur.Fields[i].Type, f.Type)
			}
			fields[i] = f
		}
		inc.Fields = fields
	}
	return inc
}

func (d PolicyDialect) resolveParsedTypeChange(cur, inc ParsedType) (TypeResolution, ParsedType, string) {
	curFmt := d.FormatType(cur)
	incFmt := d.FormatType(inc)

	if curFmt == incFmt || incFmt == "" || inc.IsNull() {
		return TypeChangeNone, ParsedType{}, ""
	}
	if curFmt == "" {
		return TypeChangeIncompatible, ParsedType{}, ""
	}

	if cur.DType == sqlengine.DT_ARRAY && inc.DType == sqlengine.DT_ARRAY && cur.Elem != nil && inc.Elem != nil {
		if cur.ArraySize > 0 && inc.ArraySize != cur.ArraySize {
			return TypeChangeIncompatible, ParsedType{}, fmt.Sprintf("cannot implicitly fit %s into fixed-size array %s", incFmt, curFmt)
		}
		res, targetElem, reason := d.resolveParsedTypeChange(*cur.Elem, *inc.Elem)
		switch res {
		case TypeChangeNone:
			return TypeChangeNone, ParsedType{}, ""
		case TypeChangeWiden:
			return TypeChangeWiden, ParsedType{
				DType:     sqlengine.DT_ARRAY,
				Elem:      &targetElem,
				ArraySize: cur.ArraySize,
			}, ""
		default:
			return TypeChangeIncompatible, ParsedType{}, reason
		}
	}

	if ((cur.DType == sqlengine.DT_STRUCT && inc.DType == sqlengine.DT_STRUCT) ||
		(cur.DType == sqlengine.DT_UNION && inc.DType == sqlengine.DT_UNION) ||
		(cur.DType == sqlengine.DT_MAP && inc.DType == sqlengine.DT_MAP)) && d.compositeAbsorbs(cur, inc) {
		return TypeChangeNone, ParsedType{}, ""
	}

	if res, target, reason, handled := d.resolveStringAndBinary(cur, inc, curFmt, incFmt); handled {
		return res, target, reason
	}

	if res, target, reason, handled := d.resolveNumeric(cur, inc, curFmt, incFmt); handled {
		return res, target, reason
	}

	if res, target, reason, handled := d.resolveTemporal(cur, inc, curFmt, incFmt); handled {
		return res, target, reason
	}

	return TypeChangeIncompatible, ParsedType{}, ""
}

func (d PolicyDialect) compositeAbsorbs(cur, inc ParsedType) bool {
	if inc.IsNull() {
		return true
	}
	switch {
	case cur.DType == sqlengine.DT_ARRAY && inc.DType == sqlengine.DT_ARRAY:
		if cur.Elem == nil || inc.Elem == nil || (cur.ArraySize > 0 && inc.ArraySize != cur.ArraySize) {
			return false
		}
		return d.compositeAbsorbs(*cur.Elem, *inc.Elem)

	case cur.DType == sqlengine.DT_MAP && inc.DType == sqlengine.DT_MAP:
		if cur.MapKey == nil || cur.MapValue == nil || inc.MapKey == nil || inc.MapValue == nil {
			return false
		}
		return d.compositeAbsorbs(*cur.MapKey, *inc.MapKey) &&
			d.compositeAbsorbs(*cur.MapValue, *inc.MapValue)

	case cur.DType == sqlengine.DT_STRUCT && inc.DType == sqlengine.DT_STRUCT:
		if len(cur.Fields) == 0 || len(cur.Fields) != len(inc.Fields) {
			return false
		}
		for i := range cur.Fields {
			if d.ColumnKey(cur.Fields[i].Name) != d.ColumnKey(inc.Fields[i].Name) {
				return false
			}
			if !d.compositeAbsorbs(cur.Fields[i].Type, inc.Fields[i].Type) {
				return false
			}
		}
		return true

	case cur.DType == sqlengine.DT_UNION && inc.DType == sqlengine.DT_UNION:
		if len(cur.Fields) == 0 || len(inc.Fields) == 0 {
			return false
		}
		curByName := make(map[string]ParsedType, len(cur.Fields))
		for _, f := range cur.Fields {
			curByName[d.ColumnKey(f.Name)] = f.Type
		}
		for _, f := range inc.Fields {
			curMember, ok := curByName[d.ColumnKey(f.Name)]
			if !ok || !d.compositeAbsorbs(curMember, f.Type) {
				return false
			}
		}
		return true

	default:
		res, _, _ := d.resolveParsedTypeChange(cur, inc)
		return res == TypeChangeNone
	}
}

func (d PolicyDialect) resolveStringAndBinary(cur, inc ParsedType, curFmt, incFmt string) (TypeResolution, ParsedType, string, bool) {
	if (cur.DType == sqlengine.DT_VARCHAR && inc.DType == sqlengine.DT_VARCHAR) ||
		(cur.DType == sqlengine.DT_BINARY && inc.DType == sqlengine.DT_BINARY) {
		if cur.Length == 0 || (inc.Length > 0 && inc.Length <= cur.Length) {
			return TypeChangeNone, ParsedType{}, "", true
		}
		if !d.Policy.AllowLengthWidening {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: cur.DType, Length: inc.Length}, "", true
	}

	if cur.DType == sqlengine.DT_VARCHAR && cur.Length == 0 && d.Policy.UnboundedVarcharAbsorbsAll {
		return TypeChangeNone, ParsedType{}, "", true
	}

	if inc.DType == sqlengine.DT_VARCHAR && d.Policy.ImplicitVarcharTargetPredicate != nil && d.Policy.ImplicitVarcharTargetPredicate(cur) {
		return TypeChangeNone, ParsedType{}, "", true
	}

	return TypeChangeIncompatible, ParsedType{}, "", false
}

func (d PolicyDialect) resolveNumeric(cur, inc ParsedType, curFmt, incFmt string) (TypeResolution, ParsedType, string, bool) {
	curSigned, curIsSigned := signedIntRankByDType[cur.DType]
	incSigned, incIsSigned := signedIntRankByDType[inc.DType]
	curUnsigned, curIsUnsigned := unsignedIntRankByDType[cur.DType]
	incUnsigned, incIsUnsigned := unsignedIntRankByDType[inc.DType]
	curIsDec := cur.DType == sqlengine.DT_DECIMAL || cur.DType == sqlengine.DT_BIGDECIMAL
	incIsDec := inc.DType == sqlengine.DT_DECIMAL || inc.DType == sqlengine.DT_BIGDECIMAL
	curIsFloat := cur.DType == sqlengine.DT_FLOAT || cur.DType == sqlengine.DT_DOUBLE
	incIsFloat := inc.DType == sqlengine.DT_FLOAT || inc.DType == sqlengine.DT_DOUBLE

	curIsNum := curIsSigned || curIsUnsigned || curIsDec || curIsFloat
	incIsNum := incIsSigned || incIsUnsigned || incIsDec || incIsFloat
	if !curIsNum || !incIsNum {
		return TypeChangeIncompatible, ParsedType{}, "", false
	}

	if curIsSigned && incIsSigned {
		if incSigned <= curSigned {
			return TypeChangeNone, ParsedType{}, "", true
		}
		if incSigned > d.maxSignedIntRank() {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: inc.DType}, "", true
	}

	if curIsUnsigned && incIsUnsigned {
		if incUnsigned <= curUnsigned {
			return TypeChangeNone, ParsedType{}, "", true
		}
		if incUnsigned > d.maxUnsignedIntRank() {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: inc.DType}, "", true
	}

	if curIsUnsigned && incIsSigned {
		targetRank := max(incSigned, curUnsigned+1)
		nextDT, ok := signedIntDTypeByRank[targetRank]
		if !ok || curUnsigned >= signedIntRankByDType[sqlengine.DT_INT128] {
			return TypeChangeIncompatible, ParsedType{}, "", true
		}
		if !d.Policy.AllowUnsignedToSigned || targetRank > d.maxSignedIntRank() {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: nextDT}, "", true
	}

	if curIsSigned && incIsUnsigned {
		if curSigned > incUnsigned {
			return TypeChangeNone, ParsedType{}, "", true
		}
		targetRank := incUnsigned + 1
		nextDT, ok := signedIntDTypeByRank[targetRank]
		if !ok || incUnsigned >= signedIntRankByDType[sqlengine.DT_INT128] {
			return TypeChangeIncompatible, ParsedType{}, "", true
		}
		if !d.Policy.AllowUnsignedToSigned || targetRank > d.maxSignedIntRank() {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: nextDT}, "", true
	}

	if curIsFloat && incIsFloat {
		if cur.DType == sqlengine.DT_DOUBLE || inc.DType == sqlengine.DT_FLOAT {
			return TypeChangeNone, ParsedType{}, "", true
		}
		return TypeChangeWiden, ParsedType{DType: sqlengine.DT_DOUBLE}, "", true
	}

	if (curIsSigned || curIsUnsigned) && incIsFloat {
		return TypeChangeIncompatible, ParsedType{}, fmt.Sprintf("widening exact integer %s to floating-point %s can lose precision and is not supported by %s", curFmt, incFmt, d.destName()), true
	}

	if curIsFloat && (incIsSigned || incIsUnsigned) {
		if cur.DType == sqlengine.DT_DOUBLE ||
			inc.DType == sqlengine.DT_TINYINT || inc.DType == sqlengine.DT_UTINYINT ||
			inc.DType == sqlengine.DT_SMALLINT || inc.DType == sqlengine.DT_USMALLINT {
			return TypeChangeNone, ParsedType{}, "", true
		}
		return TypeChangeWiden, ParsedType{DType: sqlengine.DT_DOUBLE}, "", true
	}

	if curIsDec && incIsDec {
		return d.resolveDecimalPair(cur, inc, curFmt, incFmt)
	}

	if (curIsSigned || curIsUnsigned) && incIsDec {
		incP, incS := d.effectiveDecimalPS(inc)
		if curIsSigned && incS == 0 {
			if safeDigits, ok := signedIntGuaranteedDecimalDigitsByDType[cur.DType]; ok && incP <= safeDigits {
				return TypeChangeNone, ParsedType{}, "", true
			}
		}
		intDigits, ok := intMaxDecimalDigitsByDType[cur.DType]
		if !ok {
			return TypeChangeIncompatible, ParsedType{}, "", true
		}
		if !d.Policy.AllowIntToDecimal {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		target, ok, reason := d.selectDecimalTarget(inc.DType, max(intDigits, incP-incS), incS)
		if !ok {
			return TypeChangeIncompatible, ParsedType{}, reason, true
		}
		return TypeChangeWiden, target, "", true
	}

	if curIsDec && (incIsSigned || incIsUnsigned) {
		intDigits, ok := intMaxDecimalDigitsByDType[inc.DType]
		if !ok {
			return TypeChangeIncompatible, ParsedType{}, "", true
		}
		curP, curS := d.effectiveDecimalPS(cur)
		if curP-curS >= intDigits {
			return TypeChangeNone, ParsedType{}, "", true
		}
		if !d.Policy.AllowDecimalWidening || !d.Policy.AllowIntToDecimal {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		target, ok, reason := d.selectDecimalTarget(cur.DType, intDigits, curS)
		if !ok {
			return TypeChangeIncompatible, ParsedType{}, reason, true
		}
		return TypeChangeWiden, target, "", true
	}

	if curIsDec && incIsFloat {
		return TypeChangeIncompatible, ParsedType{}, fmt.Sprintf("widening exact %s to floating-point %s can lose decimal precision", curFmt, incFmt), true
	}

	if curIsFloat && incIsDec {
		if cur.DType == sqlengine.DT_DOUBLE {
			return TypeChangeNone, ParsedType{}, "", true
		}
		return TypeChangeWiden, ParsedType{DType: sqlengine.DT_DOUBLE}, "", true
	}

	return TypeChangeIncompatible, ParsedType{}, "", false
}

func (d PolicyDialect) resolveDecimalPair(cur, inc ParsedType, curFmt, incFmt string) (TypeResolution, ParsedType, string, bool) {
	curP, curS := d.effectiveDecimalPS(cur)
	incP, incS := d.effectiveDecimalPS(inc)
	curInt := curP - curS
	incInt := incP - incS

	isUpgradeToBigDec := cur.DType == sqlengine.DT_DECIMAL && inc.DType == sqlengine.DT_BIGDECIMAL
	if !isUpgradeToBigDec && curInt >= incInt && curS >= incS {
		return TypeChangeNone, ParsedType{}, "", true
	}
	if !d.Policy.AllowDecimalWidening && (!isUpgradeToBigDec || !d.Policy.AllowDecimalToBigDecimal) {
		return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
	}

	preferredDT := cur.DType
	if inc.DType == sqlengine.DT_BIGDECIMAL {
		preferredDT = sqlengine.DT_BIGDECIMAL
	}
	target, ok, reason := d.selectDecimalTarget(preferredDT, max(curInt, incInt), max(curS, incS))
	if !ok {
		return TypeChangeIncompatible, ParsedType{}, reason, true
	}
	return TypeChangeWiden, target, "", true
}

func (d PolicyDialect) effectiveDecimalPS(pt ParsedType) (int, int) {
	if pt.Precision > 0 {
		return pt.Precision, pt.Scale
	}
	if pt.DType == sqlengine.DT_BIGDECIMAL {
		return defaultBigDecimalPrecision, defaultBigDecimalScale
	}
	if d.Policy.Format.DefaultDecimalPrecision > 0 {
		return d.Policy.Format.DefaultDecimalPrecision, d.Policy.Format.DefaultDecimalScale
	}
	return 38, 0
}

func (d PolicyDialect) selectDecimalTarget(preferred sqlengine.DType, intDigits, scale int) (ParsedType, bool, string) {
	maxP := d.Policy.MaxDecimalPrecision
	if maxP <= 0 {
		maxP = 38
	}
	maxInt := d.Policy.MaxDecimalIntDigits
	if maxInt <= 0 {
		maxInt = maxP
	}
	maxScale := d.Policy.MaxDecimalScale
	if maxScale <= 0 {
		maxScale = maxP
	}

	prec := intDigits + scale
	if preferred == sqlengine.DT_DECIMAL && intDigits <= maxInt && scale <= maxScale && prec <= maxP {
		return ParsedType{DType: sqlengine.DT_DECIMAL, Precision: prec, Scale: scale}, true, ""
	}
	if (preferred == sqlengine.DT_BIGDECIMAL || d.Policy.AllowDecimalToBigDecimal) &&
		intDigits <= defaultBigDecimalScale && scale <= defaultBigDecimalScale && prec <= defaultBigDecimalPrecision {
		return ParsedType{DType: sqlengine.DT_BIGDECIMAL, Precision: prec, Scale: scale}, true, ""
	}
	return ParsedType{}, false, fmt.Sprintf("widened decimal (%d,%d) exceeds %s limits", prec, scale, d.destName())
}

func (d PolicyDialect) resolveTemporal(cur, inc ParsedType, curFmt, incFmt string) (TypeResolution, ParsedType, string, bool) {
	if (cur.DType == sqlengine.DT_TIME || cur.DType == sqlengine.DT_TIMETZ) &&
		(inc.DType == sqlengine.DT_TIME || inc.DType == sqlengine.DT_TIMETZ) {
		if cur.DType == sqlengine.DT_TIMETZ && inc.DType == sqlengine.DT_TIME {
			return TypeChangeNone, ParsedType{}, "", true
		}
		return TypeChangeIncompatible, ParsedType{}, "cannot widen TIME to TIME WITH TIME ZONE without session-timezone ambiguity", true
	}

	curRank, curIsTS := timestampRankByDType[cur.DType]
	incRank, incIsTS := timestampRankByDType[inc.DType]
	curIsTSTZ := cur.DType == sqlengine.DT_TIMESTAMPTZ
	incIsTSTZ := inc.DType == sqlengine.DT_TIMESTAMPTZ
	curIsNS := cur.DType == sqlengine.DT_TIMESTAMP_NS
	incIsNS := inc.DType == sqlengine.DT_TIMESTAMP_NS

	if (!curIsTS && !curIsTSTZ && !curIsNS) || (!incIsTS && !incIsTSTZ && !incIsNS) {
		return TypeChangeIncompatible, ParsedType{}, "", false
	}

	if incIsNS {
		return TypeChangeIncompatible, ParsedType{}, "widening " + curFmt + " to TIMESTAMP_NS can overflow timestamps outside 1677..2262", true
	}
	if curIsNS {
		if incIsTSTZ {
			return TypeChangeIncompatible, ParsedType{}, "widening TIMESTAMP_NS to TIMESTAMP WITH TIME ZONE would lose nanosecond precision", true
		}
		return TypeChangeNone, ParsedType{}, "", true
	}

	if curIsTSTZ {
		if !incIsTSTZ && (!d.Policy.AllowTimestampToTimestampTZ || (inc.DType == sqlengine.DT_DATE && !d.Policy.AllowDateToTimestamp)) {
			return TypeChangeIncompatible, ParsedType{}, "", true
		}
		return TypeChangeNone, ParsedType{}, "", true
	}

	if incIsTSTZ {
		if !d.Policy.AllowTimestampToTimestampTZ || (cur.DType == sqlengine.DT_DATE && !d.Policy.AllowDateToTimestamp) {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: sqlengine.DT_TIMESTAMPTZ}, "", true
	}

	if incRank > curRank {
		if !d.Policy.AllowDateToTimestamp {
			return TypeChangeIncompatible, ParsedType{}, d.unsupportedWideningReason(curFmt, incFmt), true
		}
		return TypeChangeWiden, ParsedType{DType: inc.DType}, "", true
	}
	if inc.DType == sqlengine.DT_DATE && cur.DType != sqlengine.DT_DATE && !d.Policy.AllowDateToTimestamp {
		return TypeChangeIncompatible, ParsedType{}, "", true
	}
	return TypeChangeNone, ParsedType{}, "", true
}

func (d PolicyDialect) maxSignedIntRank() int {
	if r, ok := signedIntRankByDType[d.Policy.MaxSignedIntDType]; ok {
		return r
	}
	return 0
}

func (d PolicyDialect) maxUnsignedIntRank() int {
	if r, ok := unsignedIntRankByDType[d.Policy.MaxUnsignedIntDType]; ok {
		return r
	}
	return 0
}

func (d PolicyDialect) unsupportedWideningReason(curFmt, incFmt string) string {
	return fmt.Sprintf("widening %s to %s is not supported by %s", curFmt, incFmt, d.destName())
}
