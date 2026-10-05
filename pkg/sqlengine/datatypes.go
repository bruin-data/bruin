package sqlengine

// Port of the type helpers of sqlglot/expressions/datatypes.py and sqlglot/expressions/core.py
// that are not already in expr_builders.go:
//   - DataType.build / DataType.from_str / DataType.is_type live in expr_builders.go
//     (DataTypeBuild, dataTypeFromStr, DataTypeIsType) and are reused here.
//   - Expression.is_type (with the DataType and Cast overrides) -> (*Expr).IsTypeOf
//   - the Expression.type setter                                 -> (*Expr).SetTypeAny
//   - DataType.Type sets (TEXT_TYPES, ...) are generated in zz_dtypes.go (DataType_TEXT_TYPES, ...).

// IsTypeOf mirrors Expr.is_type(*dtypes), dispatching like Python does on the receiver class:
//   - DataType (and subclasses): DataType.is_type, i.e. structural comparison of the node itself;
//   - Cast (and subclasses): Cast.is_type, i.e. self.to.is_type(*dtypes);
//   - any other expression: Expression.is_type, which checks the annotated `_type` slot
//     (False when the expression has not been annotated).
//
// Each dtype may be a DType, a type string (parsed with the default dialect, udt=True) or a
// DataType expression, like Python's DATA_TYPE union.
func (e *Expr) IsTypeOf(dtypes ...any) bool {
	if e == nil {
		return false
	}
	if e.IsA(KDataType) {
		return DataTypeIsType(e, dtypes, false)
	}
	if e.IsA(KCast) {
		return e.ArgE("to").IsTypeOf(dtypes...)
	}
	t := e.typ
	return t != nil && t.IsTypeOf(dtypes...)
}

// SetTypeAny mirrors the Expression.type setter: any truthy value whose class is not exactly
// DataType (a DType, a type string, a DataType subclass, ...) is converted with DataType.build.
func (e *Expr) SetTypeAny(dtype any) {
	switch x := dtype.(type) {
	case nil:
		e.typ = nil
		return
	case *Expr:
		if x == nil {
			e.typ = nil
			return
		}
		if x.Is(KDataType) {
			e.typ = x
			return
		}
	case string:
		if x == "" {
			// Python stores the falsy "" as-is; the Go `_type` slot can only hold a DataType.
			e.typ = nil
			return
		}
	}
	e.typ = DataTypeBuild(dtype, nil, false, true)
}

// annDTypeSetArgs converts a DType set into is_type arguments (mirrors `*exp.DataType.X_TYPES`).
func annDTypeSetArgs(set DTypeSet) []any {
	items := set.Items()
	out := make([]any, len(items))
	for i, d := range items {
		out[i] = d
	}
	return out
}

// annIsTypeSet mirrors `expression.is_type(*SOME_TYPES)` for one of the DataType.*_TYPES sets.
func annIsTypeSet(e *Expr, set DTypeSet) bool {
	return e.IsTypeOf(annDTypeSetArgs(set)...)
}
