package sqlengine

// IsTypeOf reports whether e is one of dtypes, dispatching on the receiver's kind:
//   - DataType (and subkinds): DataTypeIsType, a structural comparison of the node itself;
//   - Cast (and subkinds): the check is applied to the cast's target type ("to");
//   - any other expression: the check is applied to the annotated type (false when the
//     expression has not been annotated).
//
// Each dtype may be a DType, a type string (parsed with the default dialect, user-defined types
// allowed) or a DataType expression.
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

// SetTypeAny sets e's annotated type. nil, a nil expression and "" clear it; a node of exactly
// kind DataType is stored as is; anything else (a DType, a type string, a DataType subkind, ...)
// is converted with DataTypeBuild.
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
			// An empty type string clears the type: the type slot can only hold a DataType.
			e.typ = nil
			return
		}
	}
	e.typ = DataTypeBuild(dtype, nil, false, true)
}

func annDTypeSetArgs(set DTypeSet) []any {
	items := set.Items()
	out := make([]any, len(items))
	for i, d := range items {
		out[i] = d
	}
	return out
}

func annIsTypeSet(e *Expr, set DTypeSet) bool {
	return e.IsTypeOf(annDTypeSetArgs(set)...)
}
