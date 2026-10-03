package sqlengine

type kindFlags uint8

const (
	kfVarLenArgs kindFlags = 1 << iota
	kfHashRawArgs
	kfSubquery
	kfCast
	kfDataType
	kfPrimitive
)

type argSpec struct {
	name     string
	required bool
}

type kindInfo struct {
	name     string
	module   string
	key      string
	mro      []Kind
	args     []argSpec
	flags    kindFlags
	sqlNames []string
	isa      KindSet
}

func init() {
	for k := range kindInfos {
		info := &kindInfos[k]
		for _, m := range info.mro {
			info.isa = info.isa.With(m)
		}
	}
}

// Name returns the sqlglot class name, e.g. "Select".
func (k Kind) Name() string { return kindInfos[k].name }

// Key returns the lowercase sqlglot key, e.g. "select".
func (k Kind) Key() string { return kindInfos[k].key }

func (k Kind) String() string { return k.Name() }

// IsA mirrors isinstance(instance_of_k, any of kinds).
func (k Kind) IsA(kinds ...Kind) bool {
	isa := &kindInfos[k].isa
	for _, o := range kinds {
		if isa.Has(o) {
			return true
		}
	}
	return false
}

// kindMatcher returns the set of kinds k for which k.IsA(kinds...) holds, so that a fixed
// isinstance check becomes a single bit test (matcher.Has(k)).
// It reads the static MRO data directly, so it can initialize package-level variables (which run
// before the init that fills kindInfo.isa).
func kindMatcher(kinds ...Kind) KindSet {
	var want KindSet
	for _, o := range kinds {
		want = want.With(o)
	}
	var out KindSet
	for k := range kindInfos {
		for _, m := range kindInfos[k].mro {
			if want.Has(m) {
				out = out.With(Kind(k))
				break
			}
		}
	}
	return out
}

// ArgTypes returns the ordered argument specs.
func (k Kind) ArgTypes() []argSpec { return kindInfos[k].args }

func (k Kind) hasArgType(name string) bool {
	for _, a := range kindInfos[k].args {
		if a.name == name {
			return true
		}
	}
	return false
}

// SQLNames mirrors Func.sql_names().
func (k Kind) SQLNames() []string { return kindInfos[k].sqlNames }

// SQLName mirrors Func.sql_name().
func (k Kind) SQLName() string {
	names := kindInfos[k].sqlNames
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

func (k Kind) isVarLenArgs() bool { return kindInfos[k].flags&kfVarLenArgs != 0 }
func (k Kind) isSubquery() bool   { return kindInfos[k].flags&kfSubquery != 0 }
func (k Kind) isCast() bool       { return kindInfos[k].flags&kfCast != 0 }
func (k Kind) isDataType() bool   { return kindInfos[k].flags&kfDataType != 0 }
func (k Kind) isPrimitive() bool  { return kindInfos[k].flags&kfPrimitive != 0 }
func (k Kind) hashRawArgs() bool  { return kindInfos[k].flags&kfHashRawArgs != 0 }

func kindModule(k Kind) string { return kindInfos[k].module }

// KindByKey looks up a kind by its lowercase key.
func KindByKey(key string) (Kind, bool) {
	k, ok := kindByKey[key]
	return k, ok
}
