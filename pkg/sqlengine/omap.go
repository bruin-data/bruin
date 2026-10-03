package sqlengine

// omap is an insertion-ordered map mirroring Python dict semantics: updating an existing
// key keeps its position, deleting removes it, and re-adding appends at the end.
//
// Entries live in parallel slices; small maps (the common case: scope sources, schema columns)
// are searched linearly and an index is only built past omapIndexThreshold entries.
type omap[V any] struct {
	keys []string
	vals []V
	idx  map[string]int
}

const omapIndexThreshold = 8

func newOMap[V any]() *omap[V] { return &omap[V]{} }

func (o *omap[V]) find(key string) int {
	if o.idx != nil {
		if i, ok := o.idx[key]; ok {
			return i
		}
		return -1
	}
	for i, k := range o.keys {
		if k == key {
			return i
		}
	}
	return -1
}

func (o *omap[V]) reindex() {
	if len(o.keys) <= omapIndexThreshold {
		o.idx = nil
		return
	}
	o.idx = make(map[string]int, cap(o.keys))
	for i, k := range o.keys {
		o.idx[k] = i
	}
}

// reserve makes room for n more entries without changing contents.
func (o *omap[V]) reserve(n int) {
	if need := len(o.keys) + n; need > cap(o.keys) {
		o.keys = append(make([]string, 0, need), o.keys...)
		o.vals = append(make([]V, 0, need), o.vals...)
	}
}

// Len returns the number of entries.
func (o *omap[V]) Len() int {
	if o == nil {
		return 0
	}
	return len(o.keys)
}

// Get returns the value for key.
func (o *omap[V]) Get(key string) (V, bool) {
	if o == nil {
		var zero V
		return zero, false
	}
	if i := o.find(key); i >= 0 {
		return o.vals[i], true
	}
	var zero V
	return zero, false
}

// Has reports whether key is present.
func (o *omap[V]) Has(key string) bool {
	if o == nil {
		return false
	}
	return o.find(key) >= 0
}

// Set inserts or updates key.
func (o *omap[V]) Set(key string, v V) {
	if i := o.find(key); i >= 0 {
		o.vals[i] = v
		return
	}
	o.keys = append(o.keys, key)
	o.vals = append(o.vals, v)
	if o.idx != nil {
		o.idx[key] = len(o.keys) - 1
	} else if len(o.keys) > omapIndexThreshold {
		o.reindex()
	}
}

// Delete removes key (no-op when absent).
func (o *omap[V]) Delete(key string) {
	i := o.find(key)
	if i < 0 {
		return
	}
	o.keys = append(o.keys[:i:i], o.keys[i+1:]...)
	o.vals = append(o.vals[:i:i], o.vals[i+1:]...)
	if o.idx != nil {
		o.reindex()
	}
}

// Keys returns keys in insertion order (callers must not mutate).
func (o *omap[V]) Keys() []string {
	if o == nil {
		return nil
	}
	return o.keys
}

// Values returns values in insertion order.
func (o *omap[V]) Values() []V {
	if o == nil {
		return nil
	}
	return append(make([]V, 0, len(o.vals)), o.vals...)
}

// Copy returns a shallow copy.
func (o *omap[V]) Copy() *omap[V] {
	c := newOMap[V]()
	if o == nil {
		return c
	}
	c.keys = append([]string{}, o.keys...)
	c.vals = append([]V(nil), o.vals...)
	if o.idx != nil {
		c.reindex()
	}
	return c
}

// Update mirrors dict.update(other).
func (o *omap[V]) Update(other *omap[V]) {
	if other == nil {
		return
	}
	for i, k := range other.keys {
		o.Set(k, other.vals[i])
	}
}
