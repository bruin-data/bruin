package sqlengine

// omap is an insertion-ordered map mirroring Python dict semantics: updating an existing
// key keeps its position, deleting removes it, and re-adding appends at the end.
type omap[V any] struct {
	keys []string
	m    map[string]V
}

func newOMap[V any]() *omap[V] { return &omap[V]{m: map[string]V{}} }

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
	v, ok := o.m[key]
	return v, ok
}

// Has reports whether key is present.
func (o *omap[V]) Has(key string) bool {
	if o == nil {
		return false
	}
	_, ok := o.m[key]
	return ok
}

// Set inserts or updates key.
func (o *omap[V]) Set(key string, v V) {
	if _, ok := o.m[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.m[key] = v
}

// Delete removes key (no-op when absent).
func (o *omap[V]) Delete(key string) {
	if _, ok := o.m[key]; !ok {
		return
	}
	delete(o.m, key)
	for i, k := range o.keys {
		if k == key {
			o.keys = append(o.keys[:i:i], o.keys[i+1:]...)
			break
		}
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
	out := make([]V, 0, len(o.keys))
	for _, k := range o.keys {
		out = append(out, o.m[k])
	}
	return out
}

// Copy returns a shallow copy.
func (o *omap[V]) Copy() *omap[V] {
	c := newOMap[V]()
	if o == nil {
		return c
	}
	c.keys = append([]string{}, o.keys...)
	for k, v := range o.m {
		c.m[k] = v
	}
	return c
}

// Update mirrors dict.update(other).
func (o *omap[V]) Update(other *omap[V]) {
	if other == nil {
		return
	}
	for _, k := range other.keys {
		o.Set(k, other.m[k])
	}
}
