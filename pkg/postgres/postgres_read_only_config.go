package postgres

func (c Config) IsReadOnly() bool {
	return c.ReadOnly
}
