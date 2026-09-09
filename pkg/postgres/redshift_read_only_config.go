package postgres

func (c RedShiftConfig) IsReadOnly() bool {
	return c.ReadOnly
}
