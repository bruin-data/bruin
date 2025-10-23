package couchbase

type DB struct {
	config *Config
	Name   string
}

type CouchbaseConnection interface {
	GetIngestrURI() (string, error)
}

func NewDB(c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Bucket}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}
