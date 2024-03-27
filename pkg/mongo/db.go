package mongo

type DB struct {
	config *Config
	Name   string
}

type MongoConnection interface {
	GetIngestrURI() (string, error)
}

func NewDB(c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Database}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}
