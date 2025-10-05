package mongoatlas

type DB struct {
	config *Config
	Name   string
}

type MongoAtlasConnection interface {
	GetIngestrURI() (string, error)
}

func NewDB(c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Database}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}
