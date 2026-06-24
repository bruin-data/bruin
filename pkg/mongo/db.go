package mongo

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	config *Config
	Name   string

	mu     sync.Mutex
	client *mongo.Client
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

// initClient lazily establishes a connection to MongoDB. The connection is not
// opened in NewDB so that the ingestr-only code paths (which only need the URI)
// never pay for a live connection.
func (db *DB) initClient(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.client != nil {
		return nil
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(db.config.GetIngestrURI()))
	if err != nil {
		return errors.Wrap(err, "failed to connect to MongoDB")
	}

	db.client = client
	return nil
}

func (db *DB) Ping(ctx context.Context) error {
	if err := db.initClient(ctx); err != nil {
		return err
	}

	if err := db.client.Ping(ctx, nil); err != nil {
		return errors.Wrap(err, "failed to ping MongoDB connection")
	}

	return nil
}
