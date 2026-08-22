package mongoatlas

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	client *mongo.Client
	config *Config
	Name   string

	mu sync.Mutex
}

type MongoAtlasConnection interface {
	GetIngestrURI() (string, error)
	Ping(ctx context.Context) error
}

func NewDB(c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Database}, nil
}

// NewClient returns a DB without opening a connection. The underlying MongoDB
// client is created lazily by ensureClient on first actual use, so merely
// registering the connection never starts driver monitoring or DNS discovery.
func NewClient(_ context.Context, c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Database}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

// ensureClient lazily establishes the underlying MongoDB client on first use.
func (db *DB) ensureClient(ctx context.Context) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.client != nil {
		return nil
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(db.config.GetIngestrURI()))
	if err != nil {
		return errors.Wrap(err, "failed to create MongoDB client")
	}

	db.client = client
	return nil
}

func (db *DB) Ping(ctx context.Context) error {
	if err := db.ensureClient(ctx); err != nil {
		return err
	}

	if err := db.client.Ping(ctx, nil); err != nil {
		return errors.Wrap(err, "failed to ping MongoDB Atlas connection")
	}

	return nil
}
