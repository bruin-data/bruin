package mongoatlas

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DB struct {
	client *mongo.Client
	config *Config
	Name   string
}

type MongoAtlasConnection interface {
	GetIngestrURI() (string, error)
	Ping(ctx context.Context) error
}

func NewDB(c *Config) (*DB, error) {
	return &DB{config: c, Name: c.Database}, nil
}

func NewClient(ctx context.Context, c *Config) (*DB, error) {
	clientOptions := options.Client().ApplyURI(c.GetIngestrURI())

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create MongoDB client")
	}

	return &DB{
		client: client,
		config: c,
		Name:   c.Database,
	}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) Ping(ctx context.Context) error {
	if db.client == nil {
		return errors.New("MongoDB client is not initialized")
	}

	err := db.client.Ping(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to ping MongoDB Atlas connection")
	}

	return nil
}
