package redshift

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Schema   string
	SslMode  string
}

// ToPostgresConfig converts redshift.Config to postgres.RedShiftConfig for compatibility
func (c Config) ToPostgresConfig() *postgres.RedShiftConfig {
	return &postgres.RedShiftConfig{
		Username: c.Username,
		Password: c.Password,
		Host:     c.Host,
		Port:     c.Port,
		Database: c.Database,
		Schema:   c.Schema,
		SslMode:  c.SslMode,
	}
}
type TableSensorClient struct {
	connection *pgxpool.Pool
}

func (c *TableSensorClient) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	rows, err := c.connection.Query(ctx, query.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}
	if len(collectedRows) == 0 {
		return make([][]interface{}, 0), nil
	}
	return collectedRows, nil
}

func (c *TableSensorClient) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
		}
	}
	var schemaName string
	switch len(tableComponents) {
	case 1:
		schemaName = "public"
		tableName = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		tableName = tableComponents[1]
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}
	targetTable := tableName
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = '%s' AND tablename = '%s'",
		schemaName,
		targetTable,
	)
	return strings.TrimSpace(query), nil
}
