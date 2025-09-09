package redshift

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/postgres"
)

type TableSensorClient struct {
	PostgresClient *postgres.Client
}

func NewTableSensorClient(ctx context.Context, c postgres.RedShiftConfig) (*TableSensorClient, error) {
	postgresClient, err := postgres.NewClient(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redshift table sensor client: %w", err)
	}

	return &TableSensorClient{
		PostgresClient: postgresClient,
	}, nil
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
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",
		schemaName,
		targetTable,
	)
	return strings.TrimSpace(query), nil
}
