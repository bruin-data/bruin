package spark

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/tablename"
	"github.com/pkg/errors"
)

type TableSensor struct {
	connection config.ConnectionGetter
	sensorMode string
}

func NewTableSensor(connection config.ConnectionGetter, sensorMode string) *TableSensor {
	return &TableSensor{connection: connection, sensorMode: sensorMode}
}

func (s *TableSensor) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return s.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (s *TableSensor) RunTask(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) error {
	if s.sensorMode == "skip" {
		return nil
	}
	tableName, ok := asset.Parameters.GetString("table")
	if !ok {
		return errors.New("table sensor requires a parameter named 'table'")
	}
	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}
	rawConnection := s.connection.GetConnection(connectionName)
	if rawConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}
	connection, ok := rawConnection.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a Spark connection", connectionName)
	}

	options, err := connection.config.ToOptions()
	if err != nil {
		return err
	}
	database, err := newADBCDatabase(options)
	if err != nil {
		return errors.Wrap(err, "failed to create Spark ADBC database")
	}
	defer database.Close()
	metadataConnection, err := database.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to open Spark ADBC connection")
	}
	defer metadataConnection.Close()

	defaultCatalog, currentSchema, err := sparkNamespaceDefaults(
		ctx,
		metadataConnection,
		connection.config.Catalog,
	)
	if err != nil {
		return err
	}

	printer, printerExists := ctx.Value(executor.KeyPrinter).(io.Writer)
	if printerExists {
		fmt.Fprintln(printer, "Poking:", tableName)
	}
	timeout := time.NewTimer(helpers.GetSensorTimeout(asset))
	defer timeout.Stop()

	for {
		exists, err := tableExists(ctx, metadataConnection, tableName, defaultCatalog, currentSchema)
		if err != nil {
			return err
		}
		if exists {
			return nil
		}
		if s.sensorMode == "once" || s.sensorMode == "" {
			return errors.New("Sensor didn't return the expected result")
		}

		pokeInterval := time.Duration(helpers.GetPokeInterval(ctx, asset)) * time.Second
		wait := time.NewTimer(pokeInterval)
		select {
		case <-ctx.Done():
			wait.Stop()
			return ctx.Err()
		case <-timeout.C:
			wait.Stop()
			return errors.Errorf("Sensor timed out after %s", helpers.GetSensorTimeout(asset))
		case <-wait.C:
			if printerExists {
				fmt.Fprintln(printer, "Info: Sensor didn't return the expected result, waiting for", pokeInterval)
			}
		}
	}
}

func tableExists(
	ctx context.Context,
	connection adbc.Connection,
	rawTableName,
	defaultCatalog,
	defaultSchema string,
) (bool, error) {
	capability, ok := tablename.For("spark")
	if !ok {
		return false, errors.New("Spark table-name capability not found")
	}
	name, err := capability.Parse(rawTableName, tablename.Defaults{Catalog: defaultCatalog, Schema: defaultSchema})
	if err != nil {
		return false, err
	}

	var catalogFilter *string
	if name.Catalog != "" {
		catalogFilter = &name.Catalog
	}
	reader, err := connection.GetObjects(
		ctx,
		adbc.ObjectDepthTables,
		catalogFilter,
		&name.Schema,
		&name.Table,
		nil,
		nil,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to retrieve Spark table metadata")
	}
	defer reader.Release()

	for reader.Next() {
		data, err := reader.RecordBatch().MarshalJSON()
		if err != nil {
			return false, errors.Wrap(err, "failed to encode Spark table metadata")
		}
		var catalogs []objectCatalog
		if err := json.Unmarshal(data, &catalogs); err != nil {
			return false, errors.Wrap(err, "failed to decode Spark table metadata")
		}
		if objectCatalogsContainTable(catalogs, name.Catalog, name.Schema, name.Table) {
			return true, nil
		}
	}
	if err := reader.Err(); err != nil {
		return false, errors.Wrap(err, "failed while reading Spark table metadata")
	}
	return false, nil
}

func currentSparkNamespace(ctx context.Context, connection adbc.Connection) (string, string, error) {
	statement, err := connection.NewStatement()
	if err != nil {
		return "", "", errors.Wrap(err, "failed to create Spark current-namespace statement")
	}
	defer statement.Close()

	if err := statement.SetSqlQuery("SELECT current_catalog(), current_database()"); err != nil {
		return "", "", errors.Wrap(err, "failed to configure Spark current-namespace query")
	}
	reader, _, err := statement.ExecuteQuery(ctx)
	if err != nil {
		return "", "", errors.Wrap(err, "failed to query Spark current namespace")
	}
	if reader == nil {
		return "", "", errors.New("Spark current-namespace query returned no result")
	}
	defer reader.Release()

	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return "", "", errors.Wrap(err, "failed while reading Spark current namespace")
		}
		return "", "", errors.New("Spark current-namespace query returned no rows")
	}
	record := reader.RecordBatch()
	if record.NumRows() == 0 || record.NumCols() < 2 {
		return "", "", errors.New("Spark current-namespace query returned an incomplete result")
	}
	values := make([]string, 2)
	for index := range values {
		if !record.Column(index).IsNull(0) {
			values[index] = strings.TrimSpace(record.Column(index).ValueStr(0))
		}
	}
	return values[0], values[1], nil
}

func sparkNamespaceDefaults(
	ctx context.Context,
	connection adbc.Connection,
	configuredCatalog string,
) (string, string, error) {
	currentCatalog, currentSchema, err := currentSparkNamespace(ctx, connection)
	if err != nil {
		return "", "", err
	}
	if configuredCatalog != "" {
		currentCatalog = configuredCatalog
	}
	if currentCatalog == "" {
		return "", "", errors.New("Spark current-namespace query returned an empty catalog")
	}
	if currentSchema == "" {
		currentSchema = "default"
	}
	return currentCatalog, currentSchema, nil
}

func objectCatalogsContainTable(catalogs []objectCatalog, catalogName, schemaName, tableName string) bool {
	if catalogName == "" {
		return false
	}
	for _, catalog := range catalogs {
		if catalog.Name == nil || !strings.EqualFold(*catalog.Name, catalogName) {
			continue
		}
		for _, schema := range catalog.Schemas {
			if schema.Name == nil || !strings.EqualFold(*schema.Name, schemaName) {
				continue
			}
			for _, table := range schema.Tables {
				if strings.EqualFold(table.Name, tableName) {
					return true
				}
			}
		}
	}
	return false
}
