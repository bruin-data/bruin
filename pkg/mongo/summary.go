package mongo

import (
	"context"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// systemDatabases are MongoDB's internal databases. They are skipped when
// importing, since they hold server state rather than user data.
var systemDatabases = map[string]bool{
	"admin":  true,
	"local":  true,
	"config": true,
}

// DatabaseSummary enumerates the databases and collections reachable through the
// given client and returns them as an ansisql.DBDatabase tree: each MongoDB
// database maps to a schema and each collection to a table. MongoDB is
// schemaless, so tables are returned without columns. Row counts are populated
// best-effort. It is shared by the mongo and mongo_atlas connection types.
func DatabaseSummary(ctx context.Context, client *mongo.Client, summaryName string) (*ansisql.DBDatabase, error) {
	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list MongoDB databases")
	}

	collectionsByDB := make(map[string][]string, len(dbNames))
	for _, dbName := range dbNames {
		if systemDatabases[dbName] {
			continue
		}

		collections, err := client.Database(dbName).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			// Skip databases we cannot introspect (e.g. missing privileges) rather
			// than failing the whole import.
			continue
		}
		collectionsByDB[dbName] = collections
	}

	if summaryName == "" {
		summaryName = "mongo"
	}
	summary := buildDatabaseSummary(summaryName, collectionsByDB)

	// Best-effort row counts; ignore failures so a slow or locked collection does
	// not break the import. EstimatedDocumentCount reads collection metadata and
	// avoids a full scan.
	for _, schema := range summary.Schemas {
		for _, table := range schema.Tables {
			count, err := client.Database(schema.Name).Collection(table.Name).EstimatedDocumentCount(ctx)
			if err != nil {
				continue
			}
			rowCount := count
			table.RowCount = &rowCount
		}
	}

	return summary, nil
}

// buildDatabaseSummary turns a database->collections map into an ansisql.DBDatabase
// tree. It drops system databases and system collections, and sorts databases and
// collections so the output is deterministic. It performs no I/O, which keeps it
// unit-testable without a live MongoDB.
func buildDatabaseSummary(name string, collectionsByDB map[string][]string) *ansisql.DBDatabase {
	dbNames := make([]string, 0, len(collectionsByDB))
	for dbName := range collectionsByDB {
		if systemDatabases[dbName] {
			continue
		}
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	schemas := make([]*ansisql.DBSchema, 0, len(dbNames))
	for _, dbName := range dbNames {
		collections := make([]string, 0, len(collectionsByDB[dbName]))
		for _, coll := range collectionsByDB[dbName] {
			if strings.HasPrefix(coll, "system.") {
				continue
			}
			collections = append(collections, coll)
		}
		sort.Strings(collections)

		tables := make([]*ansisql.DBTable, 0, len(collections))
		for _, coll := range collections {
			tables = append(tables, &ansisql.DBTable{
				Name:    coll,
				Type:    ansisql.DBTableTypeTable,
				Columns: []*ansisql.DBColumn{},
			})
		}

		schemas = append(schemas, &ansisql.DBSchema{Name: dbName, Tables: tables})
	}

	return &ansisql.DBDatabase{Name: name, Schemas: schemas}
}
