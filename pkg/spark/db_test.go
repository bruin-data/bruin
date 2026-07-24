package spark

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestCreateSchemaIfNotExist(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	client := &Client{connection: db}
	mock.ExpectExec(regexp.QuoteMeta("CREATE SCHEMA IF NOT EXISTS `catalog`.`analytics`")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	asset := &pipeline.Asset{Name: "catalog.analytics.events"}
	require.NoError(t, client.CreateSchemaIfNotExist(context.Background(), asset, "pipeline"))
	require.NoError(t, client.CreateSchemaIfNotExist(context.Background(), asset, "pipeline"), "schema creation should be cached")
	mock.ExpectClose()
	require.NoError(t, db.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateSchemaIfNotExistAnnotations(t *testing.T) {
	t.Parallel()

	connection := &recordingConnection{}
	client := &Client{connection: connection}
	asset := &pipeline.Asset{Name: "catalog.analytics.events"}
	ctx := context.WithValue(
		t.Context(),
		pipeline.RunConfigQueryAnnotations,
		ansisql.DefaultQueryAnnotations,
	)

	require.NoError(t, client.CreateSchemaIfNotExist(ctx, asset, "events_pipeline"))
	require.Equal(t, []string{query.QueryTypeSchema}, connection.queryTypes)
	require.Equal(
		t,
		[]string{
			"-- @bruin.config: {\"asset\":\"catalog.analytics.events\",\"pipeline\":\"events_pipeline\",\"type\":\"schema\"}\n" +
				"CREATE SCHEMA IF NOT EXISTS `catalog`.`analytics`",
		},
		connection.queries,
	)
}

func TestAppendObjectCatalogs(t *testing.T) {
	t.Parallel()

	columnType := "STRING"
	nullable := "NO"
	remarks := "event identifier"
	schemas := make(map[string]*ansisql.DBSchema)
	appendObjectCatalogs(schemas, []objectCatalog{{
		Name: pointer("spark_catalog"),
		Schemas: []objectSchema{{
			Name: pointer("analytics"),
			Tables: []objectTable{{
				Name: "events",
				Type: "TABLE",
				Columns: []objectColumn{{
					Name: "event_id", Type: &columnType, Nullable: &nullable, Remarks: &remarks,
				}},
			}},
		}},
	}}, true)

	require.Equal(t, &ansisql.DBSchema{
		Name: "spark_catalog.analytics",
		Tables: []*ansisql.DBTable{{
			Name: "events",
			Type: ansisql.DBTableTypeTable,
			Columns: []*ansisql.DBColumn{{
				Name: "event_id", Type: "STRING", Nullable: false, Description: "event identifier",
			}},
		}},
	}, schemas["spark_catalog.analytics"])
}

func TestObjectCatalogsContainTable(t *testing.T) {
	t.Parallel()

	catalogs := []objectCatalog{{
		Name: pointer("local"),
		Schemas: []objectSchema{{
			Name: pointer("analytics"),
			Tables: []objectTable{{
				Name: "events",
				Type: "TABLE",
			}},
		}},
	}}

	require.True(t, objectCatalogsContainTable(catalogs, "local", "analytics", "events"))
	require.False(t, objectCatalogsContainTable(catalogs, "", "analytics", "events"))
	require.False(t, objectCatalogsContainTable(catalogs, "spark_catalog", "analytics", "events"))
	require.False(t, objectCatalogsContainTable(catalogs, "local", "other", "events"))
	require.False(t, objectCatalogsContainTable(catalogs, "local", "analytics", "missing"))
}

func pointer(value string) *string {
	return &value
}
