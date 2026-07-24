package spark

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/tablename"
	"github.com/pkg/errors"
)

type SeedOperator struct {
	connection config.ConnectionGetter
	renderer   jinja.RendererInterface
}

func NewSeedOperator(connection config.ConnectionGetter, renderer jinja.RendererInterface) *SeedOperator {
	return &SeedOperator{connection: connection, renderer: renderer}
}

func (o SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	p := ti.GetPipeline()
	connectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}
	rawConnection := o.connection.GetConnection(connectionName)
	if rawConnection == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}
	client, ok := rawConnection.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a Spark connection", connectionName)
	}

	seedPath, ok := asset.Parameters.GetString("path")
	if !ok || strings.TrimSpace(seedPath) == "" {
		return errors.New("spark.seed requires a `path` parameter")
	}
	if o.renderer != nil {
		seedPath, err = o.renderer.Render(seedPath)
		if err != nil {
			return err
		}
	}
	if fileType, ok := asset.Parameters.GetString("file_type"); ok {
		if err := validateSparkSeedFileType(fileType); err != nil {
			return err
		}
	}

	data, err := readSeed(ctx, asset, seedPath)
	if err != nil {
		return err
	}
	header, err := csv.NewReader(bytes.NewReader(data)).Read()
	if err != nil {
		return errors.Wrap(err, "failed to read Spark seed header")
	}
	if len(header) == 0 {
		return errors.New("Spark seed file has no header row")
	}

	fields := sparkSeedFields(asset.Columns, header)
	reader := arrowcsv.NewReader(
		bytes.NewReader(data),
		arrow.NewSchema(fields, nil),
		arrowcsv.WithHeader(true),
		arrowcsv.WithChunk(4096),
		arrowcsv.WithNullReader(false),
	)
	defer reader.Release()

	if err := client.CreateSchemaIfNotExist(ctx, asset, p.Name); err != nil {
		return err
	}
	options, err := client.config.ToOptions()
	if err != nil {
		return err
	}
	database, err := newADBCDatabase(options)
	if err != nil {
		return errors.Wrap(err, "failed to create Spark ADBC database")
	}
	defer database.Close()
	connection, err := database.Open(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to open Spark ADBC connection")
	}
	defer connection.Close()

	targetTable, err := configureIngestNamespace(ctx, connection, asset.Name)
	if err != nil {
		return err
	}
	if _, err := adbc.IngestStream(
		ctx,
		connection,
		reader,
		targetTable,
		adbc.OptionValueIngestModeReplace,
		adbc.IngestStreamOptions{Extra: client.config.IngestOptions()},
	); err != nil {
		return errors.Wrap(err, "failed to ingest Spark seed")
	}
	return nil
}

func validateSparkSeedFileType(fileType string) error {
	if normalized := strings.TrimSpace(fileType); normalized != "" && !strings.EqualFold(normalized, "csv") {
		return fmt.Errorf("spark.seed only supports CSV files, got %q", fileType)
	}
	return nil
}

func sparkSeedFields(columns []pipeline.Column, header []string) []arrow.Field {
	columnTypes := make(map[string]arrow.DataType, len(columns))
	for _, column := range columns {
		columnTypes[strings.ToLower(strings.TrimSpace(column.Name))] = sparkArrowType(column.Type)
	}

	fields := make([]arrow.Field, len(header))
	for i, name := range header {
		name = strings.TrimSpace(name)
		dataType := arrow.DataType(arrow.BinaryTypes.String)
		if configuredType, ok := columnTypes[strings.ToLower(name)]; ok {
			dataType = configuredType
		}
		fields[i] = arrow.Field{Name: name, Type: dataType, Nullable: true}
	}
	return fields
}

func sparkArrowType(columnType string) arrow.DataType { //nolint:ireturn
	baseType := strings.ToUpper(strings.TrimSpace(columnType))
	if index := strings.IndexRune(baseType, '('); index >= 0 {
		baseType = strings.TrimSpace(baseType[:index])
	}
	switch baseType {
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean
	case "BYTE", "TINYINT":
		// The Spark ADBC driver cannot ingest Arrow int8, so widen tiny
		// integers losslessly to the supported int16 representation.
		return arrow.PrimitiveTypes.Int16
	case "SHORT", "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INT", "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT", "LONG":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT", "REAL":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

func configureIngestNamespace(ctx context.Context, connection adbc.Connection, assetName string) (string, error) {
	capability, ok := tablename.For("spark")
	if !ok {
		return "", errors.New("Spark table-name capability not found")
	}
	name, err := capability.Parse(assetName, tablename.Defaults{})
	if err != nil {
		return "", err
	}

	namespace := name.Schema
	if name.Catalog != "" {
		namespace = name.Catalog
		if name.Schema != "" {
			namespace += "." + name.Schema
		}
	}
	if namespace != "" {
		statement, err := connection.NewStatement()
		if err != nil {
			return "", errors.Wrap(err, "failed to create Spark namespace statement")
		}
		if err := statement.SetSqlQuery("USE " + quoteIdentifier(namespace)); err != nil {
			_ = statement.Close()
			return "", errors.Wrap(err, "failed to configure Spark seed namespace")
		}
		if _, err := statement.ExecuteUpdate(ctx); err != nil {
			_ = statement.Close()
			return "", errors.Wrap(err, "failed to configure Spark seed namespace")
		}
		if err := statement.Close(); err != nil {
			return "", errors.Wrap(err, "failed to close Spark namespace statement")
		}
	}
	return name.Table, nil
}

func readSeed(ctx context.Context, asset *pipeline.Asset, seedPath string) ([]byte, error) {
	lowerSeedPath := strings.ToLower(seedPath)
	if strings.HasPrefix(lowerSeedPath, "http://") || strings.HasPrefix(lowerSeedPath, "https://") {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, seedPath, nil)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Spark seed request")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return nil, errors.Wrap(err, "failed to download Spark seed")
		}
		defer response.Body.Close()
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("failed to download Spark seed: HTTP %s", response.Status)
		}
		data, err := io.ReadAll(response.Body)
		return data, errors.Wrap(err, "failed to read Spark seed response")
	}

	localPath := seedPath
	if !filepath.IsAbs(localPath) {
		localPath = filepath.Join(filepath.Dir(asset.ExecutableFile.Path), localPath)
	}
	data, err := os.ReadFile(localPath)
	return data, errors.Wrapf(err, "failed to read Spark seed file %s", localPath)
}
