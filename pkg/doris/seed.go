package doris

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type SeedOperator struct {
	connection config.ConnectionGetter
	renderer   jinja.RendererInterface
}

func NewSeedOperator(conn config.ConnectionGetter, renderer jinja.RendererInterface) *SeedOperator {
	return &SeedOperator{
		connection: conn,
		renderer:   renderer,
	}
}

func (o SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	p := ti.GetPipeline()

	connName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	conn, ok := rawConn.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a Doris connection", connName)
	}

	path, ok := asset.Parameters.GetString("path")
	if !ok || strings.TrimSpace(path) == "" {
		return errors.New("doris.seed requires a `path` parameter")
	}

	if o.renderer != nil {
		renderedPath, err := o.renderer.Render(path)
		if err != nil {
			return err
		}
		path = renderedPath
	}

	fileType, ok := asset.Parameters.GetString("file_type")
	if ok && fileType != "" && !strings.EqualFold(fileType, "csv") {
		return fmt.Errorf("doris.seed only supports CSV files, got %q", fileType)
	}

	absPath := path
	if !filepath.IsAbs(absPath) {
		absPath = filepath.Join(filepath.Dir(asset.ExecutableFile.Path), absPath)
	}

	header, rows, err := readCSV(absPath)
	if err != nil {
		return err
	}

	if len(header) == 0 {
		return fmt.Errorf("seed file %s has no header row", absPath)
	}

	ddlAsset := *asset
	if len(ddlAsset.Columns) == 0 {
		ddlAsset.Columns = columnsFromHeader(header)
	}

	if err := conn.CreateSchemaIfNotExist(ctx, &ddlAsset); err != nil {
		return err
	}

	if err := conn.RunQueryWithoutResult(ctx, &query.Query{Query: "DROP TABLE IF EXISTS " + quoteIdentifier(asset.Name)}); err != nil {
		return err
	}

	ddl, err := buildDDLQuery(&ddlAsset, "")
	if err != nil {
		return err
	}

	if err := conn.RunQueryWithoutResult(ctx, &query.Query{Query: ddl}); err != nil {
		return err
	}

	if len(rows) == 0 {
		return nil
	}

	return insertRows(ctx, conn, asset.Name, header, rows)
}

func readCSV(path string) ([]string, [][]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open seed file %s", path)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to read seed file header %s", path)
	}

	rows := make([][]string, 0)
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to read seed file %s", path)
		}
		rows = append(rows, record)
	}

	return header, rows, nil
}

func columnsFromHeader(header []string) []pipeline.Column {
	columns := make([]pipeline.Column, 0, len(header))
	for _, name := range header {
		columns = append(columns, pipeline.Column{
			Name: strings.TrimSpace(name),
			Type: "STRING",
		})
	}
	return columns
}

func insertRows(ctx context.Context, conn *Client, table string, columns []string, rows [][]string) error {
	columnNames := make([]string, 0, len(columns))
	for _, col := range columns {
		columnNames = append(columnNames, quoteColumnName(strings.TrimSpace(col)))
	}

	valueRows := make([]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, 0, len(columns))
		for i := range columns {
			var value string
			if i < len(row) {
				value = row[i]
			}
			values = append(values, quoteValue(value))
		}
		valueRows = append(valueRows, "("+strings.Join(values, ", ")+")")
	}

	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES\n%s",
		quoteIdentifier(table),
		strings.Join(columnNames, ", "),
		strings.Join(valueRows, ",\n"),
	)

	return conn.RunQueryWithoutResult(ctx, &query.Query{Query: insertQuery})
}

func quoteValue(value string) string {
	if value == "" {
		return "NULL"
	}

	return quoteStringLiteral(value)
}

func quoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
