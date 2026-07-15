package starrocks

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
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
		return errors.Errorf("connection '%s' is not a StarRocks connection", connName)
	}

	path, ok := asset.Parameters.GetString("path")
	if !ok || strings.TrimSpace(path) == "" {
		return errors.New("starrocks.seed requires a `path` parameter")
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
		return fmt.Errorf("starrocks.seed only supports CSV files, got %q", fileType)
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

	tempTable := temporaryTableName(asset.Name, temporaryTableRunID(), "seed")
	ddlAsset.Name = tempTable

	if err := conn.RunQueryWithoutResult(ctx, &query.Query{Query: "DROP TABLE IF EXISTS " + quoteIdentifier(tempTable)}); err != nil {
		return err
	}

	ddl, err := buildDDLQuery(&ddlAsset, "")
	if err != nil {
		return err
	}

	if err := conn.RunQueryWithoutResult(ctx, &query.Query{Query: ddl}); err != nil {
		return err
	}

	if len(rows) > 0 {
		if err := insertRows(ctx, conn, tempTable, header, rows); err != nil {
			return err
		}
	}

	exists, err := tableExists(ctx, conn, asset.Name)
	if err != nil {
		return err
	}

	swapQuery := renameTableQuery(tempTable, asset.Name)
	if exists {
		swapQuery = replaceTableQuery(asset.Name, tempTable)
	}

	return conn.RunQueryWithoutResult(ctx, &query.Query{Query: swapQuery})
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
	return quoteStringLiteral(value)
}

func quoteStringLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, "'", "''")
	return "'" + escaped + "'"
}

func tableExists(ctx context.Context, conn *Client, table string) (bool, error) {
	existsQuery, err := conn.BuildTableExistsQuery(table)
	if err != nil {
		return false, err
	}

	rows, err := conn.Select(ctx, &query.Query{Query: existsQuery})
	if err != nil {
		return false, err
	}

	if len(rows) == 0 || len(rows[0]) == 0 {
		return false, nil
	}

	count, err := strconv.ParseInt(fmt.Sprint(rows[0][0]), 10, 64)
	if err != nil {
		return false, fmt.Errorf("failed to parse StarRocks table existence result: %w", err)
	}

	return count > 0, nil
}
