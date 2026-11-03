package cmd

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

// resultSink writes a streamed result to its destination as rows arrive.
type resultSink interface {
	writeHeader(columns, columnTypes []string) error
	writeRow(row []interface{}) error
	// finish completes the output; result carries the columns when no row arrived.
	finish(result *query.QueryResult) error
	// abort ends the output after err and returns the error the command should exit with.
	abort(err error) error
}

// runStreamedQuery writes rows to stdout or the export files as they arrive, so memory stays flat regardless of result size.
func runStreamedQuery(ctx context.Context, c *cli.Command, streamer query.RowStreamer, q *query.Query, connName, queryStr string, logOpts QueryLogOptions) error {
	output := c.String("output")

	var sink resultSink
	var exports *exportSink
	switch {
	case c.Bool("export"):
		exportsDir, err := prepareExportsDir(logOpts.Asset)
		if err != nil {
			return handleError(output, errors.Wrap(err, "failed to export results to CSV"))
		}
		exports = newExportSink(output, exportsDir, c.Int("split-rows"), time.Now().UnixMilli())
		sink = exports
	case output == "json":
		sink = &jsonSink{w: bufio.NewWriter(os.Stdout), connName: connName, queryStr: queryStr}
	case output == "csv":
		sink = newCSVSink(bufio.NewWriter(os.Stdout))
	default:
		sink = &plainSink{w: bufio.NewWriter(os.Stdout)}
	}

	handler := &streamHandler{sink: sink, log: openQueryLog(queryStr, connName, logOpts)}
	result, queryErr := streamer.StreamWithSchema(ctx, q, handler)

	if logErr := handler.log.finish(queryErr); logErr != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to save query log: %v\n", logErr)
	}

	err := queryErr
	switch {
	case err != nil && handler.sinkErr == nil:
		err = errors.Wrap(err, "query execution failed")
	case err == nil:
		err = sink.finish(result)
	}

	if err != nil {
		return sink.abort(err)
	}
	if exports != nil {
		if exports.splitRows > 0 {
			return handleMultipleFilesSuccess(output, exports.paths)
		}
		return handleSuccess(output, "Results Successfully exported to "+exports.paths[0])
	}
	return nil
}

// streamHandler forwards a streamed result to a sink and writes every row to the query log.
// The log never fails the query: a log write error only stops the log.
type streamHandler struct {
	sink    resultSink
	log     *queryLogWriter
	sinkErr error
}

func (h *streamHandler) Schema(columns, columnTypes []string) error {
	h.log.writeColumns(columns)
	if err := h.sink.writeHeader(columns, columnTypes); err != nil {
		h.sinkErr = err
		return err
	}
	return nil
}

func (h *streamHandler) Row(row []interface{}) error {
	h.log.writeRow(row)
	if err := h.sink.writeRow(row); err != nil {
		h.sinkErr = err
		return err
	}
	return nil
}

type csvSink struct {
	w       *bufio.Writer
	csv     *csv.Writer
	started bool
}

func newCSVSink(w *bufio.Writer) *csvSink {
	return &csvSink{w: w, csv: csv.NewWriter(w)}
}

func (s *csvSink) writeHeader(columns, _ []string) error {
	s.started = true
	if err := s.csv.Write(columns); err != nil {
		return errors.Wrap(err, "failed to write CSV header")
	}
	return nil
}

func (s *csvSink) writeRow(row []interface{}) error {
	if err := s.csv.Write(formatQueryRowForCSV(row)); err != nil {
		return errors.Wrap(err, "failed to write CSV row")
	}
	return nil
}

func (s *csvSink) finish(result *query.QueryResult) error {
	if !s.started {
		s.started = true
		if shouldOutputExecutionSummary(result) {
			if err := writeExecutionSummaryCSV(s.csv, result.Execution); err != nil {
				return err
			}
		} else if err := s.writeHeader(result.Columns, nil); err != nil {
			return err
		}
	}
	return s.flush()
}

func (s *csvSink) flush() error {
	s.csv.Flush()
	if err := s.csv.Error(); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *csvSink) abort(err error) error {
	if !s.started {
		return handleError("csv", err)
	}
	// Rows already reached stdout, so the error goes to stderr to keep it out of the CSV.
	_ = s.flush()
	fmt.Fprintln(os.Stderr, "Error:", err.Error())
	return cli.Exit("", 1)
}

// plainTableMaxRows is the largest result shown as a table; a table needs every row to size its columns.
const plainTableMaxRows = 100

// plainSink shows results of up to plainTableMaxRows rows as a table.
// Larger results switch to CSV, so the remaining rows stream instead of being held for the table.
type plainSink struct {
	w       *bufio.Writer
	columns []string
	rows    [][]interface{}
	csv     *csvSink
}

func (s *plainSink) writeHeader(columns, _ []string) error {
	s.columns = columns
	return nil
}

func (s *plainSink) writeRow(row []interface{}) error {
	if s.csv != nil {
		return s.csv.writeRow(row)
	}
	if len(s.rows) < plainTableMaxRows {
		s.rows = append(s.rows, row)
		return nil
	}

	s.csv = newCSVSink(s.w)
	if err := s.csv.writeHeader(s.columns, nil); err != nil {
		return err
	}
	for _, buffered := range append(s.rows, row) {
		if err := s.csv.writeRow(buffered); err != nil {
			return err
		}
	}
	s.rows = nil
	return nil
}

func (s *plainSink) finish(result *query.QueryResult) error {
	if s.csv != nil {
		return s.csv.finish(result)
	}
	if shouldOutputExecutionSummary(result) && len(s.rows) == 0 {
		printQueryExecutionSummary(result.Execution)
		return nil
	}
	columns := s.columns
	if columns == nil {
		columns = result.Columns
	}
	renderTable(s.w, columns, s.rows)
	return s.w.Flush()
}

func (s *plainSink) abort(err error) error {
	if s.csv != nil {
		return s.csv.abort(err)
	}
	return handleError(outputFormatPlain, err)
}

// jsonSink writes the same document json.Marshal produces for the buffered output path.
type jsonSink struct {
	w        *bufio.Writer
	connName string
	queryStr string
	started  bool
	rows     int
}

func (s *jsonSink) writeHeader(columns, columnTypes []string) error {
	s.started = true
	jsonColumns, err := json.Marshal(queryJSONColumns(&query.QueryResult{Columns: columns, ColumnTypes: columnTypes}))
	if err != nil {
		return errors.Wrap(err, "failed to marshal result to JSON")
	}
	_, err = fmt.Fprintf(s.w, `{"columns":%s,"rows":[`, jsonColumns)
	return err
}

func (s *jsonSink) writeRow(row []interface{}) error {
	data, err := json.Marshal(formatQueryRowForJSON(row))
	if err != nil {
		return errors.Wrap(err, "failed to marshal result to JSON")
	}
	if s.rows > 0 {
		if err := s.w.WriteByte(','); err != nil {
			return err
		}
	}
	s.rows++
	_, err = s.w.Write(data)
	return err
}

func (s *jsonSink) finish(result *query.QueryResult) error {
	if !s.started {
		if err := s.writeHeader(result.Columns, result.ColumnTypes); err != nil {
			return err
		}
	}
	var execution []byte
	if s.rows == 0 && shouldOutputExecutionSummary(result) {
		var err error
		if execution, err = json.Marshal(result.Execution); err != nil {
			return errors.Wrap(err, "failed to marshal result to JSON")
		}
	}
	return s.close("execution", execution)
}

// close ends the rows array and the document, adding one optional trailing field.
func (s *jsonSink) close(key string, value []byte) error {
	name, err := json.Marshal(s.connName)
	if err != nil {
		return err
	}
	queryJSON, err := json.Marshal(s.queryStr)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(s.w, `],"connectionName":%s,"query":%s`, name, queryJSON); err != nil {
		return err
	}
	if value != nil {
		if _, err := fmt.Fprintf(s.w, `,%q:%s`, key, value); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(s.w, "}\n"); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *jsonSink) abort(err error) error {
	if !s.started {
		return handleError("json", err)
	}
	// Close the document so a reader still gets valid JSON, with the error alongside the rows already sent.
	message, marshalErr := json.Marshal(err.Error())
	if marshalErr != nil {
		message = nil
	}
	_ = s.close("error", message)
	return cli.Exit("", 1)
}

// exportSink writes CSV files under logs/exports, starting a new file every splitRows rows.
type exportSink struct {
	output    string
	dir       string
	splitRows int
	timestamp int64
	columns   []string
	paths     []string
	file      *os.File
	csv       *csv.Writer
	partRows  int
}

func newExportSink(output, dir string, splitRows int, timestamp int64) *exportSink {
	return &exportSink{output: output, dir: dir, splitRows: splitRows, timestamp: timestamp}
}

func (s *exportSink) writeHeader(columns, _ []string) error {
	s.columns = columns
	return exportErr(s.openFile())
}

func exportErr(err error) error {
	if err == nil {
		return nil
	}
	return errors.Wrap(err, "failed to export results to CSV")
}

func (s *exportSink) openFile() error {
	if err := s.closeFile(); err != nil {
		return err
	}
	name := fmt.Sprintf("query_result_%d.csv", s.timestamp)
	if s.splitRows > 0 {
		name = fmt.Sprintf("query_result_%d_part%d.csv", s.timestamp, len(s.paths)+1)
	}
	path := filepath.Join(s.dir, name)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	s.paths = append(s.paths, path)
	s.file = file
	s.csv = csv.NewWriter(file)
	s.partRows = 0
	return s.csv.Write(s.columns)
}

func (s *exportSink) writeRow(row []interface{}) error {
	if s.splitRows > 0 && s.partRows == s.splitRows {
		if err := s.openFile(); err != nil {
			return exportErr(err)
		}
	}
	s.partRows++
	return exportErr(s.csv.Write(formatQueryRowForCSV(row)))
}

func (s *exportSink) closeFile() error {
	if s.file == nil {
		return nil
	}
	s.csv.Flush()
	err := s.csv.Error()
	if closeErr := s.file.Close(); err == nil {
		err = closeErr
	}
	s.file = nil
	return err
}

func (s *exportSink) finish(result *query.QueryResult) error {
	if len(s.paths) == 0 {
		if err := s.writeHeader(result.Columns, nil); err != nil {
			return err
		}
	}
	return exportErr(s.closeFile())
}

func (s *exportSink) abort(err error) error {
	_ = s.closeFile()
	for _, path := range s.paths {
		_ = os.Remove(path)
	}
	return handleError(s.output, err)
}
