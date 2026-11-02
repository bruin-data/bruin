package cmd

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
