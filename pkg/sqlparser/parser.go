package sqlparser

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bruin-data/bruin/internal/data"
	"github.com/bruin-data/bruin/pythonsrc"
	"github.com/kluctl/go-embed-python/embed_util"
	"github.com/kluctl/go-embed-python/python"
	"github.com/pkg/errors"
)

type SQLParser struct {
	ep          *python.EmbeddedPython
	sqlglotDir  *embed_util.EmbeddedFiles
	rendererSrc *embed_util.EmbeddedFiles
	started     bool

	stdout io.ReadCloser
	stdin  io.WriteCloser
	cmd    *exec.Cmd
	mutex  sync.Mutex

	startMutex sync.Mutex
}

func NewSQLParser(randomize bool) (*SQLParser, error) {
	randomInt := 0
	if randomize {
		b := make([]byte, 4)
		_, err := rand.Read(b)
		if err != nil {
			return nil, err
		}
		randomInt = int(b[0])
	}
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("bruin-cli-embedded_%d", randomInt))

	ep, err := python.NewEmbeddedPythonWithTmpDir(tmpDir+"-python", true)
	if err != nil {
		return nil, err
	}
	sqlglotDir, err := embed_util.NewEmbeddedFilesWithTmpDir(data.Data, tmpDir+"-sqlglot-lib", true)
	if err != nil {
		return nil, err
	}
	ep.AddPythonPath(sqlglotDir.GetExtractedPath())

	rendererSrc, err := embed_util.NewEmbeddedFilesWithTmpDir(pythonsrc.RendererSource, tmpDir+"-jinja2-renderer", true)
	if err != nil {
		return nil, err
	}

	return &SQLParser{
		ep:          ep,
		sqlglotDir:  sqlglotDir,
		rendererSrc: rendererSrc,
	}, nil
}

func (s *SQLParser) Start() error {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()
	if s.started {
		return nil
	}
	var err error
	args := []string{filepath.Join(s.rendererSrc.GetExtractedPath(), "main.py")}
	s.cmd, err = s.ep.PythonCmd(args...)
	if err != nil {
		return err
	}
	// s.cmd.Stderr = os.Stderr

	s.stdout, err = s.cmd.StdoutPipe()
	if err != nil {
		return err
	}

	s.stdin, err = s.cmd.StdinPipe()
	if err != nil {
		return err
	}

	err = s.cmd.Start()
	if err != nil {
		return err
	}

	_, err = s.sendCommand(&parserCommand{
		Command: "init",
	})
	if err != nil {
		return errors.Wrap(err, "failed to send init command")
	}
	s.started = true
	return nil
}

type parserCommand struct {
	Command  string                 `json:"command"`
	Contents map[string]interface{} `json:"contents"`
}

type Schema map[string]map[string]string

type UpstreamColumn struct {
	Column string `json:"column"`
	Table  string `json:"table"`
}

type ColumnLineage struct {
	Name     string           `json:"name"`
	Upstream []UpstreamColumn `json:"upstream"`
	Type     string           `json:"type"`
}
type Lineage struct {
	Columns            []ColumnLineage `json:"columns"`
	NonSelectedColumns []ColumnLineage `json:"non_selected_columns"`
	Errors             []string        `json:"errors"`
}

func (s *SQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	command := parserCommand{
		Command: "lineage",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
			"schema":  schema,
		},
	}

	resp, err := s.sendCommand(&command)
	if err != nil {
		return nil, err
	}

	var lineage Lineage
	err = json.Unmarshal([]byte(resp), &lineage)
	if err != nil {
		return nil, err
	}

	return &lineage, nil
}

func (s *SQLParser) UsedTables(sql, dialect string) ([]string, error) {
	err := s.Start()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start sql parser")
	}

	command := parserCommand{
		Command: "get-tables",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
		},
	}

	resp, err := s.sendCommand(&command)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send command")
	}

	var tables struct {
		Tables []string `json:"tables"`
		Error  string   `json:"error"`
	}
	err = json.Unmarshal([]byte(resp), &tables)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response")
	}

	if tables.Error != "" {
		return nil, errors.New(tables.Error)
	}

	sort.Strings(tables.Tables)

	return tables.Tables, nil
}

func (s *SQLParser) RenameTables(sql string, dialect string, tableMapping map[string]string) (string, error) {
	err := s.Start()
	if err != nil {
		return "", errors.Wrap(err, "failed to start sql parser")
	}

	command := parserCommand{
		Command: "replace-table-references",
		Contents: map[string]interface{}{
			"query":         sql,
			"dialect":       dialect,
			"table_mapping": tableMapping,
		},
	}

	responsePayload, err := s.sendCommand(&command)
	if err != nil {
		return "", errors.Wrap(err, "failed to send command")
	}

	var resp struct {
		Query string `json:"query"`
		Error string `json:"error"`
	}
	err = json.Unmarshal([]byte(responsePayload), &resp)
	if err != nil {
		return "", errors.Wrap(err, "failed to unmarshal response")
	}

	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}

	return resp.Query, nil
}

func (s *SQLParser) sendCommand(pc *parserCommand) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	jsonCommand, err := json.Marshal(pc)
	if err != nil {
		return "", err
	}

	jsonCommand = append(jsonCommand, '\n')

	_, err = s.stdin.Write(jsonCommand)
	if err != nil {
		return "", errors.Wrap(err, "failed to write command to stdin")
	}

	reader := bufio.NewReader(s.stdout)

	resp, err := reader.ReadString(byte('\n'))
	return resp, err
}

func (s *SQLParser) Close() error {
	if s.stdin != nil {
		_, err := s.sendCommand(&parserCommand{
			Command: "exit",
		})
		if err != nil {
			return errors.Wrap(err, "failed to send exit command")
		}

		_ = s.stdin.Close()
		s.stdin = nil
	}

	if s.stdout != nil {
		_ = s.stdout.Close()
		s.stdout = nil
	}

	if s.cmd != nil {
		if s.cmd.Process != nil {
			timer := time.AfterFunc(5*time.Second, func() {
				_ = s.cmd.Process.Kill()
			})
			_ = s.cmd.Wait()
			timer.Stop()
		}
		s.cmd = nil
	}

	return nil
}

type QueryConfig struct {
	Name   string `json:"name"`
	Query  string `json:"query"`
	Schema Schema `json:"schema"`
}

var assetTypeDialectMap = map[pipeline.AssetType]string{
	pipeline.AssetTypeBigqueryQuery:   "bigquery",
	pipeline.AssetTypeSnowflakeQuery:  "snowflake",
	pipeline.AssetTypePostgresQuery:   "postgres",
	pipeline.AssetTypeRedshiftQuery:   "redshift",
	pipeline.AssetTypeAthenaQuery:     "athena",
	pipeline.AssetTypeClickHouse:      "clickhouse",
	pipeline.AssetTypeDatabricksQuery: "databricks",
	pipeline.AssetTypeMsSQLQuery:      "tsql",
	pipeline.AssetTypeSynapseQuery:    "tsql",
}

func AssetTypeToDialect(assetType pipeline.AssetType) (string, error) {
	dialect, ok := assetTypeDialectMap[assetType]
	if !ok {
		return "", fmt.Errorf("unsupported asset type %s", assetType)
	}
	return dialect, nil
}
