package sqlparser

import (
	"bufio"
	"encoding/json"
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
	"github.com/sourcegraph/conc"
)

type SQLParserPool struct {
	parsers     []*SQLParser
	workerCount int
	mut         sync.Mutex
	counter     int
}

func NewSQLParserPool(workerCount int) (*SQLParserPool, error) {
	sp := &SQLParserPool{
		parsers:     make([]*SQLParser, workerCount),
		workerCount: workerCount,
	}

	var mut sync.Mutex
	var wg conc.WaitGroup
	for i := range workerCount {
		wg.Go(func() {
			p, err := NewSQLParser()
			if err != nil {
				panic(err)
			}
			mut.Lock()
			defer mut.Unlock()
			sp.parsers[i] = p
		})
	}

	panics := wg.WaitAndRecover()
	if panics != nil {
		return nil, panics.AsError()
	}

	return sp, nil
}

func (sp *SQLParserPool) Start() error {
	var wg conc.WaitGroup
	for _, parser := range sp.parsers {
		wg.Go(func() {
			err := parser.Start()
			if err != nil {
				panic(err)
			}
		})
	}

	panics := wg.WaitAndRecover()
	if panics != nil {
		return panics.AsError()
	}

	return nil
}

func (sp *SQLParserPool) Close() error {
	var wg conc.WaitGroup
	for _, parser := range sp.parsers {
		wg.Go(func() {
			err := parser.Close()
			if err != nil {
				panic(err)
			}
		})
	}

	panics := wg.WaitAndRecover()
	if panics != nil {
		return panics.AsError()
	}

	return nil
}

func (sp *SQLParserPool) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	sp.mut.Lock()
	runner := sp.parsers[sp.counter%sp.workerCount]
	sp.counter++
	sp.mut.Unlock()

	return runner.ColumnLineage(sql, dialect, schema)
}

func (sp *SQLParserPool) UsedTables(sql, dialect string) ([]string, error) {
	sp.mut.Lock()
	runner := sp.parsers[sp.counter%sp.workerCount]
	sp.counter++
	sp.mut.Unlock()

	return runner.UsedTables(sql, dialect)
}

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

func NewSQLParser() (*SQLParser, error) {
	tmpDir := filepath.Join(os.TempDir(), "bruin-cli-embedded")

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
	s.cmd.Stderr = os.Stderr

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
	return err

	if err != nil {
		return errors.Wrap(err, "failed to start sql parser after retries")
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
}
type Lineage struct {
	Columns []ColumnLineage `json:"columns"`
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
