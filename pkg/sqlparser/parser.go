package sqlparser

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/bruin-data/bruin/internal/data"
	"github.com/bruin-data/bruin/python_src"
	"github.com/kluctl/go-embed-python/embed_util"
	"github.com/kluctl/go-embed-python/python"
	"github.com/pkg/errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type SqlParser struct {
	ep          *python.EmbeddedPython
	extractDir  string
	sqlglotDir  *embed_util.EmbeddedFiles
	rendererSrc *embed_util.EmbeddedFiles

	stdout io.ReadCloser
	stdin  io.WriteCloser
	cmd    *exec.Cmd
	mutex  sync.Mutex
}

func NewSqlParser() (*SqlParser, error) {
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

	rendererSrc, err := embed_util.NewEmbeddedFilesWithTmpDir(python_src.RendererSource, tmpDir+"-jinja2-renderer", true)
	if err != nil {
		return nil, err
	}

	return &SqlParser{
		ep:          ep,
		sqlglotDir:  sqlglotDir,
		rendererSrc: rendererSrc,
	}, nil
}

func (s *SqlParser) Start() error {
	args := []string{filepath.Join(s.rendererSrc.GetExtractedPath(), "main.py")}
	cmd := s.ep.PythonCmd(args...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	s.stdin = stdin
	s.stdout = stdout
	s.cmd = cmd

	_, err = s.sendCommand(&parserCommand{
		Command: "init",
	})
	if err != nil {
		return errors.Wrap(err, "failed to send init command")
	}

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

func (s *SqlParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
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

func (s *SqlParser) sendCommand(pc *parserCommand) (string, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	jsonCommand, err := json.Marshal(pc)
	if err != nil {
		return "", err
	}

	jsonCommand = append(jsonCommand, '\n')

	_, err = s.stdin.Write(jsonCommand)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(s.stdout)

	line := bytes.NewBuffer(nil)
	for true {
		l, p, err := reader.ReadLine()
		if err != nil {
			return "", err
		}
		line.Write(l)
		if !p {
			break
		}
	}

	return line.String(), nil
}

func (s *SqlParser) Close() error {
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
