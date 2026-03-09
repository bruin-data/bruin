package sqlparser

import (
	"encoding/json"
	"sort"
	"sync"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

type RustSQLParser struct {
	started        bool
	MaxQueryLength int

	mutex      sync.Mutex
	startMutex sync.Mutex
}

func NewRustSQLParser(randomize bool) (*RustSQLParser, error) {
	return NewRustSQLParserWithConfig(randomize, 10000)
}

func NewRustSQLParserWithConfig(_ bool, maxQueryLength int) (*RustSQLParser, error) {
	return &RustSQLParser{
		MaxQueryLength: maxQueryLength,
	}, nil
}

func (s *RustSQLParser) Start() error {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()
	if s.started {
		return nil
	}

	if err := ensureRustSQLParserFFI(); err != nil {
		return err
	}

	if _, err := s.sendCommand(&parserCommand{Command: "init"}); err != nil {
		return err
	}

	s.started = true
	return nil
}

func (s *RustSQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	if len(sql) > s.MaxQueryLength {
		return &Lineage{
			Columns:            []ColumnLineage{},
			NonSelectedColumns: []ColumnLineage{},
			Errors:             []string{"query is too long skipping column lineage analysis"},
		}, nil
	}

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
	if err := json.Unmarshal([]byte(resp), &lineage); err != nil {
		return nil, err
	}

	return &lineage, nil
}

func (s *RustSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	if err := s.Start(); err != nil {
		return nil, errors.Wrap(err, "failed to start rust sql parser")
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
	if err := json.Unmarshal([]byte(resp), &tables); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response")
	}

	if tables.Error != "" {
		return nil, errors.New(tables.Error)
	}

	sort.Strings(tables.Tables)
	return tables.Tables, nil
}

func (s *RustSQLParser) RenameTables(sql string, dialect string, tableMapping map[string]string) (string, error) {
	if err := s.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start rust sql parser")
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
	if err := json.Unmarshal([]byte(responsePayload), &resp); err != nil {
		return "", errors.Wrap(err, "failed to unmarshal response")
	}

	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}

	return resp.Query, nil
}

func (s *RustSQLParser) AddLimit(sql string, limit int, dialect string) (string, error) {
	if err := s.Start(); err != nil {
		return "", errors.Wrap(err, "failed to start rust sql parser")
	}

	command := parserCommand{
		Command: "add-limit",
		Contents: map[string]interface{}{
			"query":   sql,
			"limit":   limit,
			"dialect": dialect,
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
	if err := json.Unmarshal([]byte(responsePayload), &resp); err != nil {
		return "", errors.Wrap(err, "failed to unmarshal response")
	}

	if resp.Error != "" {
		return "", errors.New(resp.Error)
	}

	return resp.Query, nil
}

func (s *RustSQLParser) IsSingleSelectQuery(sql string, dialect string) (bool, error) {
	if err := s.Start(); err != nil {
		return false, errors.Wrap(err, "failed to start rust sql parser")
	}

	command := parserCommand{
		Command: "is-single-select",
		Contents: map[string]interface{}{
			"query":   sql,
			"dialect": dialect,
		},
	}

	responsePayload, err := s.sendCommand(&command)
	if err != nil {
		return false, errors.Wrap(err, "failed to send command")
	}

	var resp struct {
		IsSingleSelect bool   `json:"is_single_select"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal([]byte(responsePayload), &resp); err != nil {
		return false, errors.Wrap(err, "failed to unmarshal response")
	}

	if resp.Error != "" {
		return false, errors.New(resp.Error)
	}

	return resp.IsSingleSelect, nil
}

func (s *RustSQLParser) GetMissingDependenciesForAsset(asset *pipeline.Asset, pipeline *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error) {
	return getMissingDependenciesForAsset(s, asset, pipeline, renderer)
}

func (s *RustSQLParser) Close() error {
	if !s.started {
		return nil
	}

	_, _ = s.sendCommand(&parserCommand{Command: "exit"})
	s.started = false
	return nil
}

func (s *RustSQLParser) sendCommand(pc *parserCommand) (string, error) {
	if !s.started && pc.Command != "init" && pc.Command != "exit" {
		if err := s.Start(); err != nil {
			return "", err
		}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	jsonCommand, err := json.Marshal(pc)
	if err != nil {
		return "", err
	}

	return rustSQLParserFFIExecute(string(jsonCommand))
}
