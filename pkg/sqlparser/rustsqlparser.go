package sqlparser

import (
	"encoding/json"
	"sort"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

type RustSQLParser struct {
	MaxQueryLength int
}

func NewRustSQLParser(_ bool) (*RustSQLParser, error) {
	return NewRustSQLParserWithConfig(false, 10000)
}

func NewRustSQLParserWithConfig(_ bool, maxQueryLength int) (*RustSQLParser, error) {
	return &RustSQLParser{
		MaxQueryLength: maxQueryLength,
	}, nil
}

func (s *RustSQLParser) Start() error {
	return ensureRustSQLParserFFI()
}

func (s *RustSQLParser) ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error) {
	if len(sql) > s.MaxQueryLength {
		return &Lineage{
			Columns:            []ColumnLineage{},
			NonSelectedColumns: []ColumnLineage{},
			Errors:             []string{"query is too long skipping column lineage analysis"},
		}, nil
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal schema")
	}

	resp, err := rustFFIColumnLineage(sql, dialect, string(schemaJSON))
	if err != nil {
		return nil, err
	}

	var lineage Lineage
	if err := json.Unmarshal([]byte(resp), &lineage); err != nil {
		return nil, err
	}

	return &lineage, nil
}

func (s *RustSQLParser) columnLineageRawJSON(sql, dialect string, schemaJSON string) (string, error) {
	return rustFFIColumnLineage(sql, dialect, schemaJSON)
}

func (s *RustSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	resp, err := rustFFIGetTables(sql, dialect)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get tables")
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
	mappingJSON, err := json.Marshal(tableMapping)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal table mapping")
	}

	responsePayload, err := rustFFIRenameTables(sql, dialect, string(mappingJSON))
	if err != nil {
		return "", errors.Wrap(err, "failed to rename tables")
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
	responsePayload, err := rustFFIAddLimit(sql, limit, dialect)
	if err != nil {
		return "", errors.Wrap(err, "failed to add limit")
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
	responsePayload, err := rustFFIIsSingleSelect(sql, dialect)
	if err != nil {
		return false, errors.Wrap(err, "failed to check single select")
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
	return nil
}
