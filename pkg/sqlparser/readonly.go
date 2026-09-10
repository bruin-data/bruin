package sqlparser

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

var (
	readOnlyParserOnce sync.Once
	readOnlyParser     *SQLParser
	errReadOnlyParser  error
)

func ValidateReadOnlyQuery(query, dialect string) error {
	readOnlyParserOnce.Do(func() {
		readOnlyParser, errReadOnlyParser = NewSQLParserCached()
	})
	if errReadOnlyParser != nil {
		return fmt.Errorf("read-only query validation failed: %w", errReadOnlyParser)
	}
	readOnly, err := readOnlyParser.IsReadOnlyQuery(query, dialect)
	if err != nil {
		return fmt.Errorf("read-only query validation failed: %w", err)
	}
	if !readOnly {
		return errors.New("query is not allowed on a read-only connection")
	}
	return nil
}

func (s *SQLParser) IsReadOnlyQuery(query, dialect string) (bool, error) {
	if err := s.Start(); err != nil {
		return false, fmt.Errorf("failed to start sql parser: %w", err)
	}
	payload, err := s.sendCommand(&parserCommand{
		Command:  "is-read-only",
		Contents: map[string]interface{}{"query": query, "dialect": dialect},
	})
	if err != nil {
		return false, fmt.Errorf("failed to validate read-only query: %w", err)
	}
	return decodeReadOnlyResponse(payload)
}

func decodeReadOnlyResponse(payload string) (bool, error) {
	var response struct {
		ReadOnly bool   `json:"is_read_only"`
		Error    string `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &response); err != nil {
		return false, fmt.Errorf("failed to unmarshal read-only response: %w", err)
	}
	if response.Error != "" {
		return false, fmt.Errorf("cannot determine whether query is read-only: %s", response.Error)
	}
	return response.ReadOnly, nil
}
