package sqlparser

import (
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
		readOnlyParser, errReadOnlyParser = NewSQLParser()
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
	var response readOnlyResponse
	if err := run(&response, func() (any, error) { return isReadOnlyQuery(validUTF8(query), validUTF8(dialect)), nil }); err != nil {
		return false, fmt.Errorf("failed to validate read-only query: %w", err)
	}
	if response.Error != "" {
		return false, fmt.Errorf("cannot determine whether query is read-only: %s", response.Error)
	}
	return response.ReadOnly, nil
}
