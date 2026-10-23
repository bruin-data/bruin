package sqlparser

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

// ValidateReadOnlyQueryWithBlockedFunctions allows function calls except the blocked names, matched
// case-insensitively without schema; a trailing "*" matches a prefix. Use it only where the
// database also enforces read-only transactions, since unlisted functions are not inspected.
func ValidateReadOnlyQueryWithBlockedFunctions(query, dialect string, blocked []string) error {
	readOnlyParserOnce.Do(func() {
		readOnlyParser, errReadOnlyParser = NewSQLParserCached()
	})
	if errReadOnlyParser != nil {
		return fmt.Errorf("read-only query validation failed: %w", errReadOnlyParser)
	}
	readOnly, err := readOnlyParser.isReadOnlyQueryWithBlockedFunctions(query, dialect, blocked)
	if err != nil {
		return fmt.Errorf("read-only query validation failed: %w", err)
	}
	if !readOnly {
		return errors.New("query is not allowed on a read-only connection")
	}
	return nil
}

func (s *SQLParser) isReadOnlyQueryWithBlockedFunctions(query, dialect string, blocked []string) (bool, error) {
	if err := s.Start(); err != nil {
		return false, fmt.Errorf("failed to start sql parser: %w", err)
	}
	if !validUTF8(query, dialect) {
		return false, nil
	}
	list := make(blockedFunctions, len(blocked))
	for i, name := range blocked {
		list[i] = strings.ToLower(name)
	}
	var response readOnlyResponse
	if err := s.callDirect(&response, func() (any, error) { return checkReadOnlyQuery(query, dialect, list), nil }); err != nil {
		return false, fmt.Errorf("failed to unmarshal read-only response: %w", err)
	}
	return checkReadOnlyResponse(response)
}

func (s *SQLParser) IsReadOnlyQuery(query, dialect string) (bool, error) {
	if err := s.Start(); err != nil {
		return false, fmt.Errorf("failed to start sql parser: %w", err)
	}
	var response readOnlyResponse
	sendErr, decodeErr := s.call("is-read-only", map[string]interface{}{"query": query, "dialect": dialect}, &response)
	if sendErr != nil {
		return false, fmt.Errorf("failed to validate read-only query: %w", sendErr)
	}
	if decodeErr != nil {
		return false, fmt.Errorf("failed to unmarshal read-only response: %w", decodeErr)
	}
	return checkReadOnlyResponse(response)
}

func decodeReadOnlyResponse(payload string) (bool, error) {
	var response readOnlyResponse
	if err := json.Unmarshal([]byte(payload), &response); err != nil {
		return false, fmt.Errorf("failed to unmarshal read-only response: %w", err)
	}
	return checkReadOnlyResponse(response)
}

func checkReadOnlyResponse(response readOnlyResponse) (bool, error) {
	if response.Error != "" {
		return false, fmt.Errorf("cannot determine whether query is read-only: %s", response.Error)
	}
	return response.ReadOnly, nil
}
