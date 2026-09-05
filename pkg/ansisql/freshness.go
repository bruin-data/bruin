package ansisql

import (
	"context"
	"fmt"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// FreshnessCheck evaluates the latest timestamp, rather than the age of every row.
type FreshnessCheck struct {
	conn            config.ConnectionGetter
	quoteIdentifier func(string) string
	now             func() time.Time
}

func NewFreshnessCheck(conn config.ConnectionGetter, quoteIdentifier func(string) string) *FreshnessCheck {
	return &FreshnessCheck{conn: conn, quoteIdentifier: quoteIdentifier, now: time.Now}
}

func (c *FreshnessCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return errors.New("freshness check requires a positive duration string, such as '30h'")
	}
	maxAge, err := time.ParseDuration(*ti.Check.Value.String)
	if err != nil || maxAge <= 0 {
		return errors.New("freshness check requires a positive duration string, such as '30h'")
	}
	connectionName, err := ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}
	conn := c.conn.GetConnection(connectionName)
	if conn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}
	s, ok := conn.(selector)
	if !ok {
		return errors.Errorf("connection '%s' cannot be used for the check 'freshness'", connectionName)
	}
	q, err := AddColumnCheckAnnotationComment(ctx, &query.Query{
		Query: fmt.Sprintf("SELECT MAX(%s) FROM %s", c.quoteIdentifier(ti.Column.Name), c.quoteIdentifier(ti.GetAsset().Name)),
	}, ti.GetAsset().Name, ti.Column.Name, "freshness", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}
	ti.ExecutedQuery = q.Query
	result, err := s.Select(ctx, q)
	if err != nil {
		return errors.Wrap(err, "failed 'freshness' check")
	}
	if len(result) != 1 || len(result[0]) != 1 {
		return errors.New("freshness check expected a single MAX(timestamp) result")
	}
	if result[0][0] == nil {
		return &CheckError{
			Query: q.Query, Result: 1, Expected: 0,
			Message: fmt.Sprintf("column '%s' has no timestamp values for freshness check", ti.Column.Name),
		}
	}
	latest, err := freshnessTimestamp(result[0][0])
	if err != nil {
		return errors.Wrap(err, "failed to parse 'freshness' check result")
	}
	age := c.now().Sub(latest)
	if age > maxAge {
		return &CheckError{
			Query: q.Query, Result: 1, Expected: 0,
			Message: fmt.Sprintf("column '%s' latest timestamp is %s (age %s), exceeding maximum age %s", ti.Column.Name, latest.UTC().Format(time.RFC3339Nano), age, maxAge),
		}
	}
	return nil
}

// Drivers return timestamps either natively or as text. Zone-less timestamps
// are interpreted as UTC; numeric values are not guessed to be Unix timestamps.
func freshnessTimestamp(value interface{}) (time.Time, error) {
	var raw string
	switch value := value.(type) {
	case time.Time:
		return value, nil
	case string:
		raw = value
	case []byte:
		raw = string(value)
	default:
		return time.Time{}, errors.Errorf("expected a timestamp, got %T", value)
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05Z0700",
		"2006-01-02 15:04:05Z07",
		"2006-01-02 15:04:05 UTC",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	} {
		if timestamp, err := time.Parse(layout, raw); err == nil {
			return timestamp, nil
		}
	}
	return time.Time{}, errors.New("expected a timestamp in RFC3339 or SQL timestamp format")
}
