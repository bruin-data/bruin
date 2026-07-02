package unittest_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/unittest"
	"github.com/stretchr/testify/require"
)

func TestResolveFixtures(t *testing.T) {
	t.Parallel()

	orders := pipeline.Fixture{
		Name:  "base_orders",
		Asset: "analytics.orders",
		Rows:  []map[string]interface{}{{"id": 1, "status": "paid"}},
	}
	currency := pipeline.Fixture{
		Name:  "base_currency",
		Asset: "analytics.currency",
		Rows:  []map[string]interface{}{{"code": "USD", "rate": 1.0}},
	}
	available := []pipeline.Fixture{orders, currency}

	t.Run("no referenced fixtures returns the inputs unchanged", func(t *testing.T) {
		t.Parallel()
		test := pipeline.UnitTest{Inputs: []pipeline.UnitTestInput{{Asset: "x"}}}
		got, err := unittest.ResolveFixtures(available, test)
		require.NoError(t, err)
		require.Equal(t, test.Inputs, got)
	})

	t.Run("a referenced fixture becomes an input", func(t *testing.T) {
		t.Parallel()
		got, err := unittest.ResolveFixtures(available, pipeline.UnitTest{Fixtures: []string{"base_currency"}})
		require.NoError(t, err)
		require.Equal(t, []pipeline.UnitTestInput{{Asset: "analytics.currency", Rows: currency.Rows}}, got)
	})

	t.Run("a fixture and an explicit input for different assets both apply", func(t *testing.T) {
		t.Parallel()
		test := pipeline.UnitTest{
			Inputs:   []pipeline.UnitTestInput{{Asset: "analytics.orders", Rows: []map[string]interface{}{{"id": 9}}}},
			Fixtures: []string{"base_currency"},
		}
		got, err := unittest.ResolveFixtures(available, test)
		require.NoError(t, err)
		require.Len(t, got, 2)
		require.Equal(t, "analytics.orders", got[0].Asset)
		require.Equal(t, "analytics.currency", got[1].Asset)
	})

	t.Run("an explicit input overrides a fixture for the same asset", func(t *testing.T) {
		t.Parallel()
		ownRows := []map[string]interface{}{{"id": 42, "status": "refunded"}}
		test := pipeline.UnitTest{
			Inputs:   []pipeline.UnitTestInput{{Asset: "analytics.orders", Rows: ownRows}},
			Fixtures: []string{"base_orders"},
		}
		got, err := unittest.ResolveFixtures(available, test)
		require.NoError(t, err)
		require.Len(t, got, 1, "the fixture must not add a second input for the same asset")
		require.Equal(t, ownRows, got[0].Rows)
	})

	t.Run("referencing an undefined fixture is an error", func(t *testing.T) {
		t.Parallel()
		_, err := unittest.ResolveFixtures(available, pipeline.UnitTest{Name: "t", Fixtures: []string{"nope"}})
		require.ErrorContains(t, err, "nope")
	})

	t.Run("two fixtures for the same asset is an ambiguous error", func(t *testing.T) {
		t.Parallel()
		dup := pipeline.Fixture{Name: "more_orders", Asset: "analytics.orders", Rows: []map[string]interface{}{{"id": 2}}}
		_, err := unittest.ResolveFixtures(
			[]pipeline.Fixture{orders, dup},
			pipeline.UnitTest{Fixtures: []string{"base_orders", "more_orders"}},
		)
		require.ErrorContains(t, err, "analytics.orders")
	})
}
