package mongo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestParseMongoQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		wantErr string
		assert  func(t *testing.T, mq *mongoQuery)
	}{
		{
			name:  "simple find",
			query: `{"collection":"users","filter":{"age":{"$gt":21}}}`,
			assert: func(t *testing.T, mq *mongoQuery) {
				t.Helper()
				assert.Equal(t, "users", mq.Collection)
				assert.JSONEq(t, `{"age":{"$gt":21}}`, string(mq.Filter))
				assert.Nil(t, mq.Aggregate)
			},
		},
		{
			name:  "aggregate pipeline",
			query: `{"collection":"orders","aggregate":[{"$group":{"_id":"$status","n":{"$sum":1}}}]}`,
			assert: func(t *testing.T, mq *mongoQuery) {
				t.Helper()
				assert.Equal(t, "orders", mq.Collection)
				assert.JSONEq(t, `[{"$group":{"_id":"$status","n":{"$sum":1}}}]`, string(mq.Aggregate))
			},
		},
		{
			name:    "missing collection",
			query:   `{"filter":{"a":1}}`,
			wantErr: `must specify a "collection"`,
		},
		{
			name:    "aggregate combined with filter",
			query:   `{"collection":"c","aggregate":[{"$match":{}}],"filter":{"a":1}}`,
			wantErr: `"aggregate" cannot be combined`,
		},
		{
			name:    "unknown field",
			query:   `{"collection":"c","limitt":5}`,
			wantErr: "invalid MongoDB query",
		},
		{
			name:    "not json",
			query:   `SELECT * FROM users`,
			wantErr: "invalid MongoDB query",
		},
		{
			name:    "empty",
			query:   "   ",
			wantErr: "empty query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mq, err := parseMongoQuery(tt.query)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.assert != nil {
				tt.assert(t, mq)
			}
		})
	}
}

func TestParseMongoQueryStripsAnnotationComment(t *testing.T) {
	t.Parallel()

	// The query command prepends a `-- @bruin.config: ...` annotation comment.
	q := "-- @bruin.config: {\"type\":\"adhoc_query\"}\n{\"collection\":\"users\",\"filter\":{\"age\":{\"$gt\":21}}}"
	mq, err := parseMongoQuery(q)
	require.NoError(t, err)
	assert.Equal(t, "users", mq.Collection)
	assert.JSONEq(t, `{"age":{"$gt":21}}`, string(mq.Filter))
}

func TestLimit(t *testing.T) {
	t.Parallel()

	t.Run("injects limit and preserves filter", func(t *testing.T) {
		t.Parallel()
		out := (&DB{}).Limit(`{"collection":"users","filter":{"age":{"$gt":21}}}`, 5)
		mq, err := parseMongoQuery(out)
		require.NoError(t, err)
		require.NotNil(t, mq.Limit)
		assert.Equal(t, int64(5), *mq.Limit)
		assert.JSONEq(t, `{"age":{"$gt":21}}`, string(mq.Filter))
	})

	t.Run("unparseable query is returned unchanged", func(t *testing.T) {
		t.Parallel()
		in := `SELECT 1`
		assert.Equal(t, in, (&DB{}).Limit(in, 5))
	})
}

func TestBuildResult(t *testing.T) {
	t.Parallel()

	oid := primitive.NewObjectID()
	docs := []bson.D{
		{{Key: "_id", Value: oid}, {Key: "name", Value: "alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "_id", Value: primitive.NewObjectID()}, {Key: "age", Value: int32(25)}, {Key: "tags", Value: bson.A{"x", "y"}}},
	}

	res := buildResult(docs)

	// Columns are the union of keys in first-seen order.
	assert.Equal(t, []string{"_id", "name", "age", "tags"}, res.Columns)
	require.Len(t, res.ColumnTypes, len(res.Columns))
	assert.Equal(t, []string{"objectId", "string", "int", "array"}, res.ColumnTypes)

	require.Len(t, res.Rows, 2)
	// First row: all but tags populated.
	assert.Equal(t, oid.Hex(), res.Rows[0][0])
	assert.Equal(t, "alice", res.Rows[0][1])
	assert.Equal(t, int32(30), res.Rows[0][2])
	assert.Nil(t, res.Rows[0][3])
	// Second row: name missing -> nil, tags rendered as JSON.
	assert.Nil(t, res.Rows[1][1])
	assert.Equal(t, `["x","y"]`, res.Rows[1][3])
}

func TestFormatMongoValue(t *testing.T) {
	t.Parallel()

	oid := primitive.NewObjectID()
	ts := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

	assert.Nil(t, formatMongoValue(nil))
	assert.Equal(t, oid.Hex(), formatMongoValue(oid))
	assert.Equal(t, ts, formatMongoValue(primitive.NewDateTimeFromTime(ts)))
	assert.Equal(t, "alice", formatMongoValue("alice"))
	assert.Equal(t, int64(7), formatMongoValue(int64(7)))
	assert.Equal(t, true, formatMongoValue(true))
	// Nested document and array become Extended JSON strings.
	assert.Equal(t, `{"c":2}`, formatMongoValue(bson.M{"c": int32(2)}))
	assert.Equal(t, `[1,2]`, formatMongoValue(bson.A{int32(1), int32(2)}))
}
