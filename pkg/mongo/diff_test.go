package mongo

import (
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSplitCollectionName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identifier string
		defaultDB  string
		wantDB     string
		wantColl   string
		wantErr    bool
	}{
		{name: "collection only with default db", identifier: "users", defaultDB: "shop", wantDB: "shop", wantColl: "users"},
		{name: "qualified database.collection", identifier: "analytics.events", defaultDB: "shop", wantDB: "analytics", wantColl: "events"},
		{name: "dotted collection name keeps trailing dots", identifier: "shop.orders.2024", defaultDB: "ignored", wantDB: "shop", wantColl: "orders.2024"},
		{name: "trims surrounding whitespace", identifier: "  users  ", defaultDB: "shop", wantDB: "shop", wantColl: "users"},
		{name: "no default db and unqualified", identifier: "users", defaultDB: "", wantErr: true},
		{name: "empty identifier", identifier: "", defaultDB: "shop", wantErr: true},
		{name: "leading dot", identifier: ".users", defaultDB: "shop", wantErr: true},
		{name: "trailing dot", identifier: "shop.", defaultDB: "shop", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotDB, gotColl, err := splitCollectionName(tt.identifier, tt.defaultDB)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDB, gotDB)
			assert.Equal(t, tt.wantColl, gotColl)
		})
	}
}

func TestDominantType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		counts map[string]int64
		want   string
	}{
		{name: "single type", counts: map[string]int64{"string": 10}, want: "string"},
		{name: "ignores null when other types exist", counts: map[string]int64{"int": 5, "null": 100}, want: "int"},
		{name: "exclusively null", counts: map[string]int64{"null": 7}, want: "null"},
		{name: "picks the most common", counts: map[string]int64{"int": 3, "long": 9, "double": 1}, want: "long"},
		{name: "tie breaks on name", counts: map[string]int64{"long": 4, "int": 4}, want: "int"},
		{name: "empty", counts: map[string]int64{}, want: "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dominantType(tt.counts))
		})
	}
}

func TestSummarizeSchema(t *testing.T) {
	t.Parallel()

	facet := schemaFacetResult{
		Total: []countResult{{N: 100}},
		Fields: []schemaFacetField{
			{ID: schemaFieldKey{Field: "_id", Type: "objectId"}, Count: 100},
			{ID: schemaFieldKey{Field: "name", Type: "string"}, Count: 100},
			{ID: schemaFieldKey{Field: "age", Type: "int"}, Count: 80},
			{ID: schemaFieldKey{Field: "age", Type: "null"}, Count: 5},
			{ID: schemaFieldKey{Field: "active", Type: "bool"}, Count: 100},
			{ID: schemaFieldKey{Field: "created", Type: "date"}, Count: 100},
			{ID: schemaFieldKey{Field: "address", Type: "object"}, Count: 100},
			{ID: schemaFieldKey{Field: "score", Type: "double"}, Count: 50},
			{ID: schemaFieldKey{Field: "score", Type: "int"}, Count: 30},
		},
	}

	scanned, columns := summarizeSchema(facet)
	assert.Equal(t, int64(100), scanned)

	// `_id` must be first; the remainder alphabetical.
	gotOrder := make([]string, len(columns))
	for i, c := range columns {
		gotOrder[i] = c.Name
	}
	assert.Equal(t, []string{"_id", "active", "address", "age", "created", "name", "score"}, gotOrder)

	byName := map[string]*diff.Column{}
	for _, c := range columns {
		byName[c.Name] = c
	}

	// _id present in every doc -> not nullable, stringified type.
	assert.Equal(t, "objectId", byName["_id"].Type)
	assert.Equal(t, diff.CommonTypeString, byName["_id"].NormalizedType)
	assert.False(t, byName["_id"].Nullable)

	// age present in 80 docs (non-null) of 100 -> nullable, dominant type int.
	assert.Equal(t, "int", byName["age"].Type)
	assert.Equal(t, diff.CommonTypeNumeric, byName["age"].NormalizedType)
	assert.True(t, byName["age"].Nullable)

	// score is polymorphic (double:50, int:30) and missing in 20 docs.
	assert.Equal(t, "double", byName["score"].Type)
	assert.Equal(t, diff.CommonTypeNumeric, byName["score"].NormalizedType)
	assert.True(t, byName["score"].Nullable)

	assert.Equal(t, diff.CommonTypeBoolean, byName["active"].NormalizedType)
	assert.Equal(t, diff.CommonTypeDateTime, byName["created"].NormalizedType)
	assert.Equal(t, diff.CommonTypeJSON, byName["address"].NormalizedType)
}

func TestSummarizeSchemaEmpty(t *testing.T) {
	t.Parallel()

	scanned, columns := summarizeSchema(schemaFacetResult{})
	assert.Equal(t, int64(0), scanned)
	assert.Empty(t, columns)
}

func TestMongoTypeMapper(t *testing.T) {
	t.Parallel()

	mapper := diff.NewMongoTypeMapper()
	cases := map[string]diff.CommonDataType{
		"double":    diff.CommonTypeNumeric,
		"int":       diff.CommonTypeNumeric,
		"long":      diff.CommonTypeNumeric,
		"decimal":   diff.CommonTypeNumeric,
		"string":    diff.CommonTypeString,
		"objectId":  diff.CommonTypeString,
		"bool":      diff.CommonTypeBoolean,
		"date":      diff.CommonTypeDateTime,
		"timestamp": diff.CommonTypeDateTime,
		"object":    diff.CommonTypeJSON,
		"array":     diff.CommonTypeJSON,
		"binData":   diff.CommonTypeBinary,
		"regex":     diff.CommonTypeUnknown,
		"null":      diff.CommonTypeUnknown,
	}
	for bsonType, want := range cases {
		assert.Equalf(t, want, mapper.MapType(bsonType), "mapping for %q", bsonType)
	}
}

func TestParseStatsResult(t *testing.T) {
	t.Parallel()

	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	columns := []*diff.Column{
		{Name: "_id", NormalizedType: diff.CommonTypeString},
		{Name: "age", NormalizedType: diff.CommonTypeNumeric},
		{Name: "active", NormalizedType: diff.CommonTypeBoolean},
		{Name: "created", NormalizedType: diff.CommonTypeDateTime},
		{Name: "address", NormalizedType: diff.CommonTypeJSON},
		{Name: "blob", NormalizedType: diff.CommonTypeBinary},
	}

	doc := map[string]interface{}{
		// Scalar accumulators live in the "stats" $facet branch, keyed by index.
		"stats": bson.A{bson.M{
			// _id (string) at index 0
			"0_present": int32(100),
			"0_empty":   int32(0),
			"0_minlen":  int32(24),
			"0_maxlen":  int32(24),
			"0_avglen":  float64(24),
			// age (numeric) at index 1
			"1_present": int64(95),
			"1_min":     int32(18),
			"1_max":     int32(80),
			"1_avg":     float64(42.5),
			"1_sum":     int64(4037),
			"1_std":     float64(10.1),
			// active (boolean) at index 2
			"2_present": int32(100),
			"2_true":    int32(60),
			"2_false":   int32(40),
			// created (datetime) at index 3
			"3_present": int32(100),
			"3_min":     primitive.NewDateTimeFromTime(created),
			"3_max":     primitive.NewDateTimeFromTime(created.Add(48 * time.Hour)),
			// address (json) at index 4
			"4_present": int32(90),
			// blob (binary) at index 5
			"5_present": int32(10),
		}},
		// Distinct counts each have their own [..., {$count: "n"}] branch.
		"0_distinct": bson.A{bson.M{"n": int32(3)}},
		"3_distinct": bson.A{bson.M{"n": int32(2)}},
	}

	parseStatsResult(doc, columns, 100)

	idStats, ok := columns[0].Stats.(*diff.StringStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(100), idStats.Count)
	assert.Equal(t, int64(0), idStats.NullCount)
	assert.Equal(t, int64(3), idStats.DistinctCount) // nil excluded
	assert.Equal(t, 24, idStats.MinLength)
	assert.Equal(t, 24, idStats.MaxLength)

	ageStats, ok := columns[1].Stats.(*diff.NumericalStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(100), ageStats.Count)
	assert.Equal(t, int64(5), ageStats.NullCount)
	require.NotNil(t, ageStats.Min)
	assert.InDelta(t, 18, *ageStats.Min, 0.001)
	require.NotNil(t, ageStats.Avg)
	assert.InDelta(t, 42.5, *ageStats.Avg, 0.001)

	activeStats, ok := columns[2].Stats.(*diff.BooleanStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(60), activeStats.TrueCount)
	assert.Equal(t, int64(40), activeStats.FalseCount)

	dtStats, ok := columns[3].Stats.(*diff.DateTimeStatistics)
	require.True(t, ok)
	require.NotNil(t, dtStats.EarliestDate)
	assert.Equal(t, created.UTC(), dtStats.EarliestDate.UTC())
	assert.Equal(t, int64(2), dtStats.UniqueCount)

	jsonStats, ok := columns[4].Stats.(*diff.JSONStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(100), jsonStats.Count)
	assert.Equal(t, int64(10), jsonStats.NullCount)

	_, ok = columns[5].Stats.(*diff.UnknownStatistics)
	require.True(t, ok)
}

func TestParseStatsResultNullNumeric(t *testing.T) {
	t.Parallel()

	// A numeric field that is null/missing in every document yields nil pointers.
	columns := []*diff.Column{{Name: "x", NormalizedType: diff.CommonTypeNumeric}}
	doc := map[string]interface{}{"stats": bson.A{bson.M{"0_present": int32(0)}}}

	parseStatsResult(doc, columns, 50)

	stats, ok := columns[0].Stats.(*diff.NumericalStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(50), stats.Count)
	assert.Equal(t, int64(50), stats.NullCount)
	assert.Nil(t, stats.Min)
	assert.Nil(t, stats.Avg)
}

func TestGetTimePtr(t *testing.T) {
	t.Parallel()

	when := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)

	// BSON date.
	got := getTimePtr(primitive.NewDateTimeFromTime(when))
	require.NotNil(t, got)
	assert.Equal(t, when, got.UTC())

	// BSON timestamp: seconds since the epoch are carried in T.
	ts := getTimePtr(primitive.Timestamp{T: uint32(when.Unix()), I: 1})
	require.NotNil(t, ts)
	assert.Equal(t, when, ts.UTC())

	// Unrelated types yield nil.
	assert.Nil(t, getTimePtr("not a time"))
	assert.Nil(t, getTimePtr(nil))
}

func TestFacetCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(7), facetCount(bson.A{bson.M{"n": int32(7)}}))
	// An empty branch (no matching documents) means zero distinct values.
	assert.Equal(t, int64(0), facetCount(bson.A{}))
	assert.Equal(t, int64(0), facetCount(nil))
}

func TestDistinctExpr(t *testing.T) {
	t.Parallel()

	_, ok := distinctExpr(&diff.Column{Name: "name", NormalizedType: diff.CommonTypeString})
	assert.True(t, ok)
	_, ok = distinctExpr(&diff.Column{Name: "created", NormalizedType: diff.CommonTypeDateTime})
	assert.True(t, ok)
	// Numeric, boolean and json columns do not carry a distinct count.
	_, ok = distinctExpr(&diff.Column{Name: "age", NormalizedType: diff.CommonTypeNumeric})
	assert.False(t, ok)
}

func TestBuildSchemaPipeline(t *testing.T) {
	t.Parallel()

	// Without sampling: just the $facet stage.
	pipeline := buildSchemaPipeline(0)
	require.Len(t, pipeline, 1)

	// With sampling: a leading $sample stage.
	sampled := buildSchemaPipeline(500)
	require.Len(t, sampled, 2)
	stage := sampled[0].(bson.D)
	assert.Equal(t, "$sample", stage[0].Key)
}

func TestBuildStatsPipeline(t *testing.T) {
	t.Parallel()

	columns := []*diff.Column{
		{Name: "age", NormalizedType: diff.CommonTypeNumeric},
		{Name: "name", NormalizedType: diff.CommonTypeString},
	}

	pipeline := buildStatsPipeline(columns, 0)
	require.Len(t, pipeline, 1)
	facet := pipeline[0].(bson.D)
	assert.Equal(t, "$facet", facet[0].Key)

	// The facet carries the scalar "stats" branch plus one distinct-count branch
	// for the string column (index 1); the numeric column has no distinct branch.
	branches := facet[0].Value.(bson.D)
	keys := make([]string, len(branches))
	for i, b := range branches {
		keys[i] = b.Key
	}
	assert.Equal(t, []string{"stats", "1_distinct"}, keys)

	// The distinct branch ends in a $count stage so it returns only a number.
	distinct := branches[1].Value.(bson.A)
	last := distinct[len(distinct)-1].(bson.D)
	assert.Equal(t, "$count", last[0].Key)

	// With sampling: leading $sample stage.
	sampled := buildStatsPipeline(columns, 100)
	require.Len(t, sampled, 2)
	assert.Equal(t, "$sample", sampled[0].(bson.D)[0].Key)
	assert.Equal(t, "$facet", sampled[1].(bson.D)[0].Key)
}
