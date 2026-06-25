package mongo

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoTypeMapper maps BSON type aliases (as reported by the `$type` aggregation
// operator) to Bruin's normalized data types.
var mongoTypeMapper = diff.NewMongoTypeMapper()

// BSON type groupings used both for type inference and for coercing values when
// computing statistics. They mirror the categories in NewMongoTypeMapper.
var (
	numericBSONTypes = []string{"double", "int", "long", "decimal"}
	stringBSONTypes  = []string{"string", "objectId"}
)

// GetTableSummary implements diff.TableSummarizer for MongoDB collections. The
// table identifier is a collection name, optionally qualified as
// "database.collection".
func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	if err := db.initClient(ctx); err != nil {
		return nil, err
	}
	return BuildTableSummary(ctx, db.client, db.config.Database, tableName, schemaOnly, diff.SampleSizeFromContext(ctx))
}

// BuildTableSummary produces a diff.TableSummaryResult for a MongoDB collection.
// MongoDB has no static schema, so the field set, types and nullability are
// inferred by scanning the documents. When sampleSize > 0 at most that many
// documents are sampled (via $sample) instead of scanning the whole collection,
// which makes the resulting statistics approximate.
//
// It is shared by the mongo and mongo_atlas connection types.
func BuildTableSummary(ctx context.Context, client *mongo.Client, defaultDatabase, tableName string, schemaOnly bool, sampleSize int64) (*diff.TableSummaryResult, error) {
	dbName, collName, err := splitCollectionName(tableName, defaultDatabase)
	if err != nil {
		return nil, err
	}
	coll := client.Database(dbName).Collection(collName)

	// Pass 1: infer the schema (field names, types, nullability) from documents.
	facet, err := runSchemaFacet(ctx, coll, sampleSize)
	if err != nil {
		return nil, err
	}
	scanned, columns := summarizeSchema(facet)

	result := &diff.TableSummaryResult{
		Table: &diff.Table{
			Name:    tableName,
			Columns: columns,
		},
	}

	// In schema-only mode, mirror the SQL summarizers: no row count, no stats.
	if schemaOnly {
		return result, nil
	}

	// Row count reflects the true collection size, independent of any sampling.
	rowCount, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to count documents in collection '%s'", collName)
	}
	result.RowCount = rowCount

	if len(columns) == 0 {
		return result, nil
	}

	// Pass 2: compute per-field statistics in a single aggregation.
	statsDoc, err := runStats(ctx, coll, columns, sampleSize)
	if err != nil {
		return nil, err
	}
	parseStatsResult(statsDoc, columns, scanned)

	return result, nil
}

// splitCollectionName resolves a data-diff table identifier into a database and
// collection. The identifier may be "collection" (uses the connection's default
// database) or "database.collection". MongoDB collection names may themselves
// contain dots, so only the first dot is treated as the database separator.
func splitCollectionName(identifier, defaultDatabase string) (string, string, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return "", "", errors.New("empty collection name")
	}
	if idx := strings.Index(identifier, "."); idx != -1 {
		dbName := identifier[:idx]
		collName := identifier[idx+1:]
		if dbName == "" || collName == "" {
			return "", "", fmt.Errorf("invalid collection identifier %q; expected 'collection' or 'database.collection'", identifier)
		}
		return dbName, collName, nil
	}
	if defaultDatabase == "" {
		return "", "", fmt.Errorf("no database configured for this connection; qualify the collection as 'database.collection' (got %q)", identifier)
	}
	return defaultDatabase, identifier, nil
}

// ---- schema inference ------------------------------------------------------

type schemaFieldKey struct {
	Field string `bson:"f"`
	Type  string `bson:"t"`
}

type schemaFacetField struct {
	ID    schemaFieldKey `bson:"_id"`
	Count int64          `bson:"n"`
}

type countResult struct {
	N int64 `bson:"n"`
}

// schemaFacetResult is the shape returned by the schema-inference aggregation: a
// total document count plus per (field, BSON type) counts.
type schemaFacetResult struct {
	Total  []countResult      `bson:"total"`
	Fields []schemaFacetField `bson:"fields"`
}

// buildSchemaPipeline builds the aggregation that, for every top-level field,
// reports how many documents carry it with each BSON type. A $facet is used so
// the total document count (needed for nullability) comes from the same scan.
func buildSchemaPipeline(sampleSize int64) bson.A {
	pipeline := bson.A{}
	if sampleSize > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}})
	}
	pipeline = append(pipeline, bson.D{{Key: "$facet", Value: bson.D{
		{Key: "total", Value: bson.A{bson.D{{Key: "$count", Value: "n"}}}},
		{Key: "fields", Value: bson.A{
			bson.D{{Key: "$project", Value: bson.D{{Key: "kv", Value: bson.D{{Key: "$objectToArray", Value: "$$ROOT"}}}}}},
			bson.D{{Key: "$unwind", Value: "$kv"}},
			bson.D{{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "f", Value: "$kv.k"},
					{Key: "t", Value: bson.D{{Key: "$type", Value: "$kv.v"}}},
				}},
				{Key: "n", Value: bson.D{{Key: "$sum", Value: 1}}},
			}}},
		}},
	}}})
	return pipeline
}

func runSchemaFacet(ctx context.Context, coll *mongo.Collection, sampleSize int64) (schemaFacetResult, error) {
	pipeline := buildSchemaPipeline(sampleSize)
	cursor, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return schemaFacetResult{}, errors.Wrap(err, "failed to run schema inference aggregation")
	}
	defer cursor.Close(ctx)

	var results []schemaFacetResult
	if err := cursor.All(ctx, &results); err != nil {
		return schemaFacetResult{}, errors.Wrap(err, "failed to decode schema inference result")
	}
	if len(results) == 0 {
		return schemaFacetResult{}, nil
	}
	return results[0], nil
}

// summarizeSchema turns the raw facet counts into ordered columns. Columns are
// emitted with `_id` first, then alphabetically, for deterministic output. The
// returned count is the number of documents scanned (used for nullability and as
// the basis for statistics counts).
func summarizeSchema(facet schemaFacetResult) (int64, []*diff.Column) {
	var scanned int64
	if len(facet.Total) > 0 {
		scanned = facet.Total[0].N
	}

	typeCounts := make(map[string]map[string]int64)
	order := make([]string, 0)
	for _, f := range facet.Fields {
		field := f.ID.Field
		if _, ok := typeCounts[field]; !ok {
			typeCounts[field] = make(map[string]int64)
			order = append(order, field)
		}
		typeCounts[field][f.ID.Type] += f.Count
	}

	sort.Slice(order, func(i, j int) bool {
		switch {
		case order[i] == "_id":
			return true
		case order[j] == "_id":
			return false
		default:
			return order[i] < order[j]
		}
	})

	columns := make([]*diff.Column, 0, len(order))
	for _, field := range order {
		counts := typeCounts[field]

		var presentNonNull int64
		for t, c := range counts {
			if t != "null" {
				presentNonNull += c
			}
		}

		bsonType := dominantType(counts)
		columns = append(columns, &diff.Column{
			Name:           field,
			Type:           bsonType,
			NormalizedType: mongoTypeMapper.MapType(bsonType),
			// A field is nullable if it is absent or explicitly null in any
			// scanned document.
			Nullable: presentNonNull < scanned,
		})
	}
	return scanned, columns
}

// dominantType returns the most frequently observed BSON type for a field,
// ignoring "null" unless the field is exclusively null. Ties break on type name
// for determinism.
func dominantType(counts map[string]int64) string {
	best := ""
	var bestCount int64 = -1
	for t, c := range counts {
		if t == "null" {
			continue
		}
		if c > bestCount || (c == bestCount && t < best) {
			best = t
			bestCount = c
		}
	}
	if best == "" {
		return "null"
	}
	return best
}

// ---- statistics ------------------------------------------------------------

// buildStatsPipeline builds a single $group that computes, for every column, the
// statistics appropriate to its inferred type. Each column's accumulators are
// keyed by its positional index to avoid any clash with user field names.
func buildStatsPipeline(columns []*diff.Column, sampleSize int64) bson.A {
	group := make(bson.D, 0, 1+len(columns)*6)
	group = append(group, bson.E{Key: "_id", Value: nil})
	for i, col := range columns {
		group = append(group, statAccumulators(i, col)...)
	}

	pipeline := bson.A{}
	if sampleSize > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$sample", Value: bson.D{{Key: "size", Value: sampleSize}}}})
	}
	pipeline = append(pipeline, bson.D{{Key: "$group", Value: group}})
	return pipeline
}

func statAccumulators(i int, col *diff.Column) []bson.E {
	p := strconv.Itoa(i)
	fieldRef := "$" + col.Name
	present := bson.E{Key: p + "_present", Value: sum(presentExpr(fieldRef))}

	switch col.NormalizedType {
	case diff.CommonTypeNumeric:
		num := coerce(fieldRef, numericBSONTypes)
		return []bson.E{
			present,
			{Key: p + "_min", Value: accumulator("$min", num)},
			{Key: p + "_max", Value: accumulator("$max", num)},
			{Key: p + "_avg", Value: accumulator("$avg", num)},
			{Key: p + "_sum", Value: accumulator("$sum", num)},
			{Key: p + "_std", Value: accumulator("$stdDevPop", num)},
		}
	case diff.CommonTypeString:
		str := stringCoerce(fieldRef)
		strLen := bson.D{{Key: "$cond", Value: bson.A{
			bson.D{{Key: "$eq", Value: bson.A{str, nil}}},
			nil,
			bson.D{{Key: "$strLenCP", Value: str}},
		}}}
		return []bson.E{
			present,
			{Key: p + "_distinct", Value: accumulator("$addToSet", str)},
			{Key: p + "_empty", Value: sum(ifThen(eq(str, ""), 1, 0))},
			{Key: p + "_minlen", Value: accumulator("$min", strLen)},
			{Key: p + "_maxlen", Value: accumulator("$max", strLen)},
			{Key: p + "_avglen", Value: accumulator("$avg", strLen)},
		}
	case diff.CommonTypeBoolean:
		return []bson.E{
			present,
			{Key: p + "_true", Value: sum(ifThen(eq(fieldRef, true), 1, 0))},
			{Key: p + "_false", Value: sum(ifThen(eq(fieldRef, false), 1, 0))},
		}
	case diff.CommonTypeDateTime:
		dt := coerce(fieldRef, []string{"date"})
		return []bson.E{
			present,
			{Key: p + "_min", Value: accumulator("$min", dt)},
			{Key: p + "_max", Value: accumulator("$max", dt)},
			{Key: p + "_distinct", Value: accumulator("$addToSet", dt)},
		}
	default:
		// json, binary and unknown types only track presence.
		return []bson.E{present}
	}
}

func runStats(ctx context.Context, coll *mongo.Collection, columns []*diff.Column, sampleSize int64) (bson.M, error) {
	pipeline := buildStatsPipeline(columns, sampleSize)
	cursor, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return nil, errors.Wrap(err, "failed to run statistics aggregation")
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return nil, errors.Wrap(err, "failed to decode statistics result")
	}
	if len(results) == 0 {
		return bson.M{}, nil
	}
	return results[0], nil
}

// parseStatsResult fills in col.Stats for each column from the aggregation's
// single result document. scanned is the number of documents the statistics were
// computed over; Count is reported as scanned and NullCount as the documents that
// were missing or null for that field, mirroring the SQL summarizers.
func parseStatsResult(doc bson.M, columns []*diff.Column, scanned int64) {
	for i, col := range columns {
		p := strconv.Itoa(i)
		present := getInt64(doc[p+"_present"])
		nullCount := scanned - present

		switch col.NormalizedType {
		case diff.CommonTypeNumeric:
			col.Stats = &diff.NumericalStatistics{
				Count:     scanned,
				NullCount: nullCount,
				Min:       getFloatPtr(doc[p+"_min"]),
				Max:       getFloatPtr(doc[p+"_max"]),
				Avg:       getFloatPtr(doc[p+"_avg"]),
				Sum:       getFloatPtr(doc[p+"_sum"]),
				StdDev:    getFloatPtr(doc[p+"_std"]),
			}
		case diff.CommonTypeString:
			col.Stats = &diff.StringStatistics{
				Count:         scanned,
				NullCount:     nullCount,
				DistinctCount: distinctCount(doc[p+"_distinct"]),
				EmptyCount:    getInt64(doc[p+"_empty"]),
				MinLength:     int(getInt64(doc[p+"_minlen"])),
				MaxLength:     int(getInt64(doc[p+"_maxlen"])),
				AvgLength:     getFloat(doc[p+"_avglen"]),
			}
		case diff.CommonTypeBoolean:
			col.Stats = &diff.BooleanStatistics{
				Count:      scanned,
				NullCount:  nullCount,
				TrueCount:  getInt64(doc[p+"_true"]),
				FalseCount: getInt64(doc[p+"_false"]),
			}
		case diff.CommonTypeDateTime:
			col.Stats = &diff.DateTimeStatistics{
				Count:        scanned,
				NullCount:    nullCount,
				UniqueCount:  distinctCount(doc[p+"_distinct"]),
				EarliestDate: getTimePtr(doc[p+"_min"]),
				LatestDate:   getTimePtr(doc[p+"_max"]),
			}
		case diff.CommonTypeJSON:
			col.Stats = &diff.JSONStatistics{
				Count:     scanned,
				NullCount: nullCount,
			}
		default:
			col.Stats = &diff.UnknownStatistics{}
		}
	}
}

// ---- aggregation expression helpers ----------------------------------------

func toBSONA(values []string) bson.A {
	out := make(bson.A, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

func typeOf(fieldRef string) bson.D {
	return bson.D{{Key: "$type", Value: fieldRef}}
}

func eq(a, b interface{}) bson.D {
	return bson.D{{Key: "$eq", Value: bson.A{a, b}}}
}

func ifThen(cond, then, otherwise interface{}) bson.D {
	return bson.D{{Key: "$cond", Value: bson.A{cond, then, otherwise}}}
}

func sum(expr interface{}) bson.D {
	return bson.D{{Key: "$sum", Value: expr}}
}

func accumulator(op string, expr interface{}) bson.D {
	return bson.D{{Key: op, Value: expr}}
}

// presentExpr yields 1 when the field exists and is not null, else 0.
func presentExpr(fieldRef string) bson.D {
	return ifThen(
		bson.D{{Key: "$in", Value: bson.A{typeOf(fieldRef), bson.A{"missing", "null"}}}},
		0, 1,
	)
}

// coerce returns the field's value when its BSON type is one of `types`, else null.
func coerce(fieldRef string, types []string) bson.D {
	return ifThen(
		bson.D{{Key: "$in", Value: bson.A{typeOf(fieldRef), toBSONA(types)}}},
		fieldRef, nil,
	)
}

// stringCoerce returns string values unchanged and stringifies ObjectIds; any
// other type becomes null.
func stringCoerce(fieldRef string) bson.D {
	return ifThen(
		bson.D{{Key: "$in", Value: bson.A{typeOf(fieldRef), toBSONA(stringBSONTypes)}}},
		bson.D{{Key: "$toString", Value: fieldRef}},
		nil,
	)
}

// ---- BSON value conversion -------------------------------------------------

func getInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case primitive.Decimal128:
		f, _ := strconv.ParseFloat(n.String(), 64)
		return int64(f)
	default:
		return 0
	}
}

func getFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	case primitive.Decimal128:
		f, _ := strconv.ParseFloat(n.String(), 64)
		return f
	default:
		return 0
	}
}

func getFloatPtr(v interface{}) *float64 {
	switch v.(type) {
	case int32, int64, float64, primitive.Decimal128:
		f := getFloat(v)
		return &f
	default:
		return nil
	}
}

func getTimePtr(v interface{}) *time.Time {
	switch t := v.(type) {
	case primitive.DateTime:
		tm := t.Time().UTC()
		return &tm
	case time.Time:
		tm := t.UTC()
		return &tm
	default:
		return nil
	}
}

// distinctCount counts the non-null entries of an $addToSet result.
func distinctCount(v interface{}) int64 {
	arr, ok := v.(primitive.A)
	if !ok {
		return 0
	}
	var count int64
	for _, item := range arr {
		if item == nil {
			continue
		}
		count++
	}
	return count
}
