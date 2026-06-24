package mongo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongoQuery is the JSON envelope accepted by `bruin query` for MongoDB
// connections. It describes a single find or aggregation against one collection:
//
//	{"collection":"users","filter":{"age":{"$gt":21}},"sort":{"age":-1},"limit":10}
//	{"collection":"orders","aggregate":[{"$group":{"_id":"$status","n":{"$sum":1}}}]}
//
// The filter/projection/sort/aggregate values are parsed as MongoDB Extended
// JSON, so type wrappers such as {"$oid":"..."} and {"$date":"..."} are honored
// while ordinary query operators ($gt, $and, $match, ...) pass through unchanged.
type mongoQuery struct {
	Collection string          `json:"collection"`
	Database   string          `json:"database,omitempty"`
	Filter     json.RawMessage `json:"filter,omitempty"`
	Projection json.RawMessage `json:"projection,omitempty"`
	Sort       json.RawMessage `json:"sort,omitempty"`
	Limit      *int64          `json:"limit,omitempty"`
	Skip       *int64          `json:"skip,omitempty"`
	Aggregate  json.RawMessage `json:"aggregate,omitempty"`
}

func parseMongoQuery(q string) (*mongoQuery, error) {
	trimmed := strings.TrimSpace(stripLeadingSQLComments(q))
	if trimmed == "" {
		return nil, errors.New("empty query")
	}

	var mq mongoQuery
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&mq); err != nil {
		return nil, errors.Wrap(err, `invalid MongoDB query; expected a JSON object such as {"collection":"users","filter":{...}}`)
	}

	if mq.Collection == "" {
		return nil, errors.New(`query must specify a "collection"`)
	}
	if len(mq.Aggregate) > 0 && (len(mq.Filter) > 0 || len(mq.Projection) > 0 || len(mq.Sort) > 0) {
		return nil, errors.New(`"aggregate" cannot be combined with "filter", "projection", or "sort"`)
	}

	return &mq, nil
}

// stripLeadingSQLComments removes leading blank and `--` comment lines. The
// query command prepends a `-- @bruin.config: ...` annotation comment to ad-hoc
// queries for tracking; that comment is meaningless for MongoDB and would break
// JSON parsing, so it is dropped before decoding the envelope.
func stripLeadingSQLComments(q string) string {
	lines := strings.Split(q, "\n")
	i := 0
	for i < len(lines) {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			i++
			continue
		}
		break
	}
	return strings.Join(lines[i:], "\n")
}

func (db *DB) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	res, err := db.SelectWithSchema(ctx, q)
	if err != nil {
		return nil, err
	}
	return res.Rows, nil
}

func (db *DB) SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error) {
	if err := db.initClient(ctx); err != nil {
		return nil, err
	}
	return RunQuery(ctx, db.client, db.config.Database, q.Query)
}

// RunQuery executes a MongoDB query envelope against the given client and
// returns a tabular result. defaultDatabase is used when the envelope omits a
// "database". It is shared by the mongo and mongo_atlas connection types.
func RunQuery(ctx context.Context, client *mongo.Client, defaultDatabase, queryStr string) (*query.QueryResult, error) {
	mq, err := parseMongoQuery(queryStr)
	if err != nil {
		return nil, err
	}

	dbName := defaultDatabase
	if mq.Database != "" {
		dbName = mq.Database
	}
	if dbName == "" {
		return nil, errors.New(`no database configured for this connection; set it on the connection or via "database" in the query`)
	}

	coll := client.Database(dbName).Collection(mq.Collection)

	cursor, err := openCursor(ctx, coll, mq)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	docs := make([]bson.D, 0)
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, errors.Wrap(err, "failed to decode document")
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, errors.Wrap(err, "error during cursor iteration")
	}

	return buildResult(docs), nil
}

func openCursor(ctx context.Context, coll *mongo.Collection, mq *mongoQuery) (*mongo.Cursor, error) {
	if len(mq.Aggregate) > 0 {
		var pipeline bson.A
		if err := bson.UnmarshalExtJSON(mq.Aggregate, false, &pipeline); err != nil {
			return nil, errors.Wrap(err, `invalid "aggregate" pipeline`)
		}
		if mq.Limit != nil {
			pipeline = append(pipeline, bson.D{{Key: "$limit", Value: *mq.Limit}})
		}
		return coll.Aggregate(ctx, pipeline)
	}

	filter := bson.M{}
	if len(mq.Filter) > 0 {
		if err := bson.UnmarshalExtJSON(mq.Filter, false, &filter); err != nil {
			return nil, errors.Wrap(err, `invalid "filter"`)
		}
	}

	opts := options.Find()
	if len(mq.Projection) > 0 {
		var projection bson.M
		if err := bson.UnmarshalExtJSON(mq.Projection, false, &projection); err != nil {
			return nil, errors.Wrap(err, `invalid "projection"`)
		}
		opts.SetProjection(projection)
	}
	if len(mq.Sort) > 0 {
		var sort bson.D
		if err := bson.UnmarshalExtJSON(mq.Sort, false, &sort); err != nil {
			return nil, errors.Wrap(err, `invalid "sort"`)
		}
		opts.SetSort(sort)
	}
	if mq.Limit != nil {
		opts.SetLimit(*mq.Limit)
	}
	if mq.Skip != nil {
		opts.SetSkip(*mq.Skip)
	}

	return coll.Find(ctx, filter, opts)
}

func (db *DB) Limit(q string, limit int64) string {
	return LimitQuery(q, limit)
}

// LimitQuery lets `bruin query --limit N` work for MongoDB by injecting the
// limit into the query envelope. If the query can't be parsed it is returned
// as-is. Shared by the mongo and mongo_atlas connection types.
func LimitQuery(q string, limit int64) string {
	mq, err := parseMongoQuery(q)
	if err != nil {
		return q
	}
	mq.Limit = &limit
	b, err := json.Marshal(mq)
	if err != nil {
		return q
	}
	return string(b)
}

// buildResult flattens the returned documents into a tabular result. Columns are
// the union of top-level field names in first-seen order; nested documents and
// arrays are rendered as Extended JSON strings.
func buildResult(docs []bson.D) *query.QueryResult {
	columns := make([]string, 0)
	colIndex := make(map[string]int)
	for _, doc := range docs {
		for _, field := range doc {
			if _, ok := colIndex[field.Key]; !ok {
				colIndex[field.Key] = len(columns)
				columns = append(columns, field.Key)
			}
		}
	}

	types := make([]string, len(columns))
	rows := make([][]interface{}, 0, len(docs))
	for _, doc := range docs {
		row := make([]interface{}, len(columns))
		for _, field := range doc {
			idx := colIndex[field.Key]
			row[idx] = formatMongoValue(field.Value)
			if types[idx] == "" && field.Value != nil {
				types[idx] = mongoTypeName(field.Value)
			}
		}
		rows = append(rows, row)
	}
	for i := range types {
		if types[i] == "" {
			types[i] = "null"
		}
	}

	return &query.QueryResult{
		Columns:     columns,
		Rows:        rows,
		ColumnTypes: types,
	}
}

func formatMongoValue(v interface{}) interface{} {
	switch val := v.(type) {
	case nil:
		return nil
	case primitive.ObjectID:
		return val.Hex()
	case primitive.DateTime:
		return val.Time().UTC()
	case primitive.Decimal128:
		return val.String()
	case primitive.Binary:
		return base64.StdEncoding.EncodeToString(val.Data)
	case string, bool, int32, int64, float64:
		return val
	default:
		// Nested documents/arrays and the less common BSON types are rendered as
		// Extended JSON so they survive in a flat cell.
		return marshalExtJSONValue(val)
	}
}

// marshalExtJSONValue serializes a single BSON value to relaxed Extended JSON.
// bson.MarshalExtJSON requires a top-level document, so the value is wrapped in
// a one-key document and the wrapper is stripped back off.
func marshalExtJSONValue(v interface{}) interface{} {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, false, false)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	s := strings.TrimPrefix(string(b), `{"v":`)
	s = strings.TrimSuffix(s, "}")
	return s
}

func mongoTypeName(v interface{}) string {
	switch v.(type) {
	case primitive.ObjectID:
		return "objectId"
	case primitive.DateTime:
		return "date"
	case primitive.Decimal128:
		return "decimal"
	case primitive.Binary:
		return "binary"
	case primitive.A:
		return "array"
	case primitive.M, primitive.D:
		return "object"
	case bool:
		return "bool"
	case int32:
		return "int"
	case int64:
		return "long"
	case float64:
		return "double"
	case string:
		return "string"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%T", v)
	}
}
