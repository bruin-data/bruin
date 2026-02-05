package bigquery

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	datatransfer "cloud.google.com/go/bigquery/datatransfer/apiv1"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var scopes = []string{
	bigquery.Scope,
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/drive",
}

type Querier interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
	Ping(ctx context.Context) error
}
type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
}

type MetadataUpdater interface {
	UpdateTableMetadataIfNotExist(ctx context.Context, asset *pipeline.Asset) error
}

type TableManager interface {
	IsPartitioningOrClusteringMismatch(ctx context.Context, meta *bigquery.TableMetadata, asset *pipeline.Asset) bool
	CreateDataSetIfNotExist(asset *pipeline.Asset, ctx context.Context) error
	IsMaterializationTypeMismatch(ctx context.Context, meta *bigquery.TableMetadata, asset *pipeline.Asset) bool
	DropTableOnMismatch(ctx context.Context, tableName string, asset *pipeline.Asset) error
	BuildTableExistsQuery(tableName string) (string, error)
}

type DB interface {
	Querier
	Selector
	MetadataUpdater
	TableManager
	UsesApplicationDefaultCredentials() bool
}

var (
	datasetNameCache sync.Map // Global cache for dataset existence
	datasetLocks     sync.Map // Global map for dataset-specific locks
)

type Client struct {
	client      *bigquery.Client
	config      *Config
	typeMapper  *diff.DatabaseTypeMapper
	clientMutex sync.Mutex // Protects lazy client creation
}

func NewDB(c *Config) (*Client, error) {
	options := []option.ClientOption{
		option.WithScopes(scopes...),
	}

	// If ADC is enabled, create the client lazily (when it's actually used)
	// This allows pipelines without BigQuery assets to run even if ADC is not configured
	if c.UseApplicationDefaultCredentials {
		return &Client{
			client:     nil, // Will be created lazily when ensureClientInitialized is called
			config:     c,
			typeMapper: diff.NewBigQueryTypeMapper(),
		}, nil
	}

	// For explicit credentials, create the client immediately
	switch {
	case c.CredentialsJSON != "":
		options = append(options, option.WithCredentialsJSON([]byte(c.CredentialsJSON)))
	case c.CredentialsFilePath != "":
		options = append(options, option.WithCredentialsFile(c.CredentialsFilePath))
	case c.Credentials != nil:
		options = append(options, option.WithCredentials(c.Credentials))
	default:
		return nil, errors.New("no credentials provided")
	}

	client, err := bigquery.NewClient(
		context.Background(),
		c.ProjectID,
		options...,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bigquery client")
	}

	// Set location if specified (used for query execution region)
	if c.Location != "" {
		client.Location = c.Location
	}

	return &Client{
		client:     client,
		config:     c,
		typeMapper: diff.NewBigQueryTypeMapper(),
	}, nil
}

func (d *Client) GetIngestrURI() (string, error) {
	return d.config.GetIngestrURI()
}

func (d *Client) ProjectID() string {
	return d.config.ProjectID
}

func (d *Client) Location() string {
	return d.config.Location
}

func (d *Client) UsesApplicationDefaultCredentials() bool {
	return d.config.UseApplicationDefaultCredentials
}

// createClient creates the underlying BigQuery client if it doesn't exist.
// This is used for lazy initialization when ADC credentials weren't available at startup.
func (d *Client) createClient(ctx context.Context) error {
	d.clientMutex.Lock()
	defer d.clientMutex.Unlock()

	// Double-check in case another goroutine created it
	if d.client != nil {
		return nil
	}

	options := []option.ClientOption{
		option.WithScopes(scopes...),
	}

	// If ADC is enabled, no explicit credentials are needed
	if !d.config.UseApplicationDefaultCredentials {
		switch {
		case d.config.CredentialsJSON != "":
			options = append(options, option.WithCredentialsJSON([]byte(d.config.CredentialsJSON)))
		case d.config.CredentialsFilePath != "":
			options = append(options, option.WithCredentialsFile(d.config.CredentialsFilePath))
		case d.config.Credentials != nil:
			options = append(options, option.WithCredentials(d.config.Credentials))
		default:
			return errors.New("no credentials provided")
		}
	}

	client, err := bigquery.NewClient(
		ctx,
		d.config.ProjectID,
		options...,
	)
	if err != nil {
		return errors.Wrap(err, "failed to create bigquery client")
	}

	// Set location if specified
	if d.config.Location != "" {
		client.Location = d.config.Location
	}

	d.client = client
	return nil
}

// ensureClientInitialized ensures the underlying BigQuery client exists, creating it if necessary.
// This is required for ADC (Application Default Credentials) lazy initialization - the client
// may be nil when ADC is enabled and credentials weren't available at startup.
func (d *Client) ensureClientInitialized(ctx context.Context) error {
	if d.client == nil {
		return d.createClient(ctx)
	}
	return nil
}

func (d *Client) NewDataTransferClient(ctx context.Context) (*datatransfer.Client, error) {
	options := []option.ClientOption{
		option.WithScopes(scopes...),
	}

	// Check if ADC is explicitly disabled
	if !d.config.UseApplicationDefaultCredentials {
		switch {
		case d.config.CredentialsJSON != "":
			options = append(options, option.WithCredentialsJSON([]byte(d.config.CredentialsJSON)))
		case d.config.CredentialsFilePath != "":
			options = append(options, option.WithCredentialsFile(d.config.CredentialsFilePath))
		case d.config.Credentials != nil:
			options = append(options, option.WithCredentials(d.config.Credentials))
		default:
			return nil, errors.New("no credentials provided for Data Transfer client")
		}
	} else {
		// If ADC is enabled, proactively check if credentials are available
		_, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, &ADCCredentialError{
				ClientType:  "Data Transfer client",
				OriginalErr: err,
			}
		}
	}

	client, err := datatransfer.NewClient(ctx, options...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Data Transfer client")
	}
	return client, nil
}

func (d *Client) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return false, err
	}
	q := d.client.Query(query.ToDryRunQuery())
	q.DryRun = true

	job, err := q.Run(ctx)
	if err != nil {
		return false, formatError(err)
	}

	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return false, err
	}

	return true, nil
}

func (d *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return err
	}
	q := d.client.Query(query.String())
	_, err := q.Read(ctx)
	if err != nil {
		return formatError(err)
	}

	return nil
}

func (d *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	q := d.client.Query(query.String())
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, formatError(err)
	}

	result := make([][]interface{}, 0)
	for {
		var values []bigquery.Value
		err := rows.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		interfaces := make([]interface{}, len(values))
		for i, v := range values {
			interfaces[i] = v
		}

		result = append(result, interfaces)
	}

	return result, nil
}

func (d *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	q := d.client.Query(queryObj.String())
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate query read: %w", err)
	}

	result := &query.QueryResult{
		Columns:     []string{},
		Rows:        [][]interface{}{},
		ColumnTypes: []string{},
	}

	// Add a ColumnTypes field to store the types
	columnTypes := []string{}

	for {
		var values []bigquery.Value
		err := rows.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = v
		}
		result.Rows = append(result.Rows, row)
	}

	if rows.Schema != nil {
		for _, field := range rows.Schema {
			result.Columns = append(result.Columns, field.Name)
			// Extract the type information from the schema
			columnTypes = append(columnTypes, string(field.Type))
		}
	} else {
		return nil, errors.New("schema information is not available")
	}

	// Store the column types in the result
	result.ColumnTypes = columnTypes

	return result, nil
}

func (d *Client) QueryDryRun(ctx context.Context, queryObj *query.Query) (*bigquery.QueryStatistics, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	q := d.client.Query(queryObj.String())
	q.DryRun = true

	if d.client.Location != "" {
		q.Location = d.client.Location
	}

	job, err := q.Run(ctx)
	if err != nil {
		return nil, formatError(err)
	}

	status := job.LastStatus()
	if status == nil {
		return nil, errors.New("missing job status for dry run")
	}
	if status.Err() != nil {
		return nil, status.Err()
	}
	if status.Statistics == nil {
		return nil, errors.New("missing statistics in dry run status")
	}

	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok || qs == nil {
		return nil, errors.New("missing query statistics details in dry run status")
	}

	return qs, nil
}

type NoMetadataUpdatedError struct{}

func (m NoMetadataUpdatedError) Error() string {
	return "no metadata found for the given asset to be pushed to BigQuery"
}

// ADCCredentialError represents an error when Application Default Credentials cannot be found or are invalid.
type ADCCredentialError struct {
	ClientType  string // e.g., "BigQuery client" or "Data Transfer client"
	OriginalErr error
}

func (e *ADCCredentialError) Error() string {
	return fmt.Sprintf("ADC credentials not found for %s: %v\n"+
		"Run: gcloud auth application-default login",
		e.ClientType, e.OriginalErr)
}

func (e *ADCCredentialError) Unwrap() error {
	return e.OriginalErr
}

func (d *Client) getTableRef(ctx context.Context, tableName string) (*bigquery.Table, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	tableComponents := strings.Split(tableName, ".")
	// Check for empty components
	for _, component := range tableComponents {
		if component == "" {
			return nil, fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
		}
	}
	switch len(tableComponents) {
	case 2:
		return d.client.DatasetInProject(d.config.ProjectID, tableComponents[0]).Table(tableComponents[1]), nil
	case 3:
		return d.client.DatasetInProject(tableComponents[0], tableComponents[1]).Table(tableComponents[2]), nil
	default:
		return nil, fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
	}
}

func (d *Client) UpdateTableMetadataIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	anyColumnHasDescription := false
	colsByName := make(map[string]*pipeline.Column, len(asset.Columns))
	for _, col := range asset.Columns {
		colsByName[col.Name] = &col
		if col.Description != "" {
			anyColumnHasDescription = true
		}
	}

	if asset.Description == "" && (len(asset.Columns) == 0 || !anyColumnHasDescription) {
		return NoMetadataUpdatedError{}
	}
	tableRef, err := d.getTableRef(ctx, asset.Name)
	if err != nil {
		return err
	}

	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			return nil
		}
		return err
	}
	schema := meta.Schema
	colsChanged := false
	for _, field := range schema {
		if col, ok := colsByName[field.Name]; ok {
			field.Description = col.Description
			colsChanged = true
		}
	}

	update := bigquery.TableMetadataToUpdate{}

	if colsChanged {
		update.Schema = schema
	}

	if asset.Description != "" {
		update.Description = asset.Description
	}
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		update.TableConstraints = &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{Columns: primaryKeys},
		}
	}

	if _, err = tableRef.Update(ctx, update, meta.ETag); err != nil {
		return errors.Wrap(err, "failed to update table metadata")
	}

	return nil
}

func formatError(err error) error {
	var googleError *googleapi.Error
	if !errors.As(err, &googleError) {
		return err
	}

	if googleError.Code == 404 || googleError.Code == 400 {
		return fmt.Errorf("%s", googleError.Message)
	}

	return googleError
}

// Test runs a simple query (SELECT 1) to validate the connection.
func (d *Client) Ping(ctx context.Context) error {
	// Define the test query
	q := query.Query{
		Query: "SELECT 1",
	}

	// Use the existing RunQueryWithoutResult method
	err := d.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Bigquery connection")
	}

	return nil // Return nil if the query runs successfully
}

func (d *Client) IsPartitioningOrClusteringMismatch(ctx context.Context, meta *bigquery.TableMetadata, asset *pipeline.Asset) bool {
	if meta.TimePartitioning != nil || meta.RangePartitioning != nil || asset.Materialization.PartitionBy != "" || len(asset.Materialization.ClusterBy) > 0 {
		if !IsSamePartitioning(meta, asset) || !IsSameClustering(meta, asset) {
			return true
		}
	}
	return false
}

func IsSamePartitioning(meta *bigquery.TableMetadata, asset *pipeline.Asset) bool {
	// If asset has no partition but table does, they don't match
	if asset.Materialization.PartitionBy == "" &&
		(meta.TimePartitioning != nil || meta.RangePartitioning != nil) {
		return false
	}

	if asset.Materialization.PartitionBy != "" &&
		meta.TimePartitioning == nil &&
		meta.RangePartitioning == nil {
		return false
	}

	if meta.TimePartitioning == nil && meta.RangePartitioning == nil {
		return true
	}

	// Compile the regex for parsing partition expressions
	partitionRegex := regexp.MustCompile(`^\s*(?:(?:date(?:time)?_trunc|timestamp_trunc|date_trunc)\(\s*([A-Za-z_][\w.]*)\s*,\s*(day|hour|month|year)\s*\)|date\(\s*([A-Za-z_][\w.]*)\s*\)|([A-Za-z_][\w.]*)\s*)$`)

	// Parse the asset's partition expression
	assetPartitionBy := strings.ToLower(strings.TrimSpace(asset.Materialization.PartitionBy))
	assetMatches := partitionRegex.FindStringSubmatch(assetPartitionBy)

	var assetColumn string
	var assetPartitionType string

	// if match, FindStringSubmatch() returns a slice of exactly 5 elements [fullMatch, group1, group2, group3, group4], else returns nil.
	if assetMatches != nil {
		// Extract column and partition type from regex matches
		switch {
		case assetMatches[1] != "" && assetMatches[2] != "":
			// date_trunc/timestamp_trunc case
			assetColumn = strings.ToLower(assetMatches[1])
			assetPartitionType = strings.ToLower(assetMatches[2])
		case assetMatches[3] != "":
			// date() case
			assetColumn = strings.ToLower(assetMatches[3])
			assetPartitionType = "day" // date() defaults to day partitioning
		case assetMatches[4] != "":
			// simple column case
			assetColumn = strings.ToLower(assetMatches[4])
			assetPartitionType = "day" // default to day partitioning
		}
	}

	// If regex failed to extract a column name, the partition expression is invalid
	if assetColumn == "" {
		return false
	}

	if meta.TimePartitioning != nil {
		metaField := strings.ToLower(meta.TimePartitioning.Field)
		metaType := strings.ToLower(string(meta.TimePartitioning.Type))

		// Compare column names (case-insensitive)
		if metaField != assetColumn {
			return false
		}

		// Compare partition types (defaults to DAY for simple column names)
		if assetPartitionType != "" && metaType != "" && metaType != assetPartitionType {
			return false
		}
	}

	if meta.RangePartitioning != nil {
		metaField := strings.ToLower(meta.RangePartitioning.Field)

		// For range partitioning, only compare the column name
		if metaField != assetColumn {
			return false
		}
	}
	return true
}

func IsSameClustering(meta *bigquery.TableMetadata, asset *pipeline.Asset) bool {
	if len(asset.Materialization.ClusterBy) > 0 &&
		(meta.Clustering == nil || len(meta.Clustering.Fields) == 0) {
		return false
	}
	if meta.Clustering == nil {
		return true
	}

	bigQueryFields := meta.Clustering.Fields
	userFields := asset.Materialization.ClusterBy

	if len(bigQueryFields) != len(userFields) {
		return false
	}

	for i := range bigQueryFields {
		if bigQueryFields[i] != userFields[i] {
			return false
		}
	}

	return true
}

func (d *Client) CreateDataSetIfNotExist(asset *pipeline.Asset, ctx context.Context) error {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return err
	}
	tableName := asset.Name
	tableComponents := strings.Split(tableName, ".")
	var datasetName string
	var projectID string

	switch len(tableComponents) {
	case 2:
		projectID = d.config.ProjectID
		datasetName = tableComponents[0]
	case 3:
		datasetName = tableComponents[1]
		projectID = tableComponents[0]
	default:
		return nil
	}

	cacheKey := fmt.Sprintf("%s.%s", projectID, datasetName)

	if _, exists := datasetNameCache.Load(cacheKey); exists {
		return nil
	}

	lock, _ := datasetLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)

	mutex.Lock()
	defer mutex.Unlock()

	if _, exists := datasetNameCache.Load(cacheKey); exists {
		return nil
	}

	dataset := d.client.DatasetInProject(projectID, datasetName)
	_, err := dataset.Metadata(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		isAPIErr := errors.As(err, &apiErr)

		if !isAPIErr {
			return fmt.Errorf("failed to fetch metadata to create dataset for table '%s': %w", tableName, err)
		}

		if isAPIErr && apiErr.Code == 404 {
			if err := dataset.Create(ctx, &bigquery.DatasetMetadata{}); err != nil {
				var createApiErr *googleapi.Error //nolint:stylecheck
				if errors.As(err, &createApiErr) && createApiErr.Code == 409 {
					// Dataset already exists (created by another process), ignore this error
				} else {
					return fmt.Errorf("failed to create dataset '%s': %w", datasetName, err)
				}
			}
			datasetNameCache.Store(cacheKey, true)
		} else {
			return errors.Errorf("google api returned error, http status: %d, headers: %v, body: %s", apiErr.Code, apiErr.Header, apiErr.Body)
		}
	}

	return nil
}

func (d *Client) IsMaterializationTypeMismatch(ctx context.Context, meta *bigquery.TableMetadata, asset *pipeline.Asset) bool {
	if asset.Materialization.Type == pipeline.MaterializationTypeNone {
		return false
	}

	tableType := meta.Type
	return !strings.EqualFold(string(tableType), string(asset.Materialization.Type))
}

func (d *Client) DropTableOnMismatch(ctx context.Context, tableName string, asset *pipeline.Asset) error {
	tableRef, err := d.getTableRef(ctx, tableName)
	if err != nil {
		return err
	}
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			return nil
		}
		return fmt.Errorf("failed to fetch metadata for table '%s': %w", tableName, err)
	}
	if d.IsMaterializationTypeMismatch(ctx, meta, asset) || d.IsPartitioningOrClusteringMismatch(ctx, meta, asset) {
		if err := tableRef.Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete table '%s': %w", tableName, err)
		}
	}
	return nil
}

func (d *Client) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
		}
	}

	var datasetRef, targetTable string

	switch len(tableComponents) {
	case 2:
		datasetRef = fmt.Sprintf("%s.%s.INFORMATION_SCHEMA.TABLES", d.config.ProjectID, tableComponents[0])
		targetTable = tableComponents[1]
	case 3:
		datasetRef = fmt.Sprintf("%s.%s.INFORMATION_SCHEMA.TABLES", tableComponents[0], tableComponents[1])
		targetTable = tableComponents[2]
	default:
		return "", fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
	}

	// Use EXISTS to return true or false
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WHERE table_name = '%s'", datasetRef, targetTable)

	return strings.TrimSpace(query), nil
}

func (d *Client) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	var rowCount int64

	// Get row count only if not in schema-only mode
	if !schemaOnly {
		countQuery := fmt.Sprintf("SELECT COUNT(*) as row_count FROM `%s`", tableName)
		countResult, err := d.Select(ctx, &query.Query{Query: countQuery})
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}

		if len(countResult) > 0 && len(countResult[0]) > 0 {
			switch val := countResult[0][0].(type) {
			case int64:
				rowCount = val
			case int:
				rowCount = int64(val)
			case int32:
				rowCount = int64(val)
			case float64:
				rowCount = int64(val)
			case string:
				// Handle string representation of numbers (common with BigQuery)
				parsed, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("failed to parse row count string '%s' for table '%s': %w", val, tableName, err)
				}
				rowCount = parsed
			default:
				return nil, fmt.Errorf("unexpected row count type for table '%s': got %T with value %v", tableName, val, val)
			}
		}
	}

	// Get table schema using INFORMATION_SCHEMA
	tableComponents := strings.Split(tableName, ".")
	var datasetRef string
	var targetTable string

	switch len(tableComponents) {
	case 2:
		// dataset.table format
		datasetRef = fmt.Sprintf("%s.%s", d.config.ProjectID, tableComponents[0])
		targetTable = tableComponents[1]
	case 3:
		// project.dataset.table format
		datasetRef = fmt.Sprintf("%s.%s", tableComponents[0], tableComponents[1])
		targetTable = tableComponents[2]
	default:
		return nil, fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
	}

	schemaQuery := buildSchemaQuery(datasetRef, targetTable)

	schemaResult, err := d.Select(ctx, &query.Query{Query: schemaQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}

	columns := make([]*diff.Column, 0, len(schemaResult))
	for _, row := range schemaResult {
		if len(row) < 4 {
			continue
		}

		columnName, ok := row[0].(string)
		if !ok {
			continue
		}

		dataType, ok := row[1].(string)
		if !ok {
			continue
		}

		isNullableStr, ok := row[2].(string)
		if !ok {
			continue
		}

		isPartitioning, _ := row[3].(string)

		nullable := strings.ToLower(isNullableStr) == "yes"
		normalizedType := d.typeMapper.MapType(dataType)

		// Debug: log type mapping for troubleshooting
		if normalizedType == diff.CommonTypeUnknown {
			fmt.Printf("Warning: Unknown type mapping for BigQuery type '%s' in column '%s'\n", dataType, columnName)
		}

		// Collect statistics for this column
		var stats diff.ColumnStatistics
		if schemaOnly {
			// In schema-only mode, don't collect statistics
			stats = nil
		} else {
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = d.fetchNumericalStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s' (BigQuery type: %s, normalized: %s): %w", columnName, dataType, normalizedType, err)
				}
			case diff.CommonTypeString:
				stats, err = d.fetchStringStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s' (BigQuery type: %s, normalized: %s): %w", columnName, dataType, normalizedType, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = d.fetchBooleanStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s' (BigQuery type: %s, normalized: %s): %w", columnName, dataType, normalizedType, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = d.fetchDateTimeStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s' (BigQuery type: %s, normalized: %s): %w", columnName, dataType, normalizedType, err)
				}
			case diff.CommonTypeJSON:
				stats, err = d.fetchJSONStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON stats for column '%s' (BigQuery type: %s, normalized: %s): %w", columnName, dataType, normalizedType, err)
				}
			case diff.CommonTypeBinary, diff.CommonTypeUnknown:
				fmt.Printf("Warning: Using unknown statistics for column '%s' with BigQuery type '%s'\n", columnName, dataType)
				stats = &diff.UnknownStatistics{}
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           dataType,
			NormalizedType: normalizedType,
			Nullable:       nullable,
			PrimaryKey:     false,                   // BigQuery doesn't have traditional primary keys
			Unique:         isPartitioning == "YES", // Use partitioning as a proxy for uniqueness
			Stats:          stats,
		})
	}

	dbTable := &diff.Table{
		Name:    tableName,
		Columns: columns,
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table:    dbTable,
	}, nil
}

func buildSchemaQuery(datasetRef, targetTable string) string {
	schemaTemplate := strings.Join([]string{
		"SELECT",
		"  column_name,",
		"  data_type,",
		"  is_nullable,",
		"  is_partitioning_column",
		"FROM `%s.INFORMATION_SCHEMA.COLUMNS`",
		"WHERE table_name = '%s'",
		"ORDER BY ordinal_position",
	}, "\n")
	return fmt.Sprintf(schemaTemplate, datasetRef, targetTable)
}

func (d *Client) fetchNumericalStats(ctx context.Context, tableName, columnName string) (*diff.NumericalStatistics, error) {
	statsQuery := fmt.Sprintf(`
		SELECT 
			MIN(%s) as min_val,
			MAX(%s) as max_val,
			AVG(%s) as avg_val,
			SUM(%s) as sum_val,
			COUNT(%s) as count_val,
			COUNTIF(%s IS NULL) as null_count,
			STDDEV(%s) as stddev_val
		FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, columnName, "`"+tableName+"`")

	result, err := d.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
	}

	if len(result) == 0 || len(result[0]) < 7 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.NumericalStatistics{}

	// Handle potentially null values from BigQuery
	if val, ok := row[0].(float64); ok {
		stats.Min = &val
	}
	if val, ok := row[1].(float64); ok {
		stats.Max = &val
	}
	if val, ok := row[2].(float64); ok {
		stats.Avg = &val
	}
	if val, ok := row[3].(float64); ok {
		stats.Sum = &val
	}
	if val, ok := row[4].(int64); ok {
		stats.Count = val
	}
	if val, ok := row[5].(int64); ok {
		stats.NullCount = val
	}
	if val, ok := row[6].(float64); ok {
		stats.StdDev = &val
	}

	return stats, nil
}

func (d *Client) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	statsQuery := fmt.Sprintf(`
		SELECT 
			MIN(LENGTH(%s)) as min_len,
			MAX(LENGTH(%s)) as max_len,
			AVG(LENGTH(%s)) as avg_len,
			COUNT(DISTINCT %s) as distinct_count,
			COUNT(*) as total_count,
			COUNTIF(%s IS NULL) as null_count,
			COUNTIF(%s = '') as empty_count
		FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, "`"+tableName+"`")

	result, err := d.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
	}

	if len(result) == 0 || len(result[0]) < 7 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.StringStatistics{}

	if val, ok := row[0].(int64); ok {
		stats.MinLength = int(val)
	}
	if val, ok := row[1].(int64); ok {
		stats.MaxLength = int(val)
	}
	if val, ok := row[2].(float64); ok {
		stats.AvgLength = val
	}
	if val, ok := row[3].(int64); ok {
		stats.DistinctCount = val
	}
	if val, ok := row[4].(int64); ok {
		stats.Count = val
	}
	if val, ok := row[5].(int64); ok {
		stats.NullCount = val
	}
	if val, ok := row[6].(int64); ok {
		stats.EmptyCount = val
	}

	return stats, nil
}

func (d *Client) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	statsQuery := fmt.Sprintf(`
		SELECT 
			COUNTIF(%s = true) as true_count,
			COUNTIF(%s = false) as false_count,
			COUNT(*) as total_count,
			COUNTIF(%s IS NULL) as null_count
		FROM %s`,
		columnName, columnName, columnName, "`"+tableName+"`")

	result, err := d.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
	}

	if len(result) == 0 || len(result[0]) < 4 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.BooleanStatistics{}

	if val, ok := row[0].(int64); ok {
		stats.TrueCount = val
	}
	if val, ok := row[1].(int64); ok {
		stats.FalseCount = val
	}
	if val, ok := row[2].(int64); ok {
		stats.Count = val
	}
	if val, ok := row[3].(int64); ok {
		stats.NullCount = val
	}

	return stats, nil
}

func (d *Client) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	statsQuery := fmt.Sprintf(`
		SELECT 
			MIN(%s) as min_date,
			MAX(%s) as max_date,
			COUNT(DISTINCT %s) as unique_count,
			COUNT(*) as count_val,
			COUNTIF(%s IS NULL) as null_count
		FROM %s`,
		columnName, columnName, columnName, columnName, "`"+tableName+"`")

	result, err := d.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
	}

	if len(result) == 0 || len(result[0]) < 5 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.DateTimeStatistics{}

	// Handle datetime values - convert to proper time.Time objects
	if row[0] != nil {
		if parsedTime, err := diff.ParseDateTime(row[0]); err == nil {
			stats.EarliestDate = parsedTime
		}
	}

	if row[1] != nil {
		if parsedTime, err := diff.ParseDateTime(row[1]); err == nil {
			stats.LatestDate = parsedTime
		}
	}
	if val, ok := row[2].(int64); ok {
		stats.UniqueCount = val
	}
	if val, ok := row[3].(int64); ok {
		stats.Count = val
	}
	if val, ok := row[4].(int64); ok {
		stats.NullCount = val
	}

	return stats, nil
}

func (d *Client) fetchJSONStats(ctx context.Context, tableName, columnName string) (*diff.JSONStatistics, error) {
	statsQuery := fmt.Sprintf(`
		SELECT 
			COUNT(*) as count_val,
			COUNTIF(%s IS NULL) as null_count
		FROM %s`,
		columnName, "`"+tableName+"`")

	result, err := d.Select(ctx, &query.Query{Query: statsQuery})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
	}

	if len(result) == 0 || len(result[0]) < 2 {
		return nil, fmt.Errorf("insufficient statistical data returned for column '%s'", columnName)
	}

	row := result[0]
	stats := &diff.JSONStatistics{}

	if val, ok := row[0].(int64); ok {
		stats.Count = val
	}
	if val, ok := row[1].(int64); ok {
		stats.NullCount = val
	}

	return stats, nil
}

// tableMetadataResult holds the result of fetching table metadata.
type tableMetadataResult struct {
	Columns        []*ansisql.DBColumn
	TableType      ansisql.DBTableType
	ViewDefinition string

	// Metadata for enriched import descriptions
	CreatedAt    *time.Time
	LastModified *time.Time
	RowCount     *int64
	SizeBytes    *int64
	Description  string
}

func (d *Client) getTableMetadata(ctx context.Context, datasetID, tableID string) (*tableMetadataResult, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	meta, err := d.client.Dataset(datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for table %s.%s: %w", datasetID, tableID, err)
	}

	cols := make([]*ansisql.DBColumn, 0, len(meta.Schema))
	for _, field := range meta.Schema {
		cols = append(cols, &ansisql.DBColumn{
			Name:        field.Name,
			Type:        string(field.Type),
			Nullable:    !field.Required,
			PrimaryKey:  false,
			Unique:      false,
			Description: field.Description,
		})
	}

	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })

	result := &tableMetadataResult{
		Columns:     cols,
		Description: meta.Description,
	}

	// Populate metadata timestamps and sizes
	if !meta.CreationTime.IsZero() {
		createdAt := meta.CreationTime
		result.CreatedAt = &createdAt
	}
	if !meta.LastModifiedTime.IsZero() {
		lastModified := meta.LastModifiedTime
		result.LastModified = &lastModified
	}
	if meta.NumRows > 0 {
		rowCount := int64(meta.NumRows)
		result.RowCount = &rowCount
	}
	if meta.NumBytes > 0 {
		sizeBytes := meta.NumBytes
		result.SizeBytes = &sizeBytes
	}

	// Determine table type and get view definition
	switch meta.Type {
	case bigquery.ViewTable:
		result.TableType = ansisql.DBTableTypeView
		result.ViewDefinition = meta.ViewQuery
	case bigquery.MaterializedView:
		result.TableType = ansisql.DBTableTypeView
		if meta.MaterializedView != nil {
			result.ViewDefinition = meta.MaterializedView.Query
		}
	case bigquery.RegularTable, bigquery.ExternalTable, bigquery.Snapshot:
		result.TableType = ansisql.DBTableTypeTable
	}

	return result, nil
}

func (d *Client) getTableColumns(ctx context.Context, datasetID, tableID string) ([]*ansisql.DBColumn, error) {
	result, err := d.getTableMetadata(ctx, datasetID, tableID)
	if err != nil {
		return nil, err
	}
	return result.Columns, nil
}

func (d *Client) GetDatabases(ctx context.Context) ([]string, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	var databases []string

	datasetsIter := d.client.Datasets(ctx)
	for {
		ds, err := datasetsIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list BigQuery datasets: %w", err)
		}

		databases = append(databases, ds.DatasetID)
	}

	sort.Strings(databases)
	return databases, nil
}

// GetTables retrieves all table names from a BigQuery dataset (database).
// It takes a context and dataset name as parameters and returns a slice of table names.
// The method handles errors appropriately and returns an empty slice if the dataset has no tables.
func (d *Client) GetTables(ctx context.Context, databaseName string) ([]string, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	// Validate dataset name
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}

	// Check if dataset exists
	dataset := d.client.Dataset(databaseName)
	_, err := dataset.Metadata(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			return nil, fmt.Errorf("dataset '%s' does not exist", databaseName)
		}
		return nil, fmt.Errorf("failed to access dataset '%s': %w", databaseName, err)
	}

	// Get all tables in the dataset
	var tableNames []string
	tablesIter := dataset.Tables(ctx)
	for {
		table, err := tablesIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list tables in dataset '%s': %w", databaseName, err)
		}
		tableNames = append(tableNames, table.TableID)
	}

	sort.Strings(tableNames)
	return tableNames, nil
}

// GetColumns retrieves column information for a specific table in a BigQuery dataset.
// It takes a context, dataset name, and table name as parameters and returns a slice of column information.
// The method handles errors appropriately and returns an error if the table doesn't exist.
func (d *Client) GetColumns(ctx context.Context, databaseName, tableName string) ([]*ansisql.DBColumn, error) {
	// Validate input parameters
	if databaseName == "" {
		return nil, errors.New("database name cannot be empty")
	}
	if tableName == "" {
		return nil, errors.New("table name cannot be empty")
	}

	// Use the existing getTableColumns method
	columns, err := d.getTableColumns(ctx, databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for table '%s.%s': %w", databaseName, tableName, err)
	}

	return columns, nil
}

func (d *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}
	projectID := d.config.ProjectID

	summary := &ansisql.DBDatabase{
		Name:    projectID,
		Schemas: []*ansisql.DBSchema{},
	}

	// Phase 1: Collect all datasets and their table names (fast, no metadata fetching)
	type datasetInfo struct {
		name       string
		tableNames []string
	}
	var datasets []datasetInfo

	datasetsIter := d.client.Datasets(ctx)
	for {
		ds, err := datasetsIter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list BigQuery datasets: %w", err)
		}

		var tableNames []string
		tables := ds.Tables(ctx)
		for {
			t, err := tables.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list tables in dataset %s: %w", ds.DatasetID, err)
			}
			tableNames = append(tableNames, t.TableID)
		}

		datasets = append(datasets, datasetInfo{name: ds.DatasetID, tableNames: tableNames})
	}

	// Phase 2: For each dataset, determine which tables need metadata (skip older shards)
	// and create schema structures
	type tableJob struct {
		datasetName string
		actualTable string
		displayName string
		schemaIndex int
	}
	var jobs []tableJob

	for i, ds := range datasets {
		schema := &ansisql.DBSchema{
			Name:   ds.name,
			Tables: []*ansisql.DBTable{},
		}
		summary.Schemas = append(summary.Schemas, schema)

		// Select only tables that need metadata (non-sharded + most recent shard per group)
		tablesToFetch := selectTablesToFetch(ds.tableNames)
		for _, tf := range tablesToFetch {
			jobs = append(jobs, tableJob{
				datasetName: ds.name,
				actualTable: tf.actualName,
				displayName: tf.displayName,
				schemaIndex: i,
			})
		}
	}

	// Phase 3: Fetch all table metadata in parallel
	mu := sync.Mutex{}
	var errs []error

	workers := max(runtime.NumCPU(), 8)
	p := pool.New().WithMaxGoroutines(workers)

	for _, job := range jobs {
		p.Go(func() {
			metadata, err := d.getTableMetadata(ctx, job.datasetName, job.actualTable)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to get metadata for table %s.%s: %w", job.datasetName, job.actualTable, err))
				mu.Unlock()
				return
			}

			table := &ansisql.DBTable{
				Name:           job.displayName,
				Type:           metadata.TableType,
				ViewDefinition: metadata.ViewDefinition,
				Columns:        metadata.Columns,
				CreatedAt:      metadata.CreatedAt,
				LastModified:   metadata.LastModified,
				RowCount:       metadata.RowCount,
				SizeBytes:      metadata.SizeBytes,
				Description:    metadata.Description,
			}

			mu.Lock()
			summary.Schemas[job.schemaIndex].Tables = append(summary.Schemas[job.schemaIndex].Tables, table)
			mu.Unlock()
		})
	}

	p.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	// Sort tables within each schema and sort schemas
	for _, schema := range summary.Schemas {
		sort.Slice(schema.Tables, func(i, j int) bool { return schema.Tables[i].Name < schema.Tables[j].Name })
	}
	sort.Slice(summary.Schemas, func(i, j int) bool { return summary.Schemas[i].Name < summary.Schemas[j].Name })

	return summary, nil
}

func (d *Client) GetDatabaseSummaryForSchemas(ctx context.Context, schemas []string) (*ansisql.DBDatabase, error) {
	if err := d.ensureClientInitialized(ctx); err != nil {
		return nil, err
	}

	projectID := d.config.ProjectID

	summary := &ansisql.DBDatabase{
		Name:    projectID,
		Schemas: []*ansisql.DBSchema{},
	}

	// Phase 1: Collect all table names for each requested schema (fast, no metadata fetching)
	type datasetInfo struct {
		name       string
		tableNames []string
	}
	datasets := make([]datasetInfo, 0, len(schemas))

	for _, schemaName := range schemas {
		ds := d.client.Dataset(schemaName)
		var tableNames []string

		tables := ds.Tables(ctx)
		for {
			t, err := tables.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list tables in dataset %s: %w", schemaName, err)
			}
			tableNames = append(tableNames, t.TableID)
		}

		datasets = append(datasets, datasetInfo{name: schemaName, tableNames: tableNames})
	}

	// Phase 2: For each dataset, determine which tables need metadata (skip older shards)
	type tableJob struct {
		datasetName string
		actualTable string
		displayName string
		schemaIndex int
	}
	var jobs []tableJob

	for i, ds := range datasets {
		schema := &ansisql.DBSchema{
			Name:   ds.name,
			Tables: []*ansisql.DBTable{},
		}
		summary.Schemas = append(summary.Schemas, schema)

		// Select only tables that need metadata (non-sharded + most recent shard per group)
		tablesToFetch := selectTablesToFetch(ds.tableNames)
		for _, tf := range tablesToFetch {
			jobs = append(jobs, tableJob{
				datasetName: ds.name,
				actualTable: tf.actualName,
				displayName: tf.displayName,
				schemaIndex: i,
			})
		}
	}

	// Phase 3: Fetch all table metadata in parallel
	mu := sync.Mutex{}
	var errs []error

	workers := max(runtime.NumCPU(), 8)
	p := pool.New().WithMaxGoroutines(workers)

	for _, job := range jobs {
		p.Go(func() {
			metadata, err := d.getTableMetadata(ctx, job.datasetName, job.actualTable)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("failed to get metadata for table %s.%s: %w", job.datasetName, job.actualTable, err))
				mu.Unlock()
				return
			}

			table := &ansisql.DBTable{
				Name:           job.displayName,
				Type:           metadata.TableType,
				ViewDefinition: metadata.ViewDefinition,
				Columns:        metadata.Columns,
				CreatedAt:      metadata.CreatedAt,
				LastModified:   metadata.LastModified,
				RowCount:       metadata.RowCount,
				SizeBytes:      metadata.SizeBytes,
				Description:    metadata.Description,
			}

			mu.Lock()
			summary.Schemas[job.schemaIndex].Tables = append(summary.Schemas[job.schemaIndex].Tables, table)
			mu.Unlock()
		})
	}

	p.Wait()

	if len(errs) > 0 {
		return nil, errs[0]
	}

	// Sort tables within each schema and sort schemas
	for _, schema := range summary.Schemas {
		sort.Slice(schema.Tables, func(i, j int) bool { return schema.Tables[i].Name < schema.Tables[j].Name })
	}
	sort.Slice(summary.Schemas, func(i, j int) bool { return summary.Schemas[i].Name < summary.Schemas[j].Name })

	return summary, nil
}

// shardedTablePattern matches BigQuery sharded table naming convention: tablename_YYYYMMDD.
var shardedTablePattern = regexp.MustCompile(`^(.+)_(\d{8})$`)

// isShardedTableName checks if a table name follows the BigQuery sharded table pattern (_YYYYMMDD suffix).
func isShardedTableName(tableName string) bool {
	return shardedTablePattern.MatchString(tableName)
}

// getShardedTableBaseName extracts the base table name from a sharded table
// For "events_20240115", returns "events". For non-sharded tables, returns the original name.
func getShardedTableBaseName(tableName string) string {
	matches := shardedTablePattern.FindStringSubmatch(tableName)
	if matches == nil {
		return tableName
	}
	return matches[1]
}

// getShardDate extracts the date suffix from a sharded table name
// For "events_20240115", returns "20240115". For non-sharded tables, returns empty string.
func getShardDate(tableName string) string {
	matches := shardedTablePattern.FindStringSubmatch(tableName)
	if matches == nil {
		return ""
	}
	return matches[2]
}

// tableToFetch represents a table that needs metadata fetching.
type tableToFetch struct {
	actualName  string // The actual table name in BigQuery (e.g., "events_20240115")
	displayName string // The name to use in output (e.g., "events" for sharded, or same as actual for non-sharded)
}

// selectTablesToFetch analyzes table names and returns only the tables that need metadata fetching.
// For sharded tables, only the most recent shard is included. This optimization reduces API calls
// by skipping metadata fetches for older shards that would be discarded anyway.
func selectTablesToFetch(tableNames []string) []tableToFetch {
	if len(tableNames) == 0 {
		return nil
	}

	// Build set of non-sharded table names first
	nonShardedNames := make(map[string]bool)
	result := make([]tableToFetch, 0, len(tableNames))

	for _, name := range tableNames {
		if !isShardedTableName(name) {
			nonShardedNames[name] = true
			result = append(result, tableToFetch{actualName: name, displayName: name})
		}
	}

	// Track most recent shard for each base name
	shardGroups := make(map[string]string) // base name -> most recent actual table name
	shardDates := make(map[string]string)  // base name -> most recent date

	for _, name := range tableNames {
		if !isShardedTableName(name) {
			continue
		}

		baseName := getShardedTableBaseName(name)

		// Skip if a non-sharded table with the same name exists
		if nonShardedNames[baseName] {
			continue
		}

		shardDate := getShardDate(name)

		// Keep the most recent shard
		if existingDate, exists := shardDates[baseName]; !exists || shardDate > existingDate {
			shardDates[baseName] = shardDate
			shardGroups[baseName] = name
		}
	}

	// Add sharded tables (only most recent shard of each group)
	for baseName, actualName := range shardGroups {
		result = append(result, tableToFetch{actualName: actualName, displayName: baseName})
	}

	return result
}
