package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// RelationExists reports whether a table, view or any other relation is named
// tableName, so an incremental asset can create its table on the first run.
//
// Materializations write the asset name as it is, so dataset.table resolves in
// the connection's project and project.dataset.table in the named project. The
// lookup uses the tables.get API, which is not billed, unlike an
// INFORMATION_SCHEMA query, which is billed and fails on a missing dataset.
//
// tables.get compares names exactly, but a dataset with case-insensitive names
// resolves both its own name and its table names in any case, so a query for
// analytics.orders writes to an existing Analytics.Orders. When the exact name
// is not found, the table only counts as missing once no case-insensitive
// dataset holds it under another case.
func (d *Client) RelationExists(ctx context.Context, tableName string) (bool, error) {
	projectID, datasetID, tableID, err := d.relationRef(tableName)
	if err != nil {
		return false, err
	}
	if err := d.ensureClientInitialized(ctx); err != nil {
		return false, err
	}

	dataset := d.client.DatasetInProject(projectID, datasetID)
	_, err = dataset.Table(tableID).Metadata(ctx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
	if err == nil {
		return true, nil
	}
	if !isNotFound(err) {
		return false, fmt.Errorf("failed to check whether table '%s' exists: %w", tableName, err)
	}

	meta, err := dataset.Metadata(ctx)
	if err == nil {
		if !meta.IsCaseInsensitive {
			return false, nil
		}
		return hasTableIgnoringCase(ctx, dataset, tableID)
	}
	if !isNotFound(err) {
		return false, fmt.Errorf("failed to check whether dataset '%s.%s' exists: %w", projectID, datasetID, err)
	}

	return d.caseInsensitiveDatasetHasTable(ctx, projectID, datasetID, tableID)
}

// relationRef splits an asset name into the project, dataset and table the
// materialized query writes to, like getTableRef. Project IDs are always
// lower-case, so the project is looked up in lower case.
func (d *Client) relationRef(tableName string) (string, string, string, error) {
	invalid := fmt.Errorf("table name must be in dataset.table or project.dataset.table format, '%s' given", tableName)
	parts := strings.Split(tableName, ".")
	for _, part := range parts {
		// The lookup would not see the name the query resolves through backticks or surrounding spaces.
		if part == "" || strings.TrimSpace(part) != part || strings.Contains(part, "`") {
			return "", "", "", invalid
		}
	}

	var projectID, datasetID, tableID string
	switch len(parts) {
	case 2:
		projectID, datasetID, tableID = d.config.ProjectID, parts[0], parts[1]
	case 3:
		projectID, datasetID, tableID = parts[0], parts[1], parts[2]
	default:
		return "", "", "", invalid
	}
	if projectID == "" {
		return "", "", "", fmt.Errorf("cannot resolve table '%s': the connection has no project_id", tableName)
	}

	return strings.ToLower(projectID), datasetID, tableID, nil
}

// caseInsensitiveDatasetHasTable reports whether a case-insensitive dataset
// whose name differs from datasetID only in case holds the table.
func (d *Client) caseInsensitiveDatasetHasTable(ctx context.Context, projectID, datasetID, tableID string) (bool, error) {
	datasets := d.client.Datasets(ctx)
	datasets.ProjectID = projectID
	datasets.ListHidden = true
	for {
		dataset, err := datasets.Next()
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to list the datasets in project '%s': %w", projectID, err)
		}
		if !strings.EqualFold(dataset.DatasetID, datasetID) {
			continue
		}

		meta, err := dataset.Metadata(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to read dataset '%s.%s': %w", projectID, dataset.DatasetID, err)
		}
		if !meta.IsCaseInsensitive {
			continue
		}
		if found, err := hasTableIgnoringCase(ctx, dataset, tableID); err != nil || found {
			return found, err
		}
	}
}

// hasTableIgnoringCase reports whether the dataset holds a table, view or other
// relation named tableID in any case.
func hasTableIgnoringCase(ctx context.Context, dataset *bigquery.Dataset, tableID string) (bool, error) {
	tables := dataset.Tables(ctx)
	for {
		table, err := tables.Next()
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to list the tables in dataset '%s.%s': %w", dataset.ProjectID, dataset.DatasetID, err)
		}
		if strings.EqualFold(table.TableID, tableID) {
			return true, nil
		}
	}
}

func isNotFound(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusNotFound
}
