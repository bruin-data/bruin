package duck

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
)

// Validates the lakehouse config for DuckDB-specific requirements
func ValidateLakehouseConfig(lh *config.LakehouseConfig) error {
	if lh == nil {
		return nil
	}
	if err := lh.Validate(); err != nil {
		return err
	}
	if lh.Format == config.LakehouseFormatIceberg {
		return validateIcebergForDuckDB(lh)
	}

	return fmt.Errorf("DuckDB does not support lakehouse format: %s", lh.Format)
}

func validateIcebergForDuckDB(lh *config.LakehouseConfig) error {
	if lh.Catalog == nil {
		return errors.New("DuckDB iceberg requires catalog configuration")
	}

	if lh.Catalog.Type == "" {
		return errors.New("DuckDB iceberg requires catalog type")
	}

	// supported catalog types and requirements
	switch lh.Catalog.Type {
	case config.CatalogTypeGlue:
		if lh.Catalog.CatalogID == "" {
			return errors.New("DuckDB iceberg with glue catalog requires catalog_id")
		}
	default:
		return fmt.Errorf("DuckDB iceberg does not support catalog type: %s (supported: glue)", lh.Catalog.Type)
	}

	return nil
}

type LakehouseAttacher struct{}

func NewLakehouseAttacher() *LakehouseAttacher {
	return &LakehouseAttacher{}
}

func (l *LakehouseAttacher) GenerateAttachStatements(lh *config.LakehouseConfig, alias string) ([]string, error) {
	if lh == nil {
		return nil, nil
	}

	extensions := l.getRequiredExtensions(lh)

	statements := make([]string, 0, len(extensions)*2+3)

	for _, ext := range extensions {
		statements = append(statements, "INSTALL "+ext)
		statements = append(statements, "LOAD "+ext)
	}

	secretStatements := l.generateSecretStatements(lh, alias)
	statements = append(statements, secretStatements...)

	attachStmt, err := l.generateAttach(lh, alias)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ATTACH statement: %w", err)
	}
	statements = append(statements, attachStmt)

	return statements, nil
}

func (l *LakehouseAttacher) getRequiredExtensions(lh *config.LakehouseConfig) []string {
	var extensions []string

	if lh.Format == config.LakehouseFormatIceberg {
		extensions = append(extensions, "iceberg")
	}

	if lh.Storage != nil && lh.Storage.Type == config.StorageTypeS3 {
		extensions = append(extensions, "aws")
		extensions = append(extensions, "httpfs")
	}

	if lh.Catalog != nil && lh.Catalog.Type == config.CatalogTypeGlue {
		if lh.Storage == nil || lh.Storage.Type != config.StorageTypeS3 {
			extensions = append(extensions, "aws")
		}
	}

	return l.deduplicateExtensions(extensions)
}

func (l *LakehouseAttacher) deduplicateExtensions(extensions []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(extensions))
	for _, ext := range extensions {
		if !seen[ext] {
			seen[ext] = true
			result = append(result, ext)
		}
	}
	return result
}

func (l *LakehouseAttacher) generateSecretStatements(lh *config.LakehouseConfig, alias string) []string {
	var statements []string

	// Storage
	if lh.Storage != nil && lh.Storage.Auth != nil && lh.Storage.Auth.IsS3() {
		storageSecret := l.generateS3Secret(l.storageSecretName(alias, lh.Storage), lh.Storage, l.storageScope(lh.Storage))
		if storageSecret != "" {
			statements = append(statements, storageSecret)
		}
	}

	// Catalog
	if lh.Catalog != nil && lh.Catalog.Auth != nil && lh.Catalog.Auth.IsAWS() {
		catalogSecret := l.generateCatalogSecret(l.catalogSecretName(alias, lh.Catalog), lh.Catalog)
		if catalogSecret != "" {
			statements = append(statements, catalogSecret)
		}
	}

	return statements
}

func (l *LakehouseAttacher) storageSecretName(alias string, storage *config.StorageConfig) string {
	if storage != nil && storage.SecretName != "" {
		return storage.SecretName
	}
	return defaultSecretName(alias, "storage")
}

func (l *LakehouseAttacher) catalogSecretName(alias string, catalog *config.CatalogConfig) string {
	if catalog != nil && catalog.SecretName != "" {
		return catalog.SecretName
	}
	return defaultSecretName(alias, "catalog")
}

func (l *LakehouseAttacher) storageScope(storage *config.StorageConfig) string {
	if storage == nil {
		return ""
	}
	if storage.Scope != "" {
		return storage.Scope
	}
	if strings.HasPrefix(storage.Location, "s3://") {
		return storage.Location
	}
	return ""
}

func (l *LakehouseAttacher) generateS3Secret(name string, storage *config.StorageConfig, scope string) string {
	auth := storage.Auth
	if auth.AccessKey == "" || auth.SecretKey == "" {
		return ""
	}

	var parts []string
	parts = append(parts, "CREATE OR REPLACE SECRET "+name+" (")
	parts = append(parts, "    TYPE s3")
	parts = append(parts, ",   PROVIDER config")
	parts = append(parts, ",   KEY_ID "+dollarQuote(auth.AccessKey))
	parts = append(parts, ",   SECRET "+dollarQuote(auth.SecretKey))

	if auth.SessionToken != "" {
		parts = append(parts, ",   SESSION_TOKEN "+dollarQuote(auth.SessionToken))
	}
	if storage.Region != "" {
		parts = append(parts, ",   REGION "+dollarQuote(storage.Region))
	}
	if scope != "" {
		parts = append(parts, ",   SCOPE "+dollarQuote(scope))
	}

	parts = append(parts, ")")
	return strings.Join(parts, "\n")
}

func (l *LakehouseAttacher) generateCatalogSecret(name string, catalog *config.CatalogConfig) string {
	auth := catalog.Auth
	if auth == nil || !auth.IsAWS() {
		return ""
	}

	// Glue auth
	var parts []string
	parts = append(parts, "CREATE OR REPLACE SECRET "+name+" (")
	parts = append(parts, "    TYPE s3")
	parts = append(parts, ",   PROVIDER config")
	parts = append(parts, ",   KEY_ID "+dollarQuote(auth.AccessKey))
	parts = append(parts, ",   SECRET "+dollarQuote(auth.SecretKey))
	if auth.SessionToken != "" {
		parts = append(parts, ",   SESSION_TOKEN "+dollarQuote(auth.SessionToken))
	}
	if catalog.Region != "" {
		parts = append(parts, ",   REGION "+dollarQuote(catalog.Region))
	}
	parts = append(parts, ")")
	return strings.Join(parts, "\n")
}

func defaultSecretName(alias string, kind string) string {
	base := "bruin_"
	if alias != "" {
		base += sanitizeIdentifier(alias) + "_"
	}
	return base + kind
}

func sanitizeIdentifier(input string) string {
	if input == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(input))
	for _, r := range input {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func (l *LakehouseAttacher) generateAttach(lh *config.LakehouseConfig, alias string) (string, error) {
	if lh.Format == config.LakehouseFormatIceberg {
		return l.generateIcebergAttach(lh, alias)
	}

	return "", fmt.Errorf("unsupported lakehouse format: %s", lh.Format)
}

func (l *LakehouseAttacher) generateIcebergAttach(lh *config.LakehouseConfig, alias string) (string, error) {
	if lh.Catalog == nil {
		return "", errors.New("iceberg format requires catalog configuration")
	}

	var options []string
	options = append(options, "TYPE 'iceberg'")

	switch lh.Catalog.Type {
	case config.CatalogTypeGlue:
		// ATTACH '{catalog_id}' AS alias (TYPE 'iceberg', ENDPOINT_TYPE 'glue')
		options = append(options, "ENDPOINT_TYPE 'glue'")
		return "ATTACH '" + escapeSQL(lh.Catalog.CatalogID) + "' AS " + alias + " (" + strings.Join(options, ", ") + ")", nil

	default:
		return "", fmt.Errorf("unsupported catalog type for iceberg: %s", lh.Catalog.Type)
	}
}

// escapeSQL escapes single quotes in SQL strings.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// dollarQuote wraps a string in single quotes with SQL escaping.
func dollarQuote(s string) string {
	return "'" + escapeSQL(s) + "'"
}
