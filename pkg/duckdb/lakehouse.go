package duck

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
)

var ducklakeSupportedCatalogTypes = []config.CatalogType{
	config.CatalogTypePostgres,
	config.CatalogTypeDuckDB,
	config.CatalogTypeSQLite,
}

// Validates the lakehouse config for DuckDB-specific requirements.
func ValidateLakehouseConfig(lh *config.LakehouseConfig) error {
	if lh == nil {
		return nil
	}
	if err := lh.Validate(); err != nil {
		return err
	}

	switch lh.Format {
	case config.LakehouseFormatIceberg:
		return validateIcebergForDuckDB(*lh)
	case config.LakehouseFormatDuckLake:
		return validateDuckLakeForDuckDB(*lh)
	default:
		return fmt.Errorf("DuckDB does not support lakehouse format: %s", lh.Format)
	}
}

func validateIcebergForDuckDB(lh config.LakehouseConfig) error {
	if lh.Catalog.Type != config.CatalogTypeGlue {
		return fmt.Errorf("DuckDB iceberg does not support catalog type: '%s' (supported: glue)", lh.Catalog.Type)
	}

	if lh.Catalog.CatalogID == "" {
		return errors.New("DuckDB iceberg with glue catalog requires catalog_id")
	}
	if !lh.Catalog.Auth.IsAWS() {
		return errors.New("DuckDB iceberg with glue catalog requires access_key and secret_key")
	}

	if err := validateS3StorageForDuckDB(lh.Storage, "DuckDB iceberg"); err != nil {
		return err
	}

	return nil
}

func validateDuckLakeForDuckDB(lh config.LakehouseConfig) error {
	if !slices.Contains(ducklakeSupportedCatalogTypes, lh.Catalog.Type) {
		return fmt.Errorf("DuckDB ducklake does not support catalog type: '%s' (supported: postgres, duckdb, sqlite)", lh.Catalog.Type)
	}

	switch lh.Catalog.Type {
	case config.CatalogTypePostgres:
		if lh.Catalog.Host == "" {
			return errors.New("DuckDB ducklake with postgres catalog requires host")
		}
		if lh.Catalog.Database == "" {
			return errors.New("DuckDB ducklake with postgres catalog requires database")
		}
		if !lh.Catalog.Auth.IsPostgres() {
			return errors.New("DuckDB ducklake with postgres catalog requires username and password")
		}
	case config.CatalogTypeDuckDB, config.CatalogTypeSQLite:
		if lh.Catalog.Path == "" {
			return fmt.Errorf("DuckDB ducklake with %s catalog requires path", lh.Catalog.Type)
		}
	case config.CatalogTypeGlue:
		return fmt.Errorf("DuckDB ducklake does not support catalog type: '%s' (supported: postgres, duckdb, sqlite)", lh.Catalog.Type)
	}

	if err := validateS3StorageForDuckDB(lh.Storage, "DuckDB ducklake"); err != nil {
		return err
	}

	return nil
}

func validateS3StorageForDuckDB(storage config.StorageConfig, formatCtx string) error {
	if storage.Type != config.StorageTypeS3 {
		return fmt.Errorf("%s does not support storage type: %s (supported: s3)", formatCtx, storage.Type)
	}
	if storage.Path == "" {
		return fmt.Errorf("%s with s3 storage requires path", formatCtx)
	}
	if !storage.Auth.IsS3() {
		return fmt.Errorf("%s with s3 storage requires access_key and secret_key", formatCtx)
	}

	return nil
}

type LakehouseAttacher struct{}

func NewLakehouseAttacher() *LakehouseAttacher {
	return &LakehouseAttacher{}
}

type icebergAttachBuilder func(catalog config.CatalogConfig, alias string) (string, error)

var icebergAttachBuilders = map[config.CatalogType]icebergAttachBuilder{
	config.CatalogTypeGlue: buildIcebergGlueAttach,
}

func (l *LakehouseAttacher) GenerateAttachStatements(lh *config.LakehouseConfig, alias string) ([]string, error) {
	if lh == nil {
		return nil, nil
	}

	extensions := l.getRequiredExtensions(*lh)
	statements := make([]string, 0, len(extensions)*2+3)

	for _, ext := range extensions {
		statements = append(statements, "INSTALL "+ext)
		statements = append(statements, "LOAD "+ext)
	}

	secretStatements := l.generateSecretStatements(*lh, alias)
	statements = append(statements, secretStatements...)

	attachStmt, err := l.generateAttach(*lh, alias)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ATTACH statement: %w", err)
	}
	statements = append(statements, attachStmt)
	statements = append(statements, "CREATE SCHEMA IF NOT EXISTS "+alias+".main")
	statements = append(statements, "USE "+alias)

	return statements, nil
}

func (l *LakehouseAttacher) getRequiredExtensions(lh config.LakehouseConfig) []string {
	var extensions []string

	if lh.Format == config.LakehouseFormatIceberg {
		extensions = append(extensions, "iceberg")
	}
	if lh.Format == config.LakehouseFormatDuckLake {
		extensions = append(extensions, "ducklake")
	}

	if lh.Storage.Type == config.StorageTypeS3 {
		extensions = append(extensions, "aws", "httpfs")
	}

	switch lh.Catalog.Type {
	case config.CatalogTypeGlue:
		extensions = append(extensions, "aws")
	case config.CatalogTypePostgres:
		extensions = append(extensions, "postgres")
	case config.CatalogTypeSQLite:
		extensions = append(extensions, "sqlite")
	case config.CatalogTypeDuckDB:
		// no extension required
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

func (l *LakehouseAttacher) generateSecretStatements(lh config.LakehouseConfig, alias string) []string {
	var statements []string

	if lh.Storage.Auth.IsS3() {
		storageSecret := l.generateS3Secret(defaultSecretName(alias, "storage"), lh.Storage)
		if storageSecret != "" {
			statements = append(statements, storageSecret)
		}
	}

	catalogSecret := l.generateCatalogSecret(defaultSecretName(alias, "catalog"), lh.Catalog)
	if catalogSecret != "" {
		statements = append(statements, catalogSecret)
	}

	return statements
}

func (l *LakehouseAttacher) generateS3Secret(name string, storage config.StorageConfig) string {
	auth := storage.Auth
	if !auth.IsS3() {
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
	scope := "s3://"
	if storage.Path != "" {
		scope = storage.Path
	}
	parts = append(parts, ",   SCOPE "+dollarQuote(scope))

	parts = append(parts, ")")
	return strings.Join(parts, "\n")
}

func (l *LakehouseAttacher) generateCatalogSecret(name string, catalog config.CatalogConfig) string {
	switch catalog.Type {
	case config.CatalogTypeGlue:
		return l.generateGlueSecret(name, catalog)
	case config.CatalogTypePostgres:
		return l.generatePostgresSecret(name, catalog)
	case config.CatalogTypeDuckDB, config.CatalogTypeSQLite:
		return ""
	default:
		return ""
	}
}

func (l *LakehouseAttacher) generateGlueSecret(name string, catalog config.CatalogConfig) string {
	auth := catalog.Auth
	if !auth.IsAWS() {
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
	if catalog.Region != "" {
		parts = append(parts, ",   REGION "+dollarQuote(catalog.Region))
	}
	parts = append(parts, ")")
	return strings.Join(parts, "\n")
}

func (l *LakehouseAttacher) generatePostgresSecret(name string, catalog config.CatalogConfig) string {
	auth := catalog.Auth
	if !auth.IsPostgres() || catalog.Host == "" || catalog.Database == "" {
		return ""
	}

	port := catalog.Port
	if port == 0 {
		port = 5432
	}

	var parts []string
	parts = append(parts, "CREATE OR REPLACE SECRET "+name+" (")
	parts = append(parts, "    TYPE postgres")
	parts = append(parts, ",   HOST "+dollarQuote(catalog.Host))
	parts = append(parts, ",   PORT "+strconv.Itoa(port))
	parts = append(parts, ",   DATABASE "+dollarQuote(catalog.Database))
	parts = append(parts, ",   USER "+dollarQuote(auth.Username))
	parts = append(parts, ",   PASSWORD "+dollarQuote(auth.Password))
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

func (l *LakehouseAttacher) generateAttach(lh config.LakehouseConfig, alias string) (string, error) {
	switch lh.Format {
	case config.LakehouseFormatIceberg:
		return l.generateIcebergAttach(lh, alias)
	case config.LakehouseFormatDuckLake:
		return l.generateDuckLakeAttach(lh, alias)
	default:
		return "", fmt.Errorf("unsupported lakehouse format: %s", lh.Format)
	}
}

func (l *LakehouseAttacher) generateIcebergAttach(lh config.LakehouseConfig, alias string) (string, error) {
	buildAttach, ok := icebergAttachBuilders[lh.Catalog.Type]
	if !ok {
		return "", fmt.Errorf("unsupported catalog type for iceberg: %s", lh.Catalog.Type)
	}

	return buildAttach(lh.Catalog, alias)
}

func buildIcebergGlueAttach(catalog config.CatalogConfig, alias string) (string, error) {
	options := []string{"TYPE 'iceberg'", "ENDPOINT_TYPE 'glue'"}
	return "ATTACH " + dollarQuote(catalog.CatalogID) + " AS " + alias + " (" + strings.Join(options, ", ") + ")", nil
}

func (l *LakehouseAttacher) generateDuckLakeAttach(lh config.LakehouseConfig, alias string) (string, error) {
	switch lh.Catalog.Type {
	case config.CatalogTypePostgres:
		secretName := defaultSecretName(alias, "catalog")
		options := []string{
			"DATA_PATH " + dollarQuote(lh.Storage.Path),
			"META_SECRET " + dollarQuote(secretName),
			"OVERRIDE_DATA_PATH true",
		}
		return "ATTACH 'ducklake:postgres:' AS " + alias + " (" + strings.Join(options, ", ") + ")", nil
	case config.CatalogTypeDuckDB:
		catalogPath := strings.TrimSpace(lh.Catalog.Path)
		if catalogPath == "" {
			return "", errors.New("ducklake format requires catalog path")
		}
		catalogPath = strings.TrimPrefix(catalogPath, "ducklake:")
		options := []string{
			"DATA_PATH " + dollarQuote(lh.Storage.Path),
			"OVERRIDE_DATA_PATH true",
		}
		return "ATTACH 'ducklake:" + escapeSQL(catalogPath) + "' AS " + alias + " (" + strings.Join(options, ", ") + ")", nil
	case config.CatalogTypeSQLite:
		catalogPath := strings.TrimSpace(lh.Catalog.Path)
		if catalogPath == "" {
			return "", errors.New("ducklake format requires catalog path")
		}
		catalogPath = strings.TrimPrefix(catalogPath, "ducklake:")
		catalogPath = strings.TrimPrefix(catalogPath, "sqlite:")
		options := []string{
			"DATA_PATH " + dollarQuote(lh.Storage.Path),
			"OVERRIDE_DATA_PATH true",
		}
		return "ATTACH 'ducklake:sqlite:" + escapeSQL(catalogPath) + "' AS " + alias + " (" + strings.Join(options, ", ") + ")", nil
	case config.CatalogTypeGlue:
		return "", fmt.Errorf("unsupported catalog type for ducklake: %s", lh.Catalog.Type)
	default:
		return "", fmt.Errorf("unsupported catalog type for ducklake: %s", lh.Catalog.Type)
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
