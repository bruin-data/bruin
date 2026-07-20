// Package iceberg builds the ingestr destination URI for Apache Iceberg
// (iceberg+<catalog>://<location>?storage=s3&...), a write-only destination.
package iceberg

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
)

var supportedCatalogTypes = []config.IcebergCatalogType{
	config.IcebergCatalogGlue,
	config.IcebergCatalogSQLite,
	config.IcebergCatalogPostgres,
	config.IcebergCatalogREST,
	config.IcebergCatalogHive,
	config.IcebergCatalogHadoop,
	config.IcebergCatalogSQL,
}

type Config struct {
	Catalog config.IcebergCatalog
	Storage config.IcebergStorage

	// CatalogName is the logical catalog identifier used by the Iceberg client.
	// ingestr defaults it to "ingestr" when empty.
	CatalogName string

	// Table/namespace write options.
	CreateNamespace *bool
	TableLocation   string
	TablePath       string
	// TableProperties are emitted as table.<key>=<value> (Iceberg table properties).
	TableProperties map[string]string
	// Properties is a passthrough for non-secret ingestr Iceberg URI parameters.
	// Applied last, so it wins on conflict. Its values are NOT masked in logs,
	// so credentials belong in the dedicated fields, never here.
	Properties map[string]string
}

// GetIngestrURI builds the ingestr Iceberg destination URI
// (iceberg+<catalog>://<location>?storage=s3&...).
func (c Config) GetIngestrURI() (string, error) {
	base, catalogParams, err := icebergCatalogURI(c.Catalog)
	if err != nil {
		return "", err
	}

	q, err := icebergStorageParams(c.Storage)
	if err != nil {
		return "", err
	}

	// Storage settings take precedence; catalog settings fill any gaps (ingestr
	// aliases region/credentials into both the s3.* and glue.* namespaces).
	for key, values := range catalogParams {
		if _, exists := q[key]; !exists {
			q[key] = values
		}
	}

	if name := strings.TrimSpace(c.CatalogName); name != "" {
		q.Set("catalog_name", name)
	}
	if c.CreateNamespace != nil {
		q.Set("create_namespace", strconv.FormatBool(*c.CreateNamespace))
	}
	if v := strings.TrimSpace(c.TableLocation); v != "" {
		q.Set("table_location", v)
	}
	if v := strings.TrimSpace(c.TablePath); v != "" {
		q.Set("table_path", v)
	}
	for _, k := range sortedKeys(c.TableProperties) {
		q.Set("table."+k, c.TableProperties[k])
	}

	// Passthrough wins on conflict so users can override anything for full parity.
	for _, k := range sortedKeys(c.Properties) {
		q.Set(k, c.Properties[k])
	}

	return base + "?" + q.Encode(), nil
}

// icebergCatalogURI returns the "iceberg+<catalog>://<authority>" base and the
// catalog-specific params. Add a new catalog backend by adding a case here.
func icebergCatalogURI(cat config.IcebergCatalog) (string, url.Values, error) {
	q := url.Values{}
	switch cat.Type {
	case config.IcebergCatalogGlue:
		setAWSCredentials(q, cat.Region, cat.Auth.AccessKey, cat.Auth.SecretKey, cat.Auth.SessionToken)
		if cat.CatalogID != "" {
			q.Set("glue.id", cat.CatalogID)
		}
		return "iceberg+glue://", q, nil
	case config.IcebergCatalogSQLite:
		if cat.Path == "" {
			return "", nil, fmt.Errorf("iceberg: sqlite catalog requires %q", "path")
		}
		return "iceberg+sqlite://" + ensureLeadingSlash(cat.Path), q, nil
	case config.IcebergCatalogPostgres:
		if cat.Host == "" {
			return "", nil, fmt.Errorf("iceberg: postgres catalog requires %q", "host")
		}
		return "iceberg+postgres://" + postgresAuthority(cat), q, nil
	case config.IcebergCatalogREST:
		if cat.Host == "" {
			return "", nil, fmt.Errorf("iceberg: rest catalog requires %q", "host")
		}
		if cat.Credential != "" {
			q.Set("credential", cat.Credential)
		}
		if cat.Token != "" {
			q.Set("token", cat.Token)
		}
		return "iceberg+rest://" + hostPort(cat.Host, cat.Port), q, nil
	case config.IcebergCatalogHive:
		if cat.Host == "" {
			return "", nil, fmt.Errorf("iceberg: hive catalog requires %q", "host")
		}
		return "iceberg+hive://" + hostPort(cat.Host, cat.Port), q, nil
	case config.IcebergCatalogHadoop:
		if cat.Path == "" {
			return "", nil, fmt.Errorf("iceberg: hadoop catalog requires %q (warehouse directory)", "path")
		}
		return "iceberg+hadoop://" + ensureLeadingSlash(cat.Path), q, nil
	case config.IcebergCatalogSQL:
		// Advanced SQL catalog; the connection string comes from the sensitive uri field.
		if cat.URI == "" {
			return "", nil, fmt.Errorf("iceberg: sql catalog requires %q (catalog connection string)", "uri")
		}
		q.Set("uri", cat.URI)
		return "iceberg+sql://", q, nil
	case "":
		return "", nil, fmt.Errorf("iceberg: catalog.type must be provided (supported: %s)", supportedCatalogList())
	default:
		return "", nil, fmt.Errorf("iceberg: unsupported catalog type %q (supported: %s)", cat.Type, supportedCatalogList())
	}
}

// icebergStorageParams maps a storage backend to its ingestr Iceberg params.
// ingestr is S3-only today; add a case (e.g. GCS) when it gains support.
func icebergStorageParams(st config.IcebergStorage) (url.Values, error) {
	q := url.Values{}
	switch st.Type {
	case config.IcebergStorageS3:
		q.Set("storage", "s3")
		// The warehouse can be given as a full s3:// URI (path) or as a separate
		// bucket (+ optional prefix); ingestr builds the same warehouse from either.
		switch {
		case st.Path != "" && st.Bucket != "":
			return nil, fmt.Errorf("iceberg: storage: set either %q (a full s3:// warehouse) or %q, not both", "path", "bucket")
		case st.Prefix != "" && st.Bucket == "":
			return nil, fmt.Errorf("iceberg: storage: %q requires %q", "prefix", "bucket")
		case st.Path != "":
			q.Set("warehouse", st.Path)
		case st.Bucket != "":
			q.Set("bucket", st.Bucket)
			if st.Prefix != "" {
				q.Set("prefix", st.Prefix)
			}
		}
		if st.Endpoint != "" {
			q.Set("endpoint", st.Endpoint)
		}
		if st.UseSSL != nil {
			q.Set("use_ssl", strconv.FormatBool(*st.UseSSL))
		}
		setAWSCredentials(q, st.Region, st.Auth.AccessKey, st.Auth.SecretKey, st.Auth.SessionToken)
		return q, nil
	case "":
		return nil, fmt.Errorf("iceberg: storage.type must be provided (supported: %s)", config.StorageTypeS3)
	default:
		// e.g. StorageTypeGCS: not supported by ingestr's Iceberg destination yet.
		return nil, fmt.Errorf("iceberg: unsupported storage type %q (supported: %s)", st.Type, config.StorageTypeS3)
	}
}

// setAWSCredentials sets the shared S3/Glue region and credential parameters that
// ingestr aliases into both the s3.* and glue.* namespaces.
func setAWSCredentials(q url.Values, region, accessKey, secretKey, sessionToken string) {
	if region != "" {
		q.Set("region", region)
	}
	if accessKey != "" {
		q.Set("access_key_id", accessKey)
	}
	if secretKey != "" {
		q.Set("secret_access_key", secretKey)
	}
	if sessionToken != "" {
		q.Set("session_token", sessionToken)
	}
}

// postgresAuthority builds the "user:pass@host:port/database" portion of a
// postgres-catalog Iceberg URI.
func postgresAuthority(cat config.IcebergCatalog) string {
	var b strings.Builder
	if cat.Auth.Username != "" {
		if cat.Auth.Password != "" {
			b.WriteString(url.UserPassword(cat.Auth.Username, cat.Auth.Password).String())
		} else {
			b.WriteString(url.User(cat.Auth.Username).String())
		}
		b.WriteString("@")
	}
	b.WriteString(hostPort(cat.Host, cat.Port))
	if cat.Database != "" {
		b.WriteString("/" + strings.TrimPrefix(cat.Database, "/"))
	}
	return b.String()
}

func hostPort(host string, port int) string {
	if port != 0 {
		return fmt.Sprintf("%s:%d", host, port)
	}
	return host
}

func ensureLeadingSlash(path string) string {
	if strings.HasPrefix(path, "/") {
		return path
	}
	return "/" + path
}

func supportedCatalogList() string {
	parts := make([]string, len(supportedCatalogTypes))
	for i, t := range supportedCatalogTypes {
		parts[i] = string(t)
	}
	return strings.Join(parts, ", ")
}

func sortedKeys(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
