// Package iceberg builds the ingestr destination URI for Apache Iceberg
// (iceberg+<catalog>://<location>?storage=s3&...), a write-only destination.
package iceberg

import (
	"fmt"
	"net"
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
	config.IcebergCatalogR2,
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

	// Storage settings take precedence; catalog settings fill any gaps.
	for key, values := range catalogParams {
		if _, exists := q[key]; !exists {
			q[key] = values
		}
	}

	// Each block's credentials go under its own namespace (glue.* and s3.*) rather
	// than the shared keys ingestr aliases into both: with one flat namespace it
	// cannot tell a storage key pair from a catalog one, and would hand MinIO's to
	// AWS Glue. Sharing is then explicit, and only between two AWS endpoints --
	// when just one side is configured, the operator gave a single AWS account and
	// means it for both.
	if c.Catalog.Type == config.IcebergCatalogGlue && isAWSStorage(c.Storage) {
		if !hasAWSCredentials(c.Storage.Auth) {
			region := c.Storage.Region
			if region == "" {
				region = c.Catalog.Region
			}
			setS3Credentials(q, region, c.Catalog.Auth.AccessKey, c.Catalog.Auth.SecretKey, c.Catalog.Auth.SessionToken)
		} else if !hasAWSCredentials(c.Catalog.Auth) {
			region := c.Catalog.Region
			if region == "" {
				region = c.Storage.Region
			}
			setGlueCredentials(q, region, c.Storage.Auth.AccessKey, c.Storage.Auth.SecretKey, c.Storage.Auth.SessionToken)
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
		setGlueCredentials(q, cat.Region, cat.Auth.AccessKey, cat.Auth.SecretKey, cat.Auth.SessionToken)
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
		// ingestr builds the REST endpoint as http:// unless rest_use_ssl is set;
		// managed catalogs (Polaris, Tabular, R2, Unity, ...) are served over TLS.
		if cat.RestUseSSL != nil {
			q.Set("rest_use_ssl", strconv.FormatBool(*cat.RestUseSSL))
		}
		if cat.Credential != "" {
			q.Set("credential", cat.Credential)
		}
		if cat.Token != "" {
			q.Set("token", cat.Token)
		}
		return "iceberg+rest://" + hostPort(cat.Host, cat.Port), q, nil
	case config.IcebergCatalogR2:
		// Cloudflare R2 Data Catalog: account id + bucket build the URI, and R2 vends
		// the S3 storage credentials through the catalog, so no storage block is needed.
		if cat.CatalogID == "" || cat.Database == "" || cat.Token == "" {
			return "", nil, fmt.Errorf("iceberg: r2 catalog requires %q (account id), %q (bucket), and %q (R2 API token)", "catalog_id", "database", "token")
		}
		q.Set("token", cat.Token)
		return "iceberg+r2://" + cat.CatalogID + "/" + cat.Database, q, nil
	case config.IcebergCatalogHive:
		if cat.Host == "" {
			return "", nil, fmt.Errorf("iceberg: hive catalog requires %q", "host")
		}
		return "iceberg+hive://" + hostPort(cat.Host, cat.Port), q, nil
	case config.IcebergCatalogHadoop:
		// A local warehouse goes in the URL path. A warehouse URI cannot: it would
		// produce iceberg+hadoop:///s3://bucket/wh, which reads back as the local
		// path "/s3:" and fails with "mkdir /s3:". Forward it as the warehouse
		// instead, where the storage block's own warehouse still takes precedence.
		if cat.Path != "" {
			if strings.Contains(cat.Path, "://") {
				q.Set("warehouse", cat.Path)

				return "iceberg+hadoop://", q, nil
			}

			return "iceberg+hadoop://" + ensureLeadingSlash(cat.Path), q, nil
		}
		return "iceberg+hadoop://", q, nil
	case config.IcebergCatalogSQL:
		// Advanced SQL catalog; the connection string comes from the sensitive uri field.
		if cat.URI == "" {
			return "", nil, fmt.Errorf("iceberg: sql catalog requires %q (catalog connection string)", "uri")
		}
		q.Set("uri", cat.URI)
		// ingestr's generic sql catalog needs both driver and dialect; settable here
		// or via properties. The sqlite/postgres catalog types set them automatically.
		if cat.Driver != "" {
			q.Set("sql.driver", cat.Driver)
		}
		if cat.Dialect != "" {
			q.Set("sql.dialect", cat.Dialect)
		}
		return "iceberg+sql://", q, nil
	case "":
		return "", nil, fmt.Errorf("iceberg: catalog.type must be provided (supported: %s)", supportedCatalogList())
	default:
		return "", nil, fmt.Errorf("iceberg: unsupported catalog type %q (supported: %s)", cat.Type, supportedCatalogList())
	}
}

// icebergStorageParams maps a storage backend to its ingestr Iceberg params.
func icebergStorageParams(st config.IcebergStorage) (url.Values, error) {
	q := url.Values{}

	switch st.Type {
	case "", config.IcebergStorageS3, config.IcebergStorageGCS, config.IcebergStorageLocal:
	default:
		return nil, fmt.Errorf("iceberg: unsupported storage type %q (supported: %s, %s, %s)", st.Type, config.IcebergStorageS3, config.IcebergStorageGCS, config.IcebergStorageLocal)
	}
	if st.Path != "" && st.Bucket != "" {
		return nil, fmt.Errorf("iceberg: storage: set either %q (a full warehouse URI) or %q, not both", "path", "bucket")
	}
	if st.Prefix != "" && st.Bucket == "" {
		return nil, fmt.Errorf("iceberg: storage: %q requires %q", "prefix", "bucket")
	}

	// storage tells ingestr which scheme to build for the bucket form. s3 is its
	// default, so only gcs needs declaring; a full path or local warehouse carries
	// its own scheme. Everything else is passed through and ingestr does the rest.
	if st.Type == config.IcebergStorageGCS {
		q.Set("storage", "gcs")
	}
	switch {
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
	setS3Credentials(q, st.Region, st.Auth.AccessKey, st.Auth.SecretKey, st.Auth.SessionToken)
	if st.KeyFile != "" {
		q.Set("gcs.keypath", st.KeyFile)
	}
	if st.KeyJSON != "" {
		q.Set("gcs.jsonkey", st.KeyJSON)
	}
	return q, nil
}

// isAWSStorage reports whether the storage is AWS S3 itself rather than an
// S3-compatible service. Regional, VPC, FIPS and dualstack endpoints all live
// under amazonaws.com; MinIO, R2 and GCS interop do not.
func isAWSStorage(st config.IcebergStorage) bool {
	if st.Type == config.IcebergStorageGCS || st.Type == config.IcebergStorageLocal {
		return false
	}
	if st.Endpoint == "" {
		return true
	}

	host := st.Endpoint
	if idx := strings.Index(host, "://"); idx >= 0 {
		host = host[idx+3:]
	}
	host, _, _ = strings.Cut(host, "/")
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))

	return host == "amazonaws.com" ||
		strings.HasSuffix(host, ".amazonaws.com") ||
		strings.HasSuffix(host, ".amazonaws.com.cn")
}

// hasAWSCredentials reports whether an auth block carries an AWS key pair.
func hasAWSCredentials(auth config.IcebergAuth) bool {
	return auth.AccessKey != "" || auth.SecretKey != "" || auth.SessionToken != ""
}

// setS3Credentials sets the s3.*-namespaced region and credential parameters, so
// they configure the storage client and nothing else.
func setS3Credentials(q url.Values, region, accessKey, secretKey, sessionToken string) {
	if region != "" {
		q.Set("s3.region", region)
	}
	if accessKey != "" {
		q.Set("s3.access-key-id", accessKey)
	}
	if secretKey != "" {
		q.Set("s3.secret-access-key", secretKey)
	}
	if sessionToken != "" {
		q.Set("s3.session-token", sessionToken)
	}
}

// setGlueCredentials sets the glue.*-namespaced region and credential parameters
// so the catalog keeps its own AWS account regardless of what storage sets.
func setGlueCredentials(q url.Values, region, accessKey, secretKey, sessionToken string) {
	if region != "" {
		q.Set("glue.region", region)
	}
	if accessKey != "" {
		q.Set("glue.access-key-id", accessKey)
	}
	if secretKey != "" {
		q.Set("glue.secret-access-key", secretKey)
	}
	if sessionToken != "" {
		q.Set("glue.session-token", sessionToken)
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
