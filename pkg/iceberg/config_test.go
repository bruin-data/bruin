package iceberg

import (
	"net/url"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAWSRegion  = "us-east-1"
	testS3Path     = "s3://b/p"
	testSQLitePath = "/c.db"
	testS3LakeWh   = "s3://lake/wh"
)

func parseQuery(t *testing.T, raw string) (string, url.Values) {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u.Scheme, u.Query()
}

func TestConfig_GetIngestrURI_GlueS3(t *testing.T) {
	t.Parallel()

	c := Config{
		CatalogName: "analytics",
		Catalog: config.IcebergCatalog{
			Type:      config.IcebergCatalogGlue,
			CatalogID: "123456789012",
			Region:    testAWSRegion,
			Auth:      config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
		Storage: config.IcebergStorage{
			Type: config.IcebergStorageS3,
			Path: "s3://company-lake/warehouse",
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+glue", scheme)
	assert.Empty(t, q.Get("storage"), "storage flag is no longer emitted; the warehouse scheme carries the backend")
	assert.Equal(t, "s3://company-lake/warehouse", q.Get("warehouse"))
	assert.Equal(t, testAWSRegion, q.Get("glue.region"))
	assert.Equal(t, "AKID", q.Get("glue.access-key-id"))
	assert.Equal(t, "SECRET", q.Get("glue.secret-access-key"))
	// real S3 and only the catalog configured, so the storage inherits it
	assert.Equal(t, "AKID", q.Get("s3.access-key-id"))
	assert.Equal(t, "123456789012", q.Get("glue.id"))
	assert.Equal(t, "analytics", q.Get("catalog_name"))
}

func TestConfig_GetIngestrURI_GlueOnS3CompatibleStorage(t *testing.T) {
	t.Parallel()

	// Glue on S3-compatible storage: the two use different accounts, so the
	// catalog's AWS settings must survive the storage block.
	useSSL := true
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:   config.IcebergCatalogGlue,
			Region: "eu-north-1",
			Auth:   config.IcebergAuth{AccessKey: "ASIAKID", SecretKey: "AWSSECRET", SessionToken: "AWSTOKEN"},
		},
		Storage: config.IcebergStorage{
			Type:     config.IcebergStorageS3,
			Path:     testS3LakeWh,
			Endpoint: "storage.googleapis.com",
			UseSSL:   &useSSL,
			Region:   "auto",
			Auth:     config.IcebergAuth{AccessKey: "GOOGKID", SecretKey: "GOOGSECRET"},
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "eu-north-1", q.Get("glue.region"))
	assert.Equal(t, "ASIAKID", q.Get("glue.access-key-id"))
	assert.Equal(t, "AWSSECRET", q.Get("glue.secret-access-key"))
	assert.Equal(t, "AWSTOKEN", q.Get("glue.session-token"))

	// The storage has an endpoint, so its credentials are pinned to s3.*.
	assert.Equal(t, "auto", q.Get("s3.region"))
	assert.Equal(t, "GOOGKID", q.Get("s3.access-key-id"))
	assert.Equal(t, "GOOGSECRET", q.Get("s3.secret-access-key"))
	assert.Empty(t, q.Get("access_key_id"), "GCS interop keys must never reach the shared params")
}

func TestConfig_GetIngestrURI_StorageKeepsOwnCredentials(t *testing.T) {
	t.Parallel()

	// Storage with its own key pair and no session token must not inherit the
	// catalog's; MinIO and other S3-compatible stores reject an AWS STS token.
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:   config.IcebergCatalogGlue,
			Region: "eu-north-1",
			Auth:   config.IcebergAuth{AccessKey: "ASIAKID", SecretKey: "AWSSECRET", SessionToken: "AWSTOKEN"},
		},
		Storage: config.IcebergStorage{
			Type:     config.IcebergStorageS3,
			Path:     "s3://warehouse/wh",
			Endpoint: "localhost:9000",
			Auth:     config.IcebergAuth{AccessKey: "minioadmin", SecretKey: "minioadmin"},
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "minioadmin", q.Get("s3.access-key-id"))
	assert.Equal(t, "minioadmin", q.Get("s3.secret-access-key"))
	assert.Empty(t, q.Get("s3.session-token"), "catalog session token must not leak into S3-compatible storage")
	assert.Empty(t, q.Get("session_token"), "nor through the shared params, which ingestr aliases into s3.*")
	assert.Equal(t, "AWSTOKEN", q.Get("glue.session-token"))
}

func TestConfig_GetIngestrURI_CredentialsSharedWhenStorageHasNone(t *testing.T) {
	t.Parallel()

	// Storage without credentials still inherits the catalog's, the common
	// Glue + real-S3 setup where both sides are the same AWS account.
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:   config.IcebergCatalogGlue,
			Region: testAWSRegion,
			Auth:   config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET", SessionToken: "TOKEN"},
		},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3LakeWh},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "AKID", q.Get("s3.access-key-id"))
	assert.Equal(t, "SECRET", q.Get("s3.secret-access-key"))
	assert.Equal(t, "TOKEN", q.Get("s3.session-token"))
	assert.Equal(t, testAWSRegion, q.Get("s3.region"))
}

func TestConfig_GetIngestrURI_SQLiteS3MinIO(t *testing.T) {
	t.Parallel()

	useSSL := false
	c := Config{
		Catalog: config.IcebergCatalog{
			Type: config.IcebergCatalogSQLite,
			Path: "/state/catalog.db",
		},
		Storage: config.IcebergStorage{
			Type:     config.IcebergStorageS3,
			Path:     "s3://ingestr-iceberg/warehouse",
			Endpoint: "localhost:9000",
			UseSSL:   &useSSL,
			Region:   testAWSRegion,
			Auth:     config.IcebergAuth{AccessKey: "minioadmin", SecretKey: "minioadmin"},
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+sqlite:///state/catalog.db?"), "got: %s", got)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+sqlite", scheme)
	assert.Empty(t, q.Get("storage"), "storage flag is no longer emitted; the warehouse scheme carries the backend")
	assert.Equal(t, "localhost:9000", q.Get("endpoint"))
	assert.Equal(t, "false", q.Get("use_ssl"))
	assert.Equal(t, "minioadmin", q.Get("s3.access-key-id"))
}

func TestConfig_GetIngestrURI_BucketPrefixStorage(t *testing.T) {
	t.Parallel()

	// bucket (+ prefix) is an alternative to a full s3:// path.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Bucket: "company-lake",
			Prefix: "warehouse",
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Empty(t, q.Get("storage"), "storage flag is no longer emitted; the warehouse scheme carries the backend")
	assert.Equal(t, "company-lake", q.Get("bucket"))
	assert.Equal(t, "warehouse", q.Get("prefix"))
	assert.Empty(t, q.Get("warehouse"), "warehouse must not be set when bucket is used")
}

func TestConfig_GetIngestrURI_PostgresS3(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{
			Type:     config.IcebergCatalogPostgres,
			Host:     "metadata-db.internal",
			Port:     5432,
			Database: "iceberg_catalog",
			Auth:     config.IcebergAuth{Username: "iceberg_user", Password: "secret"},
		},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Path:   "s3://company-lake/warehouse",
			Region: "eu-west-1",
			Auth:   config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+postgres://iceberg_user:secret@metadata-db.internal:5432/iceberg_catalog?"), "got: %s", got)

	_, q := parseQuery(t, got)
	assert.Empty(t, q.Get("storage"), "storage flag is no longer emitted; the warehouse scheme carries the backend")
	assert.Equal(t, "eu-west-1", q.Get("s3.region"))
}

func TestConfig_GetIngestrURI_RESTCatalog(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{
			Type: config.IcebergCatalogREST,
			Host: "catalog.internal",
			Port: 8181,
			// REST-catalog auth via dedicated (masked) fields.
			Credential: "client-id:secret",
		},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Path:   "s3://warehouse/prod",
			Region: testAWSRegion,
		},
		// Non-secret advanced options via the passthrough.
		Properties: map[string]string{
			"oauth2-server-uri": "https://auth.internal/token",
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+rest://catalog.internal:8181?"), "got: %s", got)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+rest", scheme)
	assert.Empty(t, q.Get("storage"), "storage flag is no longer emitted; the warehouse scheme carries the backend")
	assert.Equal(t, "client-id:secret", q.Get("credential"))
	assert.Equal(t, "https://auth.internal/token", q.Get("oauth2-server-uri"))
}

func TestConfig_GetIngestrURI_R2Catalog(t *testing.T) {
	t.Parallel()

	// R2 needs no storage block: the catalog vends the S3 credentials.
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:      config.IcebergCatalogR2,
			CatalogID: "a430f6cbf157ff5d1518cdab0cba50aa",
			Database:  "ingestr-r2-test",
			Token:     "cfat_secret",
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+r2://a430f6cbf157ff5d1518cdab0cba50aa/ingestr-r2-test?"), "got: %s", got)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+r2", scheme)
	assert.Equal(t, "cfat_secret", q.Get("token"))
	assert.Empty(t, q.Get("warehouse"), "R2 derives the warehouse from the account/bucket in the URI")
}

func TestConfig_GetIngestrURI_R2CatalogRequiresAccountBucketToken(t *testing.T) {
	t.Parallel()

	for _, cat := range []config.IcebergCatalog{
		{Type: config.IcebergCatalogR2, Database: "b", Token: "t"},
		{Type: config.IcebergCatalogR2, CatalogID: "acct", Token: "t"},
		{Type: config.IcebergCatalogR2, CatalogID: "acct", Database: "b"},
	} {
		_, err := Config{Catalog: cat}.GetIngestrURI()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "r2 catalog requires")
	}
}

func TestConfig_GetIngestrURI_HadoopAndHive(t *testing.T) {
	t.Parallel()

	hadoop := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHadoop, Path: "/tmp/warehouse"},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
	}
	got, err := hadoop.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+hadoop:///tmp/warehouse?"), "got: %s", got)

	hive := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHive, Host: "metastore", Port: 9083},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
	}
	got, err = hive.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+hive://metastore:9083?"), "got: %s", got)
}

func TestConfig_GetIngestrURI_HadoopWarehouseURIInCatalogPath(t *testing.T) {
	t.Parallel()

	// A warehouse URI in catalog.path is not a filesystem path: in the URL path it
	// would come back as "/s3://bucket/wh" and the catalog would treat it as local.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHadoop, Path: "s3://bucket/wh"},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+hadoop://?"), "warehouse URI must not land in the URL path, got: %s", got)

	_, q := parseQuery(t, got)
	assert.Equal(t, "s3://bucket/wh", q.Get("warehouse"))
}

func TestConfig_GetIngestrURI_HadoopStorageWarehouseWins(t *testing.T) {
	t.Parallel()

	// storage.path is the documented place for the warehouse, so it takes
	// precedence over a URI left in catalog.path.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHadoop, Path: "s3://from-catalog/wh"},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://from-storage/wh"},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "s3://from-storage/wh", q.Get("warehouse"))
}

func TestConfig_GetIngestrURI_S3CompatibleStorageKeepsCredentialsToItself(t *testing.T) {
	t.Parallel()

	// The catalog has no credentials of its own: it authenticates with whatever
	// the AWS default chain provides (env, SSO, instance role). MinIO's key pair
	// must not reach glue.* through ingestr's aliasing, or Glue is called with it.
	useSSL := false
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: "eu-north-1"},
		Storage: config.IcebergStorage{
			Type:     config.IcebergStorageS3,
			Path:     "s3://warehouse/wh",
			Endpoint: "localhost:9000",
			UseSSL:   &useSSL,
			Region:   testAWSRegion,
			Auth:     config.IcebergAuth{AccessKey: "minioadmin", SecretKey: "minioadmin"},
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "minioadmin", q.Get("s3.access-key-id"))
	assert.Equal(t, "minioadmin", q.Get("s3.secret-access-key"))
	assert.Equal(t, testAWSRegion, q.Get("s3.region"))
	assert.Empty(t, q.Get("access_key_id"), "storage keys must not sit in the shared params, which ingestr aliases into glue.*")
	assert.Empty(t, q.Get("secret_access_key"))
	assert.Equal(t, "eu-north-1", q.Get("glue.region"))
	assert.Empty(t, q.Get("glue.access-key-id"), "the catalog brought no credentials, so it must fall through to the default chain")
}

func TestConfig_GetIngestrURI_AWSEndpointStillSharesCredentials(t *testing.T) {
	t.Parallel()

	// Regional, VPC and FIPS endpoints are still AWS, so the credentials stay in
	// the shared keys and a Glue catalog without its own auth keeps inheriting them.
	for _, endpoint := range []string{
		"s3.eu-north-1.amazonaws.com",
		"https://s3.eu-north-1.amazonaws.com",
		"bucket.vpce-0123-abcd.s3.eu-north-1.vpce.amazonaws.com",
		"s3-fips.us-east-1.amazonaws.com:443",
	} {
		c := Config{
			Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
			Storage: config.IcebergStorage{
				Type:     config.IcebergStorageS3,
				Path:     testS3LakeWh,
				Endpoint: endpoint,
				Auth:     config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
			},
		}

		_, q := parseQuery(t, mustURI(t, c))
		assert.Equalf(t, "AKID", q.Get("glue.access-key-id"), "endpoint %q is AWS, so Glue must still inherit the credentials", endpoint)
		assert.Equalf(t, "AKID", q.Get("s3.access-key-id"), "the storage keeps its own credentials for endpoint %q", endpoint)
	}
}

func TestConfig_GetIngestrURI_RealS3StillSharesCredentials(t *testing.T) {
	t.Parallel()

	// No endpoint means real AWS S3, where catalog and storage are normally the
	// same account, so credentials given on one side serve both.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage: config.IcebergStorage{
			Type: config.IcebergStorageS3,
			Path: testS3LakeWh,
			Auth: config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "AKID", q.Get("s3.access-key-id"))
	assert.Equal(t, "SECRET", q.Get("s3.secret-access-key"))
	assert.Equal(t, "AKID", q.Get("glue.access-key-id"), "only the storage is configured, so Glue inherits it")
	assert.Equal(t, testAWSRegion, q.Get("glue.region"))
}

func TestConfig_GetIngestrURI_RESTUseSSL(t *testing.T) {
	t.Parallel()

	useSSL := true
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:       config.IcebergCatalogREST,
			Host:       "catalog.example.com",
			Port:       443,
			RestUseSSL: &useSSL,
		},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "true", q.Get("rest_use_ssl"))
}

func TestConfig_GetIngestrURI_SQLCatalogDriverDialect(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{
			Type:    config.IcebergCatalogSQL,
			URI:     "postgresql://u:p@h:5432/db",
			Driver:  "pgx",
			Dialect: "postgres",
		},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
	}
	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "postgresql://u:p@h:5432/db", q.Get("uri"))
	assert.Equal(t, "pgx", q.Get("sql.driver"))
	assert.Equal(t, "postgres", q.Get("sql.dialect"))
}

func TestConfig_GetIngestrURI_GCSStorage(t *testing.T) {
	t.Parallel()

	// Native GCS via a gs:// warehouse + service-account key file.
	path := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{
			Type:    config.IcebergStorageGCS,
			Path:    "gs://company-lake/warehouse",
			KeyFile: "/creds/gcs-sa.json",
		},
	}
	_, q := parseQuery(t, mustURI(t, path))
	assert.Equal(t, "gcs", q.Get("storage"))
	assert.Equal(t, "gs://company-lake/warehouse", q.Get("warehouse"))
	assert.Equal(t, "/creds/gcs-sa.json", q.Get("gcs.keypath"))

	// Bucket (+ prefix): storage=gcs tells ingestr to build the gs:// warehouse.
	bucket := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageGCS,
			Bucket: "company-lake",
			Prefix: "warehouse",
		},
	}
	_, q = parseQuery(t, mustURI(t, bucket))
	assert.Equal(t, "gcs", q.Get("storage"))
	assert.Equal(t, "company-lake", q.Get("bucket"))
	assert.Equal(t, "warehouse", q.Get("prefix"))
}

func TestConfig_GetIngestrURI_LocalStorage(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Type: config.IcebergStorageLocal, Path: "file:///tmp/warehouse"},
	}
	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "file:///tmp/warehouse", q.Get("warehouse"))
	assert.Empty(t, q.Get("storage"), "local storage sets no storage flag")
}

func TestConfig_GetIngestrURI_HadoopObjectStorage(t *testing.T) {
	t.Parallel()

	// No catalog path: the Hadoop warehouse comes from the storage block (s3://).
	c := Config{
		Catalog:    config.IcebergCatalog{Type: config.IcebergCatalogHadoop},
		Storage:    config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3LakeWh},
		Properties: map[string]string{"allow-unsafe-commits": "true"},
	}
	got := mustURI(t, c)
	assert.True(t, strings.HasPrefix(got, "iceberg+hadoop://?"), "got: %s", got)
	_, q := parseQuery(t, got)
	assert.Equal(t, testS3LakeWh, q.Get("warehouse"))
	assert.Equal(t, "true", q.Get("allow-unsafe-commits"))
}

func TestConfig_GetIngestrURI_SQLCatalogNoStorage(t *testing.T) {
	t.Parallel()

	// The advanced sql catalog may own its warehouse, so storage is optional.
	c := Config{
		Catalog: config.IcebergCatalog{
			Type:    config.IcebergCatalogSQL,
			URI:     "postgresql://u:p@h:5432/db",
			Driver:  "pgx",
			Dialect: "postgres",
		},
	}
	got := mustURI(t, c)
	assert.True(t, strings.HasPrefix(got, "iceberg+sql://?"), "got: %s", got)
}

func TestConfig_GetIngestrURI_HadoopLocalNoStorage(t *testing.T) {
	t.Parallel()

	// A local Hadoop warehouse (catalog path) needs no storage block.
	c := Config{Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHadoop, Path: "/tmp/wh"}}
	got := mustURI(t, c)
	assert.True(t, strings.HasPrefix(got, "iceberg+hadoop:///tmp/wh"), "got: %s", got)
}

func TestConfig_GetIngestrURI_NoStorage(t *testing.T) {
	t.Parallel()

	// Glue/REST/SQL catalogs may supply their own warehouse; storage is optional.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
	}
	got := mustURI(t, c)
	assert.True(t, strings.HasPrefix(got, "iceberg+glue://?"), "got: %s", got)
	_, q := parseQuery(t, got)
	assert.Empty(t, q.Get("storage"))
	assert.Equal(t, testAWSRegion, q.Get("glue.region"))
}

func TestConfig_GetIngestrURI_SQLCatalogViaProperties(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQL, URI: "postgresql://u:p@h:5432/db"},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
	}
	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+sql://?"), "got: %s", got)
	_, q := parseQuery(t, got)
	assert.Equal(t, "postgresql://u:p@h:5432/db", q.Get("uri"))
}

func TestConfig_GetIngestrURI_TableAndNamespaceParams(t *testing.T) {
	t.Parallel()

	createNS := false
	c := Config{
		Catalog:         config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage:         config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3LakeWh},
		CreateNamespace: &createNS,
		TableLocation:   "s3://lake/wh/{namespace}/{table}",
		TablePath:       "{namespace}/{table}",
		TableProperties: map[string]string{
			"write.format.default": "parquet",
			"format-version":       "2",
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "false", q.Get("create_namespace"))
	assert.Equal(t, "s3://lake/wh/{namespace}/{table}", q.Get("table_location"))
	assert.Equal(t, "{namespace}/{table}", q.Get("table_path"))
	assert.Equal(t, "parquet", q.Get("table.write.format.default"))
	assert.Equal(t, "2", q.Get("table.format-version"))
}

func TestConfig_GetIngestrURI_PropertiesOverride(t *testing.T) {
	t.Parallel()

	// A passthrough property wins over a structured value on conflict.
	c := Config{
		Catalog:    config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage:    config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3LakeWh},
		Properties: map[string]string{"region": "eu-west-1"},
	}
	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "eu-west-1", q.Get("region"))
}

func mustURI(t *testing.T, c Config) string {
	t.Helper()
	uri, err := c.GetIngestrURI()
	require.NoError(t, err)
	return uri
}

func TestConfig_GetIngestrURI_InferStorageFromScheme(t *testing.T) {
	t.Parallel()

	// gs:// warehouse, no storage.type → inferred GCS.
	gcs := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Path: "gs://lake/wh", KeyFile: "/sa.json"},
	}
	_, q := parseQuery(t, mustURI(t, gcs))
	assert.Equal(t, "gs://lake/wh", q.Get("warehouse"))
	assert.Equal(t, "/sa.json", q.Get("gcs.keypath"))
	assert.Empty(t, q.Get("storage"))

	// s3:// warehouse, no type → inferred S3 (credentials emitted).
	s3 := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Path: testS3LakeWh, Region: testAWSRegion, Auth: config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"}},
	}
	_, q = parseQuery(t, mustURI(t, s3))
	assert.Equal(t, testS3LakeWh, q.Get("warehouse"))
	assert.Equal(t, "AKID", q.Get("s3.access-key-id"))

	// file:// warehouse, no type → inferred local.
	local := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Path: "file:///tmp/wh"},
	}
	_, q = parseQuery(t, mustURI(t, local))
	assert.Equal(t, "file:///tmp/wh", q.Get("warehouse"))

	// bucket with no type → defaults to s3 (bucket/prefix passthrough).
	bucket := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Bucket: "lake", Prefix: "wh"},
	}
	_, q = parseQuery(t, mustURI(t, bucket))
	assert.Equal(t, "lake", q.Get("bucket"))
	assert.Equal(t, "wh", q.Get("prefix"))

	// bucket with type=gcs → storage=gcs + bucket/prefix (ingestr builds gs://).
	gcsBucket := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite, Path: testSQLitePath},
		Storage: config.IcebergStorage{Type: config.IcebergStorageGCS, Bucket: "lake", Prefix: "wh"},
	}
	_, q = parseQuery(t, mustURI(t, gcsBucket))
	assert.Equal(t, "gcs", q.Get("storage"))
	assert.Equal(t, "lake", q.Get("bucket"))
	assert.Equal(t, "wh", q.Get("prefix"))
}

func TestConfig_GetIngestrURI_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "missing catalog type",
			config:  Config{Storage: config.IcebergStorage{Type: config.IcebergStorageS3}},
			wantErr: "catalog.type must be provided",
		},
		{
			name: "unsupported catalog type",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogType("duckdb")},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3},
			},
			wantErr: `unsupported catalog type "duckdb"`,
		},
		{
			name: "sqlite requires path",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3},
			},
			wantErr: "sqlite catalog requires",
		},
		{
			name: "sql requires uri",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQL},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path},
			},
			wantErr: "sql catalog requires",
		},
		{
			name: "unsupported storage type",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageType("azure"), Path: "az://b/p"},
			},
			wantErr: `unsupported storage type "azure"`,
		},
		{
			name: "path and bucket are mutually exclusive",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3Path, Bucket: "b"},
			},
			wantErr: `set either "path"`,
		},
		{
			name: "prefix requires bucket",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Prefix: "warehouse"},
			},
			wantErr: `"prefix" requires "bucket"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.config.GetIngestrURI()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: testS3LakeWh},
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(uri, "iceberg+glue://?"), "got: %s", uri)
}
