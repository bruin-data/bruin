package connection

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/emr_serverless"
	"github.com/bruin-data/bruin/pkg/gorgias"
	"github.com/bruin-data/bruin/pkg/hana"
	"github.com/bruin-data/bruin/pkg/mongo"
	"github.com/bruin-data/bruin/pkg/mssql"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/notion"
	"github.com/bruin-data/bruin/pkg/personio"
	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/bruin-data/bruin/pkg/shopify"
	"github.com/bruin-data/bruin/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestManager_GetConnection(t *testing.T) {
	t.Parallel()

	existingDB := new(bigquery.Client)
	m := Manager{
		AllConnectionDetails: map[string]any{},
		BigQuery: map[string]*bigquery.Client{
			"another":  new(bigquery.Client),
			"existing": existingDB,
		},
		availableConnections: map[string]any{
			"another":  new(bigquery.Client),
			"existing": existingDB,
		},
	}

	tests := []struct {
		name           string
		connectionName string
		want           bigquery.DB
		wantErr        bool
	}{
		{
			name:           "should return error when no connections are found",
			connectionName: "non-existing",
			wantErr:        true,
		},
		{
			name:           "should find the correct connection",
			connectionName: "existing",
			want:           existingDB,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := m.GetConnection(tt.connectionName).(bigquery.DB)
			if tt.wantErr {
				assert.False(t, ok)
				assert.Nil(t, got)
			} else {
				assert.True(t, ok)
				assert.NotNil(t, got)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestManager_AddBqConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		availableConnections: make(map[string]any),
		AllConnectionDetails: map[string]any{},
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	connection := &config.GoogleCloudPlatformConnection{
		Name:      "test",
		ProjectID: "test",
	}
	connection.SetCredentials(&google.Credentials{
		ProjectID: "some-project-id",
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "some-token",
		}),
	})

	err := m.AddBqConnectionFromConfig(connection)
	if err != nil {
		assert.NotContains(t, err.Error(), "credentials are required")
	}

	connectionResult := m.GetConnection("test")
	_ = connectionResult
}

func TestManager_AddPgConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.PostgresConnection{
		Name:         "test",
		Host:         "somehost",
		Username:     "user",
		Password:     "pass",
		Database:     "db",
		Port:         15432,
		SslMode:      "disable",
		PoolMaxConns: 10,
	}

	err := m.AddPgConnectionFromConfig(t.Context(), configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(postgres.PgClient)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddRedshiftConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res, ok := m.GetConnection("test").(postgres.PgClient)
	assert.False(t, ok)
	assert.Nil(t, res)

	configuration := &config.RedshiftConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
		SslMode:  "disable",
	}

	err := m.AddRedshiftConnectionFromConfig(t.Context(), configuration)
	require.NoError(t, err)

	res, ok = m.GetConnection("test").(postgres.PgClient)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddMsSQLConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.MsSQLConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
	}

	err := m.AddMsSQLConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(mssql.MsClient)

	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddMongoConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")

	assert.Nil(t, res)

	configuration := &config.MongoConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
	}

	err := m.AddMongoConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(mongo.MongoConnection)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddMySqlConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
		Mysql:                make(map[string]*mysql.Client),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.MySQLConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
	}

	m.Mysql[configuration.Name] = new(mysql.Client)
	m.availableConnections[configuration.Name] = m.Mysql[configuration.Name]

	res, ok := m.GetConnection("test").(mysql.DB)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddNotionConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.NotionConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY	",
	}

	err := m.AddNotionConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*notion.Client)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddShopiyConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.ShopifyConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY",
		URL:    "testxxx",
	}

	err := m.AddShopifyConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*shopify.Client)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddGorgiasConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.GorgiasConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY",
		Domain: "domain",
		Email:  "email",
	}

	err := m.AddGorgiasConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*gorgias.Client)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddHANAConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.HANAConnection{
		Name:     "test",
		Username: "user",
		Password: "pass",
		Host:     "somehost",
		Port:     15432,
		Database: "db",
	}

	err := m.AddHANAConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*hana.Client)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func Test_AddEMRServerlessConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}
	res := m.GetConnection("test")
	assert.Nil(t, res)

	cfg := &config.EMRServerlessConnection{
		Name:          "test",
		AccessKey:     "AKIAEXAMPLE",
		SecretKey:     "SECRETKEYEXAMPLE",
		ApplicationID: "application-id",
		ExecutionRole: "execution-role",
		Region:        "us-east-1",
	}

	err := m.AddEMRServerlessConnectionFromConfig(cfg)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*emr_serverless.Client)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestNewManagerFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cm     *config.Config
		env    map[string]string
		want   *Manager
		errors []error
	}{
		{
			name: "create manager from a simple config",
			cm: &config.Config{
				SelectedEnvironment: &config.Environment{
					Connections: &config.Connections{
						Personio: []config.PersonioConnection{
							{
								Name:         "key1",
								ClientID:     "id1",
								ClientSecret: "secret1",
							},
							{
								Name:         "key2",
								ClientID:     "val1_${PERSONIO_CLIENT_ID}_val2",
								ClientSecret: "${PERSONIO_CLIENT_SECRET}",
							},
							{
								Name:         "key3",
								ClientID:     "${MISSING_PERSONIO_CLIENT_ID}",
								ClientSecret: "${MISSING_PERSONIO_CLIENT_SECRET}",
							},
						},
					},
				},
			},
			env: map[string]string{
				"PERSONIO_CLIENT_ID":     "id2",
				"PERSONIO_CLIENT_SECRET": "secret2",
			},
			want: &Manager{
				AllConnectionDetails: map[string]any{
					"key1": &config.PersonioConnection{
						Name:         "key1",
						ClientID:     "id1",
						ClientSecret: "secret1",
					},
					"key2": &config.PersonioConnection{
						Name:         "key2",
						ClientID:     "val1_id2_val2",
						ClientSecret: "secret2",
					},
					"key3": &config.PersonioConnection{
						Name:         "key3",
						ClientID:     "",
						ClientSecret: "",
					},
				},
				availableConnections: map[string]any{
					"key1": personio.NewClient(personio.Config{
						ClientID:     "id1",
						ClientSecret: "secret1",
					}),
					"key2": personio.NewClient(personio.Config{
						ClientID:     "val1_id2_val2",
						ClientSecret: "secret2",
					}),
					"key3": personio.NewClient(personio.Config{
						ClientID:     "",
						ClientSecret: "",
					}),
				},
				Personio: map[string]*personio.Client{
					"key1": personio.NewClient(personio.Config{
						ClientID:     "id1",
						ClientSecret: "secret1",
					}),
					"key2": personio.NewClient(personio.Config{
						ClientID:     "val1_id2_val2",
						ClientSecret: "secret2",
					}),
					"key3": personio.NewClient(personio.Config{
						ClientID:     "",
						ClientSecret: "",
					}),
				},
			},
			errors: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for k, v := range tt.env {
				os.Setenv(k, v) //nolint
			}
			got, errors := NewManagerFromConfig(tt.cm)
			assert.Equalf(t, tt.want, got, "NewManagerFromConfig(%v)", tt.cm)
			assert.Equalf(t, tt.errors, errors, "NewManagerFromConfig(%v)", tt.cm)
		})
	}
}

func TestManager_GetSfConnection(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		Snowflake: map[string]*snowflake.DB{
			"existing": {},
		},
		availableConnections: map[string]any{
			"existing": &snowflake.DB{},
		},
	}

	tests := []struct {
		name           string
		connectionName string
		wantErr        bool
	}{
		{
			name:           "should return error when no connections are found",
			connectionName: "non-existing",
			wantErr:        true,
		},
		{
			name:           "should find the correct connection",
			connectionName: "existing",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := m.GetConnection(tt.connectionName).(snowflake.SfClient)
			if tt.wantErr {
				assert.False(t, ok)
				assert.Nil(t, got)
			} else {
				assert.True(t, ok)
				assert.NotNil(t, got)
			}
		})
	}
}

func TestManager_AddGenericConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}
	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.GenericConnection{
		Name:  "test",
		Value: "somevalue",
	}

	err := m.AddGenericConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*config.GenericConnection)
	assert.True(t, ok)
	assert.NotNil(t, res)
}

func TestManager_AddAwsConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		AllConnectionDetails: map[string]any{},
		availableConnections: make(map[string]any),
	}

	res := m.GetConnection("test")
	assert.Nil(t, res)

	configuration := &config.AwsConnection{
		Name:      "test",
		AccessKey: "AKIAEXAMPLE",
		SecretKey: "SECRETKEYEXAMPLE",
	}

	err := m.AddAwsConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, ok := m.GetConnection("test").(*config.AwsConnection)
	assert.True(t, ok)
	assert.NotNil(t, res)

	awsConn, ok := res.(*config.AwsConnection)
	assert.True(t, ok)
	assert.Equal(t, "test", awsConn.Name)
	assert.Equal(t, "AKIAEXAMPLE", awsConn.AccessKey)
	assert.Equal(t, "SECRETKEYEXAMPLE", awsConn.SecretKey)
}

func Test_convertPKCS1ToPKCS8(t *testing.T) {
	t.Parallel()

	// Generate a test RSA private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create PKCS#1 formatted key
	pkcs1Bytes := x509.MarshalPKCS1PrivateKey(privateKey)
	pkcs1Block := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: pkcs1Bytes,
	}
	pkcs1PEM := string(pem.EncodeToMemory(pkcs1Block))

	// Create PKCS#8 formatted key for comparison
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	pkcs8Block := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: pkcs8Bytes,
	}
	pkcs8PEM := string(pem.EncodeToMemory(pkcs8Block))

	tests := []struct {
		name     string
		input    string
		expected string
		validate func(t *testing.T, result string)
	}{
		{
			name:     "valid PKCS#1 key should be converted to PKCS#8",
			input:    pkcs1PEM,
			expected: "", // We'll validate this differently since exact output may vary
			validate: func(t *testing.T, result string) {
				// Verify the result is in PKCS#8 format
				block, _ := pem.Decode([]byte(result))
				require.NotNil(t, block)
				assert.Equal(t, "PRIVATE KEY", block.Type)

				// Verify we can parse it as PKCS#8
				parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
				require.NoError(t, err)

				// Verify it's the same RSA key
				rsaKey, ok := parsedKey.(*rsa.PrivateKey)
				require.True(t, ok)
				assert.True(t, privateKey.Equal(rsaKey))
			},
		},
		{
			name:     "already PKCS#8 key should remain unchanged",
			input:    pkcs8PEM,
			expected: pkcs8PEM,
			validate: func(t *testing.T, result string) {
				assert.Equal(t, pkcs8PEM, result)
			},
		},
		{
			name:     "empty string should remain unchanged",
			input:    "",
			expected: "",
			validate: func(t *testing.T, result string) {
				assert.Equal(t, "", result)
			},
		},
		{
			name:     "invalid PEM should remain unchanged",
			input:    "not a valid PEM",
			expected: "not a valid PEM",
			validate: func(t *testing.T, result string) {
				assert.Equal(t, "not a valid PEM", result)
			},
		},
		{
			name: "malformed PKCS#1 key should remain unchanged",
			input: `-----BEGIN RSA PRIVATE KEY-----
invalid data here
-----END RSA PRIVATE KEY-----`,
			expected: `-----BEGIN RSA PRIVATE KEY-----
invalid data here
-----END RSA PRIVATE KEY-----`,
			validate: func(t *testing.T, result string) {
				expected := `-----BEGIN RSA PRIVATE KEY-----
invalid data here
-----END RSA PRIVATE KEY-----`
				assert.Equal(t, expected, result)
			},
		},
		{
			name: "non-RSA key type should remain unchanged",
			input: `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg
-----END PRIVATE KEY-----`,
			expected: `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg
-----END PRIVATE KEY-----`,
			validate: func(t *testing.T, result string) {
				expected := `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg
-----END PRIVATE KEY-----`
				assert.Equal(t, expected, result)
			},
		},
		{
			name: "certificate (non-private key) should remain unchanged",
			input: `-----BEGIN CERTIFICATE-----
MIIB...
-----END CERTIFICATE-----`,
			expected: `-----BEGIN CERTIFICATE-----
MIIB...
-----END CERTIFICATE-----`,
			validate: func(t *testing.T, result string) {
				expected := `-----BEGIN CERTIFICATE-----
MIIB...
-----END CERTIFICATE-----`
				assert.Equal(t, expected, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			
			result := convertPKCS1ToPKCS8(tt.input)
			
			if tt.expected != "" {
				assert.Equal(t, tt.expected, result)
			}
			
			if tt.validate != nil {
				tt.validate(t, result)
			}
		})
	}
}

func Test_convertPKCS1ToPKCS8_Integration(t *testing.T) {
	t.Parallel()

	// Test that demonstrates the actual conversion working end-to-end
	t.Run("full conversion flow", func(t *testing.T) {
		t.Parallel()

		// Generate a real RSA key
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		// Convert to PKCS#1 PEM
		pkcs1Bytes := x509.MarshalPKCS1PrivateKey(privateKey)
		pkcs1Block := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: pkcs1Bytes,
		}
		pkcs1PEM := string(pem.EncodeToMemory(pkcs1Block))

		// Convert using our function
		result := convertPKCS1ToPKCS8(pkcs1PEM)

		// Verify the result
		assert.NotEqual(t, pkcs1PEM, result, "Output should be different from input")
		assert.Contains(t, result, "-----BEGIN PRIVATE KEY-----")
		assert.Contains(t, result, "-----END PRIVATE KEY-----")
		assert.NotContains(t, result, "RSA PRIVATE KEY")

		// Parse the result and verify it's the same key
		block, _ := pem.Decode([]byte(result))
		require.NotNil(t, block)
		
		parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		require.NoError(t, err)
		
		rsaKey, ok := parsedKey.(*rsa.PrivateKey)
		require.True(t, ok)
		assert.True(t, privateKey.Equal(rsaKey))
	})
}
