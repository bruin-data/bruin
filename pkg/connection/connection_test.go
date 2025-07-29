package connection

import (
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
	require.Error(t, err)

	res, ok := m.GetConnection("test").(bigquery.DB)
	assert.False(t, ok)
	assert.Nil(t, res)
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

	err := m.AddPgConnectionFromConfig(configuration)
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

	err := m.AddRedshiftConnectionFromConfig(configuration)
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
