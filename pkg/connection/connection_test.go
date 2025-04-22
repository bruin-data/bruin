package connection

import (
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/mysql"
	"github.com/bruin-data/bruin/pkg/personio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

func TestManager_GetBqConnection(t *testing.T) {
	t.Parallel()

	existingDB := new(bigquery.Client)
	m := Manager{
		BigQuery: map[string]*bigquery.Client{
			"another":  new(bigquery.Client),
			"existing": existingDB,
		},
	}

	tests := []struct {
		name           string
		connectionName string
		want           bigquery.DB
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name:           "should return error when no connections are found",
			connectionName: "non-existing",
			wantErr:        assert.Error,
		},
		{
			name:           "should find the correct connection",
			connectionName: "existing",
			want:           existingDB,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := m.GetBqConnection(tt.connectionName)
			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestManager_AddBqConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetBqConnection("test")
	require.Error(t, err)
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

	err = m.AddBqConnectionFromConfig(connection)
	require.Error(t, err)

	res, err = m.GetBqConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)
}

func TestManager_AddPgConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetPgConnection("test")
	require.Error(t, err)
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

	err = m.AddPgConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetPgConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddRedshiftConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetPgConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.RedshiftConnection{
		Name:         "test",
		Host:         "somehost",
		Username:     "user",
		Password:     "pass",
		Database:     "db",
		Port:         15432,
		SslMode:      "disable",
		PoolMaxConns: 10,
	}

	err = m.AddRedshiftConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetPgConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddMsSQLConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetMsConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.MsSQLConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
	}

	err = m.AddMsSQLConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetMsConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddMongoConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetMongoConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.MongoConnection{
		Name:     "test",
		Host:     "somehost",
		Username: "user",
		Password: "pass",
		Database: "db",
		Port:     15432,
	}

	err = m.AddMongoConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetMongoConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddMySqlConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{
		Mysql: make(map[string]*mysql.Client),
	}

	res, err := m.GetMySQLConnection("test")
	require.Error(t, err)
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

	res, err = m.GetMySQLConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddNotionConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetNotionConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.NotionConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY	",
	}

	err = m.AddNotionConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetNotionConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddShopiyConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetShopifyConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.ShopifyConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY",
		URL:    "testxxx",
	}

	err = m.AddShopifyConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetShopifyConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddGorgiasConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetGorgiasConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.GorgiasConnection{
		Name:   "test",
		APIKey: "xXXXXxxxxxYYY",
		Domain: "domain",
		Email:  "email",
	}

	err = m.AddGorgiasConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetGorgiasConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestManager_AddHANAConnectionFromConfigConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}

	res, err := m.GetHANAConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	configuration := &config.HANAConnection{
		Name:     "test",
		Username: "user",
		Password: "pass",
		Host:     "somehost",
		Port:     15432,
		Database: "db",
	}

	err = m.AddHANAConnectionFromConfig(configuration)
	require.NoError(t, err)

	res, err = m.GetHANAConnection("test")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func Test_AddEMRServerlessConnectionFromConfig(t *testing.T) {
	t.Parallel()

	m := Manager{}
	res, err := m.GetEMRServerlessConnection("test")
	require.Error(t, err)
	assert.Nil(t, res)

	cfg := &config.EMRServerlessConnection{
		Name:          "test",
		AccessKey:     "AKIAEXAMPLE",
		SecretKey:     "SECRETKEYEXAMPLE",
		ApplicationID: "application-id",
		ExecutionRole: "execution-role",
		Region:        "us-east-1",
	}

	err = m.AddEMRServerlessConnectionFromConfig(cfg)
	require.NoError(t, err)

	res, err = m.GetEMRServerlessConnection("test")
	require.NoError(t, err)
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
				os.Setenv(k, v)
			}
			got, errors := NewManagerFromConfig(tt.cm)
			assert.Equalf(t, tt.want, got, "NewManagerFromConfig(%v)", tt.cm)
			assert.Equalf(t, tt.errors, errors, "NewManagerFromConfig(%v)", tt.cm)
		})
	}
}
