package oracle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_DSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
		checkDSN    func(t *testing.T, dsn string)
	}{
		{
			name: "basic service name configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "localhost")
				assert.Contains(t, dsn, "1521")
				assert.Contains(t, dsn, "testuser")
				assert.Contains(t, dsn, "testpass")
				assert.Contains(t, dsn, "ORCL")
			},
		},
		{
			name: "basic SID configuration",
			config: Config{
				Host:     "localhost",
				Port:     "1521",
				Username: "testuser",
				Password: "testpass",
				SID:      "ORCL",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "localhost")
				assert.Contains(t, dsn, "1521")
				assert.Contains(t, dsn, "testuser")
				assert.Contains(t, dsn, "testpass")
				assert.Contains(t, dsn, "SID=ORCL")
			},
		},
		{
			name: "default port when not specified",
			config: Config{
				Host:        "localhost",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "1521")
			},
		},
		{
			name: "SSL configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				SSL:         true,
				SSLVerify:   true,
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "SSL=enable")
			},
		},
		{
			name: "SSL with verification disabled",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				SSL:         true,
				SSLVerify:   false,
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "SSL=enable")
				assert.Contains(t, dsn, "SSL Verify=false")
			},
		},
		{
			name: "SSL with wallet",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				SSL:         true,
				Wallet:      "/path/to/wallet",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "SSL=enable")
				assert.Contains(t, dsn, "WALLET=%2Fpath%2Fto%2Fwallet")
			},
		},
		{
			name: "DBA role configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				Role:        "SYSDBA",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "DBA Privilege=SYSDBA")
			},
		},
		{
			name: "prefetch rows configuration",
			config: Config{
				Host:         "localhost",
				Port:         "1521",
				Username:     "testuser",
				Password:     "testpass",
				ServiceName:  "ORCL",
				PrefetchRows: 1000,
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "PREFETCH_ROWS=1000")
			},
		},
		{
			name: "trace file configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				TraceFile:   "/path/to/trace.log",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "TRACE FILE=%2Fpath%2Fto%2Ftrace.log")
			},
		},
		{
			name: "service name takes precedence over SID",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				SID:         "ORCL_SID",
			},
			expectError: false,
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "ORCL")
				assert.NotContains(t, dsn, "SID=ORCL_SID")
			},
		},
		{
			name: "neither service name nor SID specified",
			config: Config{
				Host:     "localhost",
				Port:     "1521",
				Username: "testuser",
				Password: "testpass",
			},
			expectError: true,
			errorMsg:    "either ServiceName or SID must be specified",
		},
		{
			name: "invalid port number",
			config: Config{
				Host:        "localhost",
				Port:        "invalid",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expectError: false, // Should use default port 1521
			checkDSN: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "1521")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dsn, err := tt.config.DSN()

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, dsn)
				if tt.checkDSN != nil {
					tt.checkDSN(t, dsn)
				}
			}
		})
	}
}

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "service name configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expected: "oracle+cx_oracle://testuser:testpass@localhost:1521/?service_name=ORCL",
		},
		{
			name: "SID configuration when no service name",
			config: Config{
				Host:     "localhost",
				Port:     "1521",
				Username: "testuser",
				Password: "testpass",
				SID:      "ORCL",
			},
			expected: "oracle+cx_oracle://testuser:testpass@localhost:1521/ORCL",
		},
		{
			name: "default port when not specified",
			config: Config{
				Host:        "localhost",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expected: "oracle+cx_oracle://testuser:testpass@localhost:1521/?service_name=ORCL",
		},
		{
			name: "service name takes precedence over SID",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
				SID:         "ORCL_SID",
			},
			expected: "oracle+cx_oracle://testuser:testpass@localhost:1521/?service_name=ORCL",
		},
		{
			name: "special characters in password",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "test@pass:word",
				ServiceName: "ORCL",
			},
			expected: "oracle+cx_oracle://testuser:test%40pass%3Aword@localhost:1521/?service_name=ORCL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri, err := tt.config.GetIngestrURI()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, uri)
		})
	}
}

func TestConfig_GetIngestrURI_Error(t *testing.T) {
	t.Parallel()

	config := Config{
		Host:     "localhost",
		Port:     "1521",
		Username: "testuser",
		Password: "testpass",
	}

	uri, err := config.GetIngestrURI()
	require.Error(t, err)
	assert.Equal(t, "", uri)
	assert.Contains(t, err.Error(), "either ServiceName or SID must be specified")
}

func TestConfig_DSN_ComplexScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		validate func(t *testing.T, dsn string)
	}{
		{
			name: "all options configured",
			config: Config{
				Host:         "oracle.example.com",
				Port:         "1521",
				Username:     "admin",
				Password:     "securepass",
				ServiceName:  "PRODDB",
				Role:         "SYSDBA",
				SSL:          true,
				SSLVerify:    false,
				Wallet:       "/etc/oracle/wallet",
				PrefetchRows: 5000,
				TraceFile:    "/var/log/oracle/trace.log",
			},
			validate: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "oracle.example.com")
				assert.Contains(t, dsn, "1521")
				assert.Contains(t, dsn, "admin")
				assert.Contains(t, dsn, "securepass")
				assert.Contains(t, dsn, "PRODDB")
				assert.Contains(t, dsn, "DBA Privilege=SYSDBA")
				assert.Contains(t, dsn, "SSL=enable")
				assert.Contains(t, dsn, "SSL Verify=false")
				assert.Contains(t, dsn, "WALLET=%2Fetc%2Foracle%2Fwallet")
				assert.Contains(t, dsn, "PREFETCH_ROWS=5000")
				assert.Contains(t, dsn, "TRACE FILE=%2Fvar%2Flog%2Foracle%2Ftrace.log")
			},
		},
		{
			name: "SID with all options",
			config: Config{
				Host:         "oracle.example.com",
				Port:         "1521",
				Username:     "admin",
				Password:     "securepass",
				SID:          "PRODDB",
				Role:         "SYSOPER",
				SSL:          true,
				SSLVerify:    true,
				Wallet:       "/etc/oracle/wallet",
				PrefetchRows: 1000,
				TraceFile:    "/var/log/oracle/trace.log",
			},
			validate: func(t *testing.T, dsn string) {
				assert.Contains(t, dsn, "oracle.example.com")
				assert.Contains(t, dsn, "1521")
				assert.Contains(t, dsn, "admin")
				assert.Contains(t, dsn, "securepass")
				assert.Contains(t, dsn, "SID=PRODDB")
				assert.Contains(t, dsn, "DBA Privilege=SYSOPER")
				assert.Contains(t, dsn, "SSL=enable")
				assert.NotContains(t, dsn, "SSL Verify=false") // Should not be present when SSLVerify is true
				assert.Contains(t, dsn, "WALLET=%2Fetc%2Foracle%2Fwallet")
				assert.Contains(t, dsn, "PREFETCH_ROWS=1000")
				assert.Contains(t, dsn, "TRACE FILE=%2Fvar%2Flog%2Foracle%2Ftrace.log")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dsn, err := tt.config.DSN()
			require.NoError(t, err)
			assert.NotEmpty(t, dsn)
			tt.validate(t, dsn)
		})
	}
}
