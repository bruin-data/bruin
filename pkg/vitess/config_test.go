package vitess

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		want   string
	}{
		{
			name: "basic uri uses the vitess scheme",
			config: Config{
				Username: "user",
				Password: "password",
				Host:     "vtgate.internal",
				Port:     15306,
				Database: "commerce",
			},
			want: "vitess://user:password@vtgate.internal:15306/commerce",
		},
		{
			name: "default port is applied when unset",
			config: Config{
				Username: "user",
				Password: "password",
				Host:     "vtgate.internal",
				Database: "commerce",
			},
			want: "vitess://user:password@vtgate.internal:15306/commerce",
		},
		{
			name: "grpc settings are carried as query params",
			config: Config{
				Username: "user",
				Password: "password",
				Host:     "vtgate.internal",
				Port:     15306,
				Database: "commerce",
				GrpcPort: 15991,
				GrpcHost: "vtgate.grpc.internal",
				GrpcTLS:  true,
			},
			want: "vitess://user:password@vtgate.internal:15306/commerce?grpc_host=vtgate.grpc.internal&grpc_port=15991&grpc_tls=true",
		},
		{
			name: "ssl paths are carried as query params",
			config: Config{
				Username:    "user",
				Password:    "password",
				Host:        "vtgate.internal",
				Port:        15306,
				Database:    "commerce",
				SslCaPath:   "/path/to/ca.pem",
				SslCertPath: "/path/to/cert.pem",
				SslKeyPath:  "/path/to/key.pem",
			},
			want: "vitess://user:password@vtgate.internal:15306/commerce?ssl_ca=%2Fpath%2Fto%2Fca.pem&ssl_cert=%2Fpath%2Fto%2Fcert.pem&ssl_key=%2Fpath%2Fto%2Fkey.pem",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.config.GetIngestrURI())
		})
	}
}

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	c := Config{
		Username: "user",
		Password: "password",
		Host:     "vtgate.internal",
		Port:     15306,
		Database: "commerce",
	}
	assert.Equal(t, "user:password@tcp(vtgate.internal:15306)/commerce?multiStatements=true", c.ToDBConnectionURI())

	// TLS is requested when grpc TLS or any SSL path is configured.
	c.GrpcTLS = true
	assert.Equal(t, "user:password@tcp(vtgate.internal:15306)/commerce?multiStatements=true&tls=true", c.ToDBConnectionURI())
}
