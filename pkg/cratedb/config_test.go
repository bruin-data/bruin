package cratedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		c    Config
		want string
	}{
		{
			name: "localhost without password",
			c: Config{
				Username: "crate",
				Host:     "localhost",
				Port:     5432,
				SslMode:  "disable",
			},
			want: "cratedb://crate@localhost:5432?sslmode=disable",
		},
		{
			name: "cloud connection",
			c: Config{
				Username: "admin",
				Password: "password",
				Host:     "cluster.eks1.eu-west-1.aws.cratedb.net",
				Port:     5432,
				SslMode:  "require",
			},
			want: "cratedb://admin:password@cluster.eks1.eu-west-1.aws.cratedb.net:5432?sslmode=require",
		},
		{
			name: "default port",
			c: Config{
				Username: "crate",
				Host:     "localhost",
				SslMode:  "disable",
			},
			want: "cratedb://crate@localhost:5432?sslmode=disable",
		},
		{
			name: "without sslmode",
			c: Config{
				Username: "admin",
				Password: "password",
				Host:     "localhost",
				Port:     5432,
			},
			want: "cratedb://admin:password@localhost:5432",
		},
		{
			name: "encodes credentials",
			c: Config{
				Username: "admin@example.com",
				Password: "p@ss word",
				Host:     "localhost",
				Port:     5432,
				SslMode:  "require",
			},
			want: "cratedb://admin%40example.com:p%40ss%20word@localhost:5432?sslmode=require",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.c.GetIngestrURI())
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Username: "crate",
		Host:     "localhost",
		SslMode:  "disable",
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)

	assert.Equal(t, "cratedb://crate@localhost:5432?sslmode=disable", uri)
}
