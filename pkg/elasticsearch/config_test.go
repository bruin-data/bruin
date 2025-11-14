package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "cloud elasticsearch with authentication and secure connection",
			config: Config{
				Username:    "elastic",
				Password:    "changeme",
				Host:        "cluster.cloud.es.io",
				Port:        443,
				Secure:      "true",
				VerifyCerts: "true",
			},
			expected: "elasticsearch://elastic:changeme@cluster.cloud.es.io:443?secure=true&verify_certs=true",
		},
		{
			name: "local elasticsearch without authentication",
			config: Config{
				Host:        "localhost",
				Port:        9200,
				Secure:      "false",
				VerifyCerts: "false",
			},
			expected: "elasticsearch://localhost:9200?secure=false&verify_certs=false",
		},
		{
			name: "with special characters in password",
			config: Config{
				Username:    "user",
				Password:    "p@ssw0rd!",
				Host:        "es-cluster.example.com",
				Port:        9200,
				Secure:      "true",
				VerifyCerts: "true",
			},
			expected: "elasticsearch://user:p%40ssw0rd%21@es-cluster.example.com:9200?secure=true&verify_certs=true",
		},
		{
			name: "with special characters in username",
			config: Config{
				Username:    "user@example.com",
				Password:    "password",
				Host:        "es.example.com",
				Port:        9200,
				Secure:      "false",
				VerifyCerts: "false",
			},
			expected: "elasticsearch://user%40example.com:password@es.example.com:9200?secure=false&verify_certs=false",
		},
		{
			name: "cloud instance with default secure settings",
			config: Config{
				Username:    "gifito",
				Password:    "mbappegol1",
				Host:        "6bb8fcbc3f724f2db9facd8cfd8ec97a.us-central1.gcp.cloud.es.io",
				Port:        443,
				Secure:      "true",
				VerifyCerts: "true",
			},
			expected: "elasticsearch://gifito:mbappegol1@6bb8fcbc3f724f2db9facd8cfd8ec97a.us-central1.gcp.cloud.es.io:443?secure=true&verify_certs=true",
		},
		{
			name: "with spaces in password",
			config: Config{
				Username:    "admin",
				Password:    "pass word",
				Host:        "localhost",
				Port:        9200,
				Secure:      "false",
				VerifyCerts: "false",
			},
			expected: "elasticsearch://admin:pass%20word@localhost:9200?secure=false&verify_certs=false",
		},
		{
			name: "secure false with verify_certs true",
			config: Config{
				Username:    "elastic",
				Password:    "password",
				Host:        "es.example.com",
				Port:        9200,
				Secure:      "false",
				VerifyCerts: "true",
			},
			expected: "elasticsearch://elastic:password@es.example.com:9200?secure=false&verify_certs=true",
		},
		{
			name: "empty secure and verify_certs (should default to true)",
			config: Config{
				Username: "user",
				Password: "pass",
				Host:     "es.example.com",
				Port:     443,
				Secure:   "",
				VerifyCerts: "",
			},
			expected: "elasticsearch://user:pass@es.example.com:443?secure=true&verify_certs=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.config.GetIngestrURI()
			assert.Equal(t, tt.expected, result)
		})
	}
}
