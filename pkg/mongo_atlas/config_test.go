package mongoatlas

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
			name: "basic configuration",
			config: Config{
				Username: "user",
				Password: "password",
				Host:     "cluster0.mongodb.net",
				Database: "test",
			},
			expected: "mongodb+srv://user:password@cluster0.mongodb.net/test",
		},
		{
			name: "with special characters in password",
			config: Config{
				Username: "user",
				Password: "p@ssw0rd!",
				Host:     "cluster0.mongodb.net",
				Database: "mydb",
			},
			expected: "mongodb+srv://user:p%40ssw0rd%21@cluster0.mongodb.net/mydb",
		},
		{
			name: "with special characters in username",
			config: Config{
				Username: "user@example.com",
				Password: "password",
				Host:     "cluster0.mongodb.net",
				Database: "mydb",
			},
			expected: "mongodb+srv://user%40example.com:password@cluster0.mongodb.net/mydb",
		},
		{
			name: "with spaces in password",
			config: Config{
				Username: "user",
				Password: "pass word",
				Host:     "cluster0.mongodb.net",
				Database: "test",
			},
			expected: "mongodb+srv://user:pass%20word@cluster0.mongodb.net/test",
		},
		{
			name: "production-like cluster",
			config: Config{
				Username: "admin",
				Password: "securePassword123",
				Host:     "production-cluster.ab12cd.mongodb.net",
				Database: "analytics",
			},
			expected: "mongodb+srv://admin:securePassword123@production-cluster.ab12cd.mongodb.net/analytics",
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
