package clevertap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccountID = "acc-123"
	testPasscode  = "pass-123"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		want    string
		wantErr bool
	}{
		{
			name:   "required fields only",
			config: Config{AccountID: testAccountID, Passcode: testPasscode},
			want:   "clevertap://?account_id=acc-123&passcode=pass-123",
		},
		{
			name:   "all fields set",
			config: Config{AccountID: testAccountID, Passcode: testPasscode, Region: "in1", Timezone: "Asia/Kolkata"},
			want:   "clevertap://?account_id=acc-123&passcode=pass-123&region=in1&timezone=Asia%2FKolkata",
		},
		{
			name:   "passcode with special characters is escaped",
			config: Config{AccountID: testAccountID, Passcode: "p/ss+word"},
			want:   "clevertap://?account_id=acc-123&passcode=p%2Fss%2Bword",
		},
		{
			name:   "blank optional fields are omitted",
			config: Config{AccountID: testAccountID, Passcode: testPasscode, Region: "  ", Timezone: ""},
			want:   "clevertap://?account_id=acc-123&passcode=pass-123",
		},
		{
			name:    "missing account_id",
			config:  Config{Passcode: testPasscode},
			wantErr: true,
		},
		{
			name:    "missing passcode",
			config:  Config{AccountID: testAccountID},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := tt.config.GetIngestrURI()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{AccountID: testAccountID, Passcode: testPasscode, Region: "eu1", Timezone: "UTC"})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "clevertap://?account_id=acc-123&passcode=pass-123&region=eu1&timezone=UTC", uri)
}
