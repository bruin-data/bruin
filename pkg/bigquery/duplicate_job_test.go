package bigquery

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
)

func TestDuplicateJob(t *testing.T) {
	t.Parallel()

	conflict := func(msg string) error { return &googleapi.Error{Code: 409, Message: msg} }
	tests := []struct {
		name         string
		err          error
		prefix       string
		wantID       string
		wantLocation string
		wantOK       bool
	}{
		{
			name:         "message as BigQuery returns it",
			err:          conflict("Already Exists: Job buluttan-production:europe-west1.bruin_main-2eSTkB8HNhPfRYDY6pUxWteiP6P"),
			prefix:       "bruin_main",
			wantID:       "bruin_main-2eSTkB8HNhPfRYDY6pUxWteiP6P",
			wantLocation: "europe-west1",
			wantOK:       true,
		},
		{
			name:         "trailing reason",
			err:          conflict("Already Exists: Job p:US.bruin_main-abc, duplicate"),
			prefix:       "bruin_main",
			wantID:       "bruin_main-abc",
			wantLocation: "US",
			wantOK:       true,
		},
		{
			name:         "domain-scoped project",
			err:          conflict("Already Exists: Job example.com:project:EU.bruin_sensor-abc"),
			prefix:       "bruin_sensor",
			wantID:       "bruin_sensor-abc",
			wantLocation: "EU",
			wantOK:       true,
		},
		{name: "another prefix's job", err: conflict("Already Exists: Job p:US.bruin_column-abc"), prefix: "bruin_main"},
		{name: "no prefix set", err: conflict("Already Exists: Job p:US.job-abc"), prefix: ""},
		{name: "not a conflict", err: &googleapi.Error{Code: 404, Message: "Not found: Job p:US.bruin_main-abc"}, prefix: "bruin_main"},
		{name: "unrecognized message", err: conflict("Something else entirely"), prefix: "bruin_main"},
		{name: "not an API error", err: errors.New("Already Exists: Job p:US.bruin_main-abc"), prefix: "bruin_main"},
		{name: "no error", err: nil, prefix: "bruin_main"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id, location, ok := duplicateJob(tt.err, tt.prefix)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantLocation, location)
		})
	}
}
