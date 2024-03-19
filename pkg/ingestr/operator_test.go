package ingestr

import (
	"context"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

type mockClient struct {
	client.Client
	pullErrors   bool
	readerErrors bool
}

func (m *mockClient) ImagePull(ctx context.Context, refStr string, options types.ImagePullOptions) (io.ReadCloser, error) {
	if m.pullErrors {
		return nil, errors.New("pull error")
	}

	if m.readerErrors {
		iotest.ErrReader(errors.New("reader error"))
	}
	return io.NopCloser(strings.NewReader("Hello, world!")), nil
}

func TestNewBasicOperator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		Name      string
		Initiator clientInitiator
		wantError bool
	}{
		{
			Name: "initiator returns error",
			Initiator: func(ops ...client.Opt) (client.CommonAPIClient, error) {
				return nil, errors.New("init error")
			},
			wantError: true,
		},
		{
			Name: "pull returns error",
			Initiator: func(ops ...client.Opt) (client.CommonAPIClient, error) {
				return &mockClient{pullErrors: true}, errors.New("test error")
			},
			wantError: true,
		},
		{
			Name: "reader returns error",
			Initiator: func(ops ...client.Opt) (client.CommonAPIClient, error) {
				return &mockClient{readerErrors: true}, errors.New("test error")
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()
			operator, err := NewBasicOperator(nil, tt.Initiator)
			if tt.wantError {
				require.Error(t, err)
			} else {
				assert.NotNil(t, operator)
				require.NoError(t, err)
			}
		})
	}
}
