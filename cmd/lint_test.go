package cmd

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_unwrapAllErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "nil error",
			err:  nil,
			want: []string{},
		},
		{
			name: "single error",
			err:  errors.New("single error"),
			want: []string{"single error"},
		},
		{
			name: "nested errors",
			err:  errors.Wrap(errors.Wrapf(errors.New("inner error"), "%s error", "middle"), "outer error"),
			want: []string{"outer error", "middle error", "inner error"},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := unwrapAllErrors(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}
