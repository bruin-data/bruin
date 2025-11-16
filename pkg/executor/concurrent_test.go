package executor

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConcurrent_Start(t *testing.T) {
	t.Parallel()

	t11 := &pipeline.Asset{
		Name: "task11",
		Type: "test",
	}

	t21 := &pipeline.Asset{
		Name: "task21",
		Type: "test",
	}

	t12 := &pipeline.Asset{
		Name: "task12",
		Type: "test",
		Upstreams: []pipeline.Upstream{
			{Value: "task11", Type: "asset"},
		},
	}

	t22 := &pipeline.Asset{
		Name: "task22",
		Type: "test",
		Upstreams: []pipeline.Upstream{
			{Value: "task21", Type: "asset"},
		},
	}

	t3 := &pipeline.Asset{
		Name: "task3",
		Type: "test",
		Upstreams: []pipeline.Upstream{
			{Value: "task12", Type: "asset"},
			{Value: "task22", Type: "asset"},
		},
	}

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{t11, t21, t12, t22, t3},
	}

	mockOperator := new(mockOperator)
	for _, a := range p.Assets {
		mockOperator.On("Run", mock.Anything, mock.MatchedBy(func(ti scheduler.TaskInstance) bool {
			return ti.GetAsset().Name == a.Name
		})).
			Return(nil).
			Once()
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, p, "test")
	assert.Equal(t, 5, s.InstanceCount())

	ops := map[pipeline.AssetType]Config{
		"test": {
			scheduler.TaskInstanceTypeMain: mockOperator,
		},
	}

	ex, err := NewConcurrent(logger, ops, 8, FormattingOptions{})
	require.NoError(t, err)
	ex.Start(t.Context(), s.WorkQueue, s.Results)

	results := s.Run(t.Context())
	assert.Len(t, results, len(p.Assets))

	mockOperator.AssertExpectations(t)
}

func TestWorkerWriter_Write(t *testing.T) {
	t.Parallel()

	task := &pipeline.Asset{
		Name: "test-task",
	}

	sprintfFunc := fmt.Sprintf

	tests := []struct {
		name                string
		input               []byte
		doNotLogTimestamp   bool
		doNotLogTaskName    bool
		expectedContains    []string
		expectedNotContains []string
	}{
		{
			name:              "with timestamp and task name",
			input:             []byte("hello world\n"),
			doNotLogTimestamp: false,
			doNotLogTaskName:  false,
			expectedContains:  []string{"[", "]", "[test-task]", "hello world\n"},
		},
		{
			name:                "without timestamp, with task name",
			input:               []byte("hello world\n"),
			doNotLogTimestamp:   true,
			doNotLogTaskName:    false,
			expectedContains:    []string{"[test-task]", "hello world\n"},
			expectedNotContains: []string{"[20", "[21", "[22", "[23"},
		},
		{
			name:                "with timestamp, without task name",
			input:               []byte("hello world\n"),
			doNotLogTimestamp:   false,
			doNotLogTaskName:    true,
			expectedContains:    []string{"[", "]", "hello world\n"},
			expectedNotContains: []string{"[test-task]"},
		},
		{
			name:                "without timestamp and task name",
			input:               []byte("hello world\n"),
			doNotLogTimestamp:   true,
			doNotLogTaskName:    true,
			expectedContains:    []string{"hello world\n"},
			expectedNotContains: []string{"[test-task]", "[20", "[21", "[22", "[23"},
		},
		{
			name:              "empty input",
			input:             []byte(""),
			doNotLogTimestamp: true,
			doNotLogTaskName:  true,
			expectedContains:  []string{""},
		},
		{
			name:              "multiline input",
			input:             []byte("line1\nline2\n"),
			doNotLogTimestamp: true,
			doNotLogTaskName:  false,
			expectedContains:  []string{"[test-task]", "line1\nline2\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer

			w := &workerWriter{
				w:                 &buf,
				task:              task,
				sprintfFunc:       sprintfFunc,
				DoNotLogTimestamp: tt.doNotLogTimestamp,
				DoNotLogTaskName:  tt.doNotLogTaskName,
			}

			n, err := w.Write(tt.input)

			require.NoError(t, err)
			assert.Equal(t, len(tt.input), n)

			output := buf.String()
			for _, expected := range tt.expectedContains {
				assert.Contains(t, output, expected, "output should contain: %q", expected)
			}
			for _, notExpected := range tt.expectedNotContains {
				assert.NotContains(t, output, notExpected, "output should not contain: %q", notExpected)
			}
		})
	}
}

func TestWorkerWriter_Write_WriteError(t *testing.T) {
	t.Parallel()

	task := &pipeline.Asset{
		Name: "test-task",
	}

	sprintfFunc := fmt.Sprintf

	mockWriter := &mockWriter{
		writeError: assert.AnError,
	}

	w := &workerWriter{
		w:                 mockWriter,
		task:              task,
		sprintfFunc:       sprintfFunc,
		DoNotLogTimestamp: true,
		DoNotLogTaskName:  true,
	}

	n, err := w.Write([]byte("test"))

	require.Error(t, err)
	assert.Equal(t, assert.AnError, err)
	assert.Equal(t, 0, n)
}

func TestWorkerWriter_Write_ShortWrite(t *testing.T) {
	t.Parallel()

	task := &pipeline.Asset{
		Name: "test-task",
	}

	sprintfFunc := fmt.Sprintf

	mockWriter := &mockWriter{
		shortWrite: true,
	}

	w := &workerWriter{
		w:                 mockWriter,
		task:              task,
		sprintfFunc:       sprintfFunc,
		DoNotLogTimestamp: true,
		DoNotLogTaskName:  true,
	}

	n, err := w.Write([]byte("test"))

	require.Error(t, err)
	assert.Equal(t, io.ErrShortWrite, err)
	assert.Equal(t, 2, n) // mockWriter writes half the bytes
}

type mockWriter struct {
	writeError error
	shortWrite bool
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.writeError != nil {
		return 0, m.writeError
	}
	if m.shortWrite {
		return len(p) / 2, io.ErrShortWrite
	}
	return len(p), nil
}
