package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"go.uber.org/zap"
)

// Mock implementations
type mockAssetCounter struct {
	count int
}

func (m *mockAssetCounter) GetAssetCountWithTasksPending() int {
	return m.count
}

func TestValidate_ShouldValidateFalse_ReturnsNil(t *testing.T) {
	t.Parallel()
	err := Validate(false, &mockAssetCounter{count: 1}, nil, context.Background(), &pipeline.Pipeline{}, "path", zap.NewNop().Sugar())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidate_AssetsPending_CallsLintCheckerWithFalse(t *testing.T) {
	t.Parallel()
	called := false
	var validateOnlyAssetLevel bool

	lintChecker := func(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, voal bool) error {
		called = true
		validateOnlyAssetLevel = voal
		return nil
	}

	err := Validate(true, &mockAssetCounter{count: 2}, lintChecker, context.Background(), &pipeline.Pipeline{}, "path", zap.NewNop().Sugar())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !called {
		t.Error("expected lintChecker to be called")
	}
	if validateOnlyAssetLevel {
		t.Error("expected validateOnlyAssetLevel to be false when assets are pending")
	}
}

func TestValidate_NoAssetsPending_CallsLintCheckerWithTrue(t *testing.T) {
	t.Parallel()
	called := false
	var validateOnlyAssetLevel bool

	lintChecker := func(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, voal bool) error {
		called = true
		validateOnlyAssetLevel = voal
		return nil
	}

	err := Validate(true, &mockAssetCounter{count: 0}, lintChecker, context.Background(), &pipeline.Pipeline{}, "path", zap.NewNop().Sugar())
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if !called {
		t.Error("expected lintChecker to be called")
	}
	if !validateOnlyAssetLevel {
		t.Error("expected validateOnlyAssetLevel to be true when no assets are pending")
	}
}

func TestValidate_LintCheckerReturnsError_PropagatesError(t *testing.T) {
	t.Parallel()
	expectedErr := errors.New("lint error")
	lintChecker := func(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, voal bool) error {
		return expectedErr
	}

	err := Validate(true, &mockAssetCounter{count: 0}, lintChecker, context.Background(), &pipeline.Pipeline{}, "path", zap.NewNop().Sugar())
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
}
