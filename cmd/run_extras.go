package cmd

import (
	"context"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type assetCounter interface {
	GetAssetCountWithTasksPending() int
}

type lintChecker func(ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger, validateOnlyAssetLevel bool) error

func Validate(shouldvalidate bool, assetCounter assetCounter, lintChecker lintChecker, ctx context.Context, foundPipeline *pipeline.Pipeline, pipelinePath string, logger logger.Logger) error {
	if !shouldvalidate {
		return nil
	}

	if assetCounter.GetAssetCountWithTasksPending() > 0 {
		return lintChecker(ctx, foundPipeline, pipelinePath, logger, false)
	}

	return lintChecker(ctx, foundPipeline, pipelinePath, logger, true)
}
