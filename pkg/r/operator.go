package r

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/env"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	logger2 "github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type executionContext struct {
	repo     *git.Repo
	renvLock string

	envVariables map[string]string
	pipeline     *pipeline.Pipeline
	asset        *pipeline.Asset
}

type renvPathFinder interface {
	FindRenvLockInPath(path string, executable *pipeline.ExecutableFile) (string, error)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type localRunner interface {
	Run(ctx context.Context, execution *executionContext) error
}

type LocalOperator struct {
	repoFinder   repoFinder
	renvFinder   renvPathFinder
	runner       localRunner
	envVariables map[string]string
	config       config.ConnectionDetailsGetter
}

func NewLocalOperator(config config.ConnectionAndDetailsGetter, envVariables map[string]string) (*LocalOperator, error) {
	cmdRunner := &CommandRunner{}

	pathToRscript, err := findPathToExecutable([]string{"Rscript"})
	if err != nil {
		return nil, errors.New(`R is not installed or not in PATH.

Please install R to use R assets:
  • macOS: brew install r
  • Ubuntu/Debian: sudo apt-get install r-base
  • Windows: Download from https://cran.r-project.org/bin/windows/base/

After installation, make sure 'Rscript' is available in your PATH.`)
	}

	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		renvFinder: &RenvPathFinder{},
		runner: &localRRunner{
			cmd:           cmdRunner,
			renvInstaller: &RenvInstaller{cmd: cmdRunner},
			pathToRscript: pathToRscript,
		},
		envVariables: envVariables,
		config:       config,
	}, nil
}

func (o *LocalOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	_, ok := ti.(*scheduler.AssetInstance)
	if !ok {
		return errors.New("r assets can only be run as a main asset")
	}

	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *LocalOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	repo, err := o.repoFinder.Repo(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run R")
	}

	var ctxWithLogger context.Context
	if ctx.Value(executor.ContextLogger) == nil {
		logger := zap.NewNop().Sugar()
		ctxWithLogger = context.WithValue(ctx, executor.ContextLogger, logger)
	} else {
		ctxWithLogger = ctx
	}

	logger := ctxWithLogger.Value(executor.ContextLogger).(logger2.Logger)

	logger.Debugf("running R asset %s in repo %s", t.Name, repo.Path)

	renvLock, err := o.renvFinder.FindRenvLockInPath(repo.Path, &t.ExecutableFile)
	if err != nil {
		var noRenvError *NoRenvFoundError
		switch {
		case !errors.As(err, &noRenvError):
			return errors.Wrap(err, "failed to find renv.lock")
		default:
			//
		}
	}

	// Create a copy of environment variables to avoid race conditions when multiple goroutines
	// are running concurrently and modifying the same map
	envCopy := make(map[string]string, len(o.envVariables))
	for k, v := range o.envVariables {
		envCopy[k] = v
	}

	perAssetEnvVariables, err := env.SetupVariables(ctx, p, t, envCopy)
	if err != nil {
		return errors.Wrap(err, "failed to setup environment variables")
	}

	envVariables := make(map[string]string)
	for k, v := range perAssetEnvVariables {
		envVariables[k] = v
	}
	envVariables["BRUIN_ASSET"] = t.Name
	envVariables["BRUIN_THIS"] = t.Name

	for _, mapping := range t.Secrets {
		conn := o.config.GetConnectionDetails(mapping.SecretKey)
		if conn == nil {
			return errors.New(fmt.Sprintf("there's no secret with the name '%s'.", mapping.SecretKey))
		}

		val, ok := conn.(*config.GenericConnection)
		if ok {
			envVariables[mapping.InjectedKey] = val.Value
			continue
		}

		res, err := json.Marshal(conn)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal connection")
		}
		envVariables[mapping.InjectedKey] = string(res)
	}

	err = o.runner.Run(ctx, &executionContext{
		repo:         repo,
		renvLock:     renvLock,
		pipeline:     p,
		asset:        t,
		envVariables: envVariables,
	})
	if err != nil {
		return errors.Wrap(err, "failed to execute R script")
	}

	return nil
}

func findPathToExecutable(alternatives []string) (string, error) {
	for _, alternative := range alternatives {
		path, err := exec.LookPath(alternative)
		if err == nil {
			return path, nil
		}
	}

	return "", errors.New("no executable found for alternatives: " + strings.Join(alternatives, ", "))
}
