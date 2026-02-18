package python

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
	"github.com/bruin-data/bruin/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type executionContext struct {
	repo            *git.Repo
	module          string
	requirementsTxt string

	// dependencyConfig holds the full dependency configuration including
	// pyproject.toml and uv.lock support for uv-based workflows.
	dependencyConfig *DependencyConfig

	envVariables map[string]string
	pipeline     *pipeline.Pipeline
	asset        *pipeline.Asset
}

type modulePathFinder interface {
	FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error)
	FindRequirementsTxtInPath(path string, executable *pipeline.ExecutableFile) (string, error)
	FindDependencyConfig(path string, executable *pipeline.ExecutableFile) (*DependencyConfig, error)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type localRunner interface {
	Run(ctx context.Context, execution *executionContext) error
}

type LocalOperator struct {
	repoFinder   repoFinder
	module       modulePathFinder
	runner       localRunner
	envVariables map[string]string
	config       config.ConnectionDetailsGetter
}

func NewLocalOperator(config config.ConnectionAndDetailsGetter, envVariables map[string]string) *LocalOperator {
	cmdRunner := &CommandRunner{}
	fs := afero.NewOsFs()

	pathToPython, err := findPathToExecutable([]string{"python3", "python"})
	if err != nil {
		panic("No executable found for Python, neither 'python3' nor 'python', are you sure Python is installed?")
	}

	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		module:     &ModulePathFinder{},
		runner: &localPythonRunner{
			cmd: cmdRunner,
			requirementsInstaller: &installReqsToHomeDir{
				fs:           fs,
				cmd:          cmdRunner,
				config:       user.NewConfigManager(fs),
				pathToPython: pathToPython,
			},
			pathToPython: pathToPython,
		},
		envVariables: envVariables,
		config:       config,
	}
}

func NewLocalOperatorWithUv(config config.ConnectionAndDetailsGetter, envVariables map[string]string) *LocalOperator {
	cmdRunner := &CommandRunner{}

	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		module:     &ModulePathFinder{},
		runner: &UvPythonRunner{
			Cmd:         cmdRunner,
			UvInstaller: &UvChecker{},
			conn:        config,
		},
		envVariables: envVariables,
		config:       config,
	}
}

func (o *LocalOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	_, ok := ti.(*scheduler.AssetInstance)
	if !ok {
		return errors.New("python assets can only be run as a main asset")
	}

	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *LocalOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	repo, err := o.repoFinder.Repo(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Python")
	}

	var ctxWithLogger context.Context
	if ctx.Value(executor.ContextLogger) == nil {
		logger := zap.NewNop().Sugar()
		ctxWithLogger = context.WithValue(ctx, executor.ContextLogger, logger)
	} else {
		ctxWithLogger = ctx
	}

	logger := ctxWithLogger.Value(executor.ContextLogger).(logger2.Logger)

	logger.Debugf("running Python asset %s in repo %s", t.Name, repo.Path)

	module, err := o.module.FindModulePath(repo, &t.ExecutableFile)
	if err != nil {
		logger.Debugf("failed to find module path for asset, repo: %s - executable: %s", repo.Path, t.ExecutableFile.Path)
		return errors.Wrap(err, "failed to build a module path")
	}

	logger.Debugf("using module path: %s", module)

	// Find dependency configuration (supports requirements.txt, pyproject.toml, and uv.lock)
	depConfig, err := o.module.FindDependencyConfig(repo.Path, &t.ExecutableFile)
	if err != nil {
		logger.Debugf("failed to find dependency config: %v", err)
		// Non-fatal: continue without dependencies
		depConfig = &DependencyConfig{Type: DependencyTypeNone}
	}

	// Extract requirementsTxt for backward compatibility
	var requirementsTxt string
	if depConfig != nil && depConfig.Type == DependencyTypeRequirementsTxt {
		requirementsTxt = depConfig.RequirementsTxt
	}

	// Log which dependency method is being used
	switch depConfig.Type {
	case DependencyTypePyproject:
		logger.Debugf("using pyproject.toml from %s", depConfig.ProjectRoot)
	case DependencyTypeRequirementsTxt:
		logger.Debugf("using requirements.txt from %s", depConfig.RequirementsTxt)
	case DependencyTypeNone:
		logger.Debugf("no dependency configuration found, running without dependencies")
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
	if t.Connection != "" {
		envVariables["BRUIN_CONNECTION"] = t.Connection
	}

	connectionTypes := make(map[string]string)
	for _, mapping := range t.Secrets {
		conn := o.config.GetConnectionDetails(mapping.SecretKey)
		if conn == nil {
			return errors.New(fmt.Sprintf("there's no secret with the name '%s'.", mapping.SecretKey))
		}

		connType := o.config.GetConnectionType(mapping.SecretKey)
		if connType != "" {
			connectionTypes[mapping.InjectedKey] = connType
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

	if len(connectionTypes) > 0 {
		typesJSON, err := json.Marshal(connectionTypes)
		if err != nil {
			return errors.Wrap(err, "failed to marshal connection types")
		}
		envVariables["BRUIN_CONNECTION_TYPES"] = string(typesJSON)
	}

	err = o.runner.Run(ctx, &executionContext{
		repo:             repo,
		module:           module,
		requirementsTxt:  requirementsTxt,
		dependencyConfig: depConfig,
		pipeline:         p,
		asset:            t,
		envVariables:     envVariables,
	})
	if err != nil {
		return errors.Wrap(err, "failed to execute Python script")
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
