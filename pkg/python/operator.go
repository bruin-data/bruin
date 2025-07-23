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

	envVariables map[string]string
	pipeline     *pipeline.Pipeline
	asset        *pipeline.Asset
}

type modulePathFinder interface {
	FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error)
	FindRequirementsTxtInPath(path string, executable *pipeline.ExecutableFile) (string, error)
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
	config       config.ConnectionGetter
}

func NewLocalOperator(config config.ConnectionGetter, envVariables map[string]string) *LocalOperator {
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

func NewLocalOperatorWithUv(config config.ConnectionGetter, conn config.ConnectionGetter, envVariables map[string]string) *LocalOperator {
	cmdRunner := &CommandRunner{}

	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		module:     &ModulePathFinder{},
		runner: &UvPythonRunner{
			Cmd: cmdRunner,
			UvInstaller: &UvChecker{
				cmd: CommandRunner{},
			},
			conn: conn,
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

	requirementsTxt, err := o.module.FindRequirementsTxtInPath(repo.Path, &t.ExecutableFile)
	if err != nil {
		var noReqsError *NoRequirementsFoundError
		switch {
		case !errors.As(err, &noReqsError):
			return errors.Wrap(err, "failed to find requirements.txt")
		default:
			//
		}
	}

	perAssetEnvVariables, err := env.SetupVariables(ctx, p, t, o.envVariables)
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
		conn := o.config.GetConnection(mapping.SecretKey)
		if conn == nil {
			return errors.New(fmt.Sprintf("there's no secret with the name '%s'.", mapping.SecretKey))
		}

		val, ok := conn.(*config.GenericConnection)
		if ok {
			envVariables[mapping.InjectedKey] = val.Value
			continue
		}

		// TODO this is hacky, we should make config comply with that interface from the beginning
		detailsGetter, ok := o.config.(config.ConnectionDetailsGetter)
		if !ok {
			return errors.New(fmt.Sprintf("could not get details for connection '%s'.", mapping.SecretKey))
		}
		details := detailsGetter.GetConnectionDetails(mapping.SecretKey)

		res, err := json.Marshal(details)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal connection")
		}
		envVariables[mapping.InjectedKey] = string(res)
	}

	err = o.runner.Run(ctx, &executionContext{
		repo:            repo,
		module:          module,
		requirementsTxt: requirementsTxt,
		pipeline:        p,
		asset:           t,
		envVariables:    envVariables,
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
