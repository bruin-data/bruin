package python

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
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
	task         *pipeline.Asset
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
	config       secretFinder
}

type secretFinder interface {
	GetSecretByKey(key string) (string, error)
}

func NewLocalOperator(config *config.Config, envVariables map[string]string) *LocalOperator {
	cmdRunner := &commandRunner{}
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

func (o *LocalOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	_, ok := ti.(*scheduler.AssetInstance)
	if !ok {
		return errors.New("python assets can only be run as a main task")
	}

	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *LocalOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	repo, err := o.repoFinder.Repo(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Python")
	}

	logger := zap.NewNop().Sugar()
	if ctx.Value(executor.ContextLogger) != nil {
		logger = ctx.Value(executor.ContextLogger).(*zap.SugaredLogger)
	}

	logger.Debugf("running Python task %s in repo %s", t.Name, repo.Path)

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

	envVariables := o.envVariables
	if envVariables == nil {
		envVariables = make(map[string]string)
	}
	envVariables["BRUIN_ASSET"] = t.Name

	for _, mapping := range t.Secrets {
		val, err := o.config.GetSecretByKey(mapping.SecretKey)
		if err != nil {
			return errors.Wrapf(err, "there's no secret with the name '%s', make sure you are referring to the right secret and the secret is defined correctly in your .bruin.yml file.", mapping.SecretKey)
		}

		if val == "" {
			return errors.New(fmt.Sprintf("there's no secret with the name '%s', make sure you are referring to the right secret and the secret is defined correctly in your .bruin.yml file.", mapping.SecretKey))
		}

		envVariables[mapping.InjectedKey] = val
	}

	err = o.runner.Run(ctx, &executionContext{
		repo:            repo,
		module:          module,
		requirementsTxt: requirementsTxt,
		pipeline:        p,
		task:            t,
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
