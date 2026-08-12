package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"
	"syscall"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/go-viper/mapstructure/v2"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

type (
	curlConnectionLookup         func(string) (any, error)
	curlConnectionLookupResolver func(context.Context, *cli.Command) (curlConnectionLookup, error)
	curlExecutor                 func(context.Context, []string, io.Reader, io.Writer, io.Writer) error
)

// Curl proxies arguments to the installed curl executable after rendering each
// argument with Bruin connections. Curl arguments must follow -- so that curl's
// option surface can evolve independently of Bruin.
func Curl() *cli.Command {
	return newCurlCommand(resolveCurlConnectionLookup, executeCurl)
}

func newCurlCommand(resolveConnections curlConnectionLookupResolver, execute curlExecutor) *cli.Command {
	return &cli.Command{
		Name:      "curl",
		Usage:     "run curl with arguments rendered from Bruin connections",
		ArgsUsage: "-- [curl options and URLs]",
		Description: "Passes every argument after -- directly to the installed curl executable after Jinja rendering, without inspecting curl options. " +
			"Build the URL, headers, and body from the connection — do not hardcode hosts, project ids, or tokens. " +
			"Connection fields are available as {{ bruin.connection(\"name\").field }}; run `bruin connections list` for field names.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"env"},
				Usage:   "the environment to use, ignored when a secrets backend is selected",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file, ignored when a secrets backend is selected",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			root := c.Root()

			curlArgs := c.Args().Slice()
			if len(curlArgs) == 0 {
				return failCurlCommand(root, errors.New("at least one curl option or URL is required after --"))
			}

			connectionLookup, err := resolveConnections(ctx, c)
			if err != nil {
				return failCurlCommand(root, err)
			}

			renderedArgs, err := renderCurlArgs(curlArgs, connectionLookup)
			if err != nil {
				return failCurlCommand(root, err)
			}

			if err := execute(ctx, renderedArgs, root.Reader, root.Writer, root.ErrWriter); err != nil {
				var exitError *exec.ExitError
				if errors.As(err, &exitError) {
					return cli.Exit("", curlExitCode(exitError))
				}
				return failCurlCommand(root, errors.Wrap(err, "failed to execute curl"))
			}

			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

// failCurlCommand reports the error on stderr and exits with 1. curl owns stdout,
// so Bruin's own diagnostics must never be mixed into the response body, and the
// message stays uncolored because this command is meant to be scripted.
func failCurlCommand(root *cli.Command, err error) error {
	fmt.Fprintf(root.ErrWriter, "%v\n", err)
	return cli.Exit("", 1)
}

// curlExitCode reports curl's own exit code, mapping a signalled curl onto the
// 128+signal convention that a shell would have reported for curl itself.
func curlExitCode(exitError *exec.ExitError) int {
	code := exitError.ExitCode()
	if code >= 0 {
		return code
	}
	if status, ok := exitError.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return 128 + int(status.Signal())
	}
	return 1
}

func resolveCurlConnectionLookup(ctx context.Context, c *cli.Command) (curlConnectionLookup, error) {
	var lookup curlConnectionLookup
	return func(name string) (any, error) {
		if lookup == nil {
			resolved, err := loadCurlConnectionLookup(ctx, c)
			if err != nil {
				return nil, err
			}
			lookup = resolved
		}
		return lookup(name)
	}, nil
}

// loadCurlConnectionLookup is deferred until a template actually calls
// bruin.connection(), so plain curl passthrough and built-in-only templates do
// not require a repository or create a .bruin.yml file.
func loadCurlConnectionLookup(ctx context.Context, c *cli.Command) (curlConnectionLookup, error) {
	ctx, cm, err := ingestrURIConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	if secretsBackendFromContext(ctx) == "" {
		connections := cm.SelectedEnvironment.Connections
		return func(name string) (any, error) {
			details := connections.GetConnection(name)
			if details == nil {
				return nil, config.NewConnectionNotFoundError(ctx, "", name)
			}
			return details, nil
		}, nil
	}

	manager, errs := connectionManagerFromConfig(ctx, cm, makeLogger(false))
	if len(errs) > 0 {
		return nil, errors.Wrap(errs[0], "failed to create connection manager")
	}

	return func(name string) (any, error) {
		details := manager.GetConnectionDetails(name)
		if details == nil {
			return nil, config.NewConnectionNotFoundError(ctx, "", name)
		}
		return details, nil
	}, nil
}

func renderCurlArgs(args []string, lookupConnection curlConnectionLookup) ([]string, error) {
	connectionCache := make(map[string]map[string]any)
	var connectionLookupErr error

	bruinContext := jinja.BuiltinFunctions()
	bruinContext["connection"] = func(name string) map[string]any {
		if connectionLookupErr != nil {
			return nil
		}
		if fields, ok := connectionCache[name]; ok {
			return fields
		}

		details, err := lookupConnection(name)
		if err != nil {
			connectionLookupErr = err
			return nil
		}
		fields, err := connectionFields(details)
		if err != nil {
			connectionLookupErr = errors.Wrapf(err, "failed to prepare connection %q for rendering", name)
			return nil
		}

		connectionCache[name] = fields
		return fields
	}

	renderer := jinja.NewRenderer(jinja.Context{"bruin": bruinContext})
	rendered := make([]string, len(args))
	for i, arg := range args {
		var err error
		rendered[i], err = renderCurlArgument(renderer, arg)
		if connectionLookupErr != nil {
			return nil, errors.Wrapf(connectionLookupErr, "failed to render curl argument %d", i+1)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "failed to render curl argument %d", i+1)
		}
	}

	return rendered, nil
}

// curl allows a variable to chain several functions, each separated by a colon,
// e.g. {{name:trim:url}}.
var curlVariableExpression = regexp.MustCompile(`\{\{[[:alnum:]_]+(?::[[:alnum:]_,]+)*\}\}`)

func renderCurlArgument(renderer *jinja.Renderer, arg string) (string, error) {
	curlVariables := curlVariableExpression.FindAllString(arg, -1)
	protected := arg
	placeholders := make([]string, len(curlVariables))
	for i, variable := range curlVariables {
		placeholder, err := newCurlVariablePlaceholder(protected)
		if err != nil {
			return "", err
		}
		placeholders[i] = placeholder
		protected = strings.Replace(protected, variable, placeholder, 1)
	}

	rendered, err := renderer.Render(protected)
	if err != nil {
		return "", err
	}
	for i, variable := range curlVariables {
		rendered = strings.ReplaceAll(rendered, placeholders[i], variable)
	}
	return rendered, nil
}

func newCurlVariablePlaceholder(protected string) (string, error) {
	var token [16]byte
	for {
		if _, err := rand.Read(token[:]); err != nil {
			return "", errors.Wrap(err, "failed to protect curl variable")
		}
		placeholder := "__BRUIN_CURL_VARIABLE_" + hex.EncodeToString(token[:]) + "__"
		if !strings.Contains(protected, placeholder) {
			return placeholder, nil
		}
	}
}

func connectionFields(details any) (map[string]any, error) {
	fields := make(map[string]any)
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:  &fields,
		TagName: "mapstructure",
		Squash:  true,
		Deep:    true,
	})
	if err != nil {
		return nil, err
	}
	if err := decoder.Decode(details); err != nil {
		return nil, err
	}

	return fields, nil
}

func executeCurl(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	command := exec.CommandContext(ctx, "curl", args...) //nolint:gosec
	command.Stdin = stdin
	command.Stdout = stdout
	command.Stderr = stderr
	return command.Run()
}
