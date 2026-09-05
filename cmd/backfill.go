package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bruin-data/bruin/pkg/backfill"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/google/uuid"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

// Reuse run's flag definitions so defaults, aliases and variable parsing stay in
// sync. Stateful/interactive modes and full refresh do not compose with partitions.
var backfillRunFlags = []string{
	"environment", "config-file", "selector", "tag", "exclude-tag", "downstream",
	"var", "variant", "only", "force", "push-metadata", "apply-interval-modifiers",
	"sensor-mode", "no-validation", "no-timestamp", "no-color", "verbose",
	"mask-credentials", "timeout", "query-annotations",
}

func Backfill(isDebug *bool) *cli.Command {
	flags := []cli.Flag{
		&cli.StringFlag{Name: "start-date", Usage: "first date or timestamp to process"},
		&cli.StringFlag{Name: "end-date", Usage: "inclusive last date, or exclusive end timestamp"},
		&cli.StringFlag{Name: "partition", Value: "daily", Usage: "hourly, daily, weekly, monthly, yearly, or a Go duration (e.g. 6h)"},
		&cli.StringFlag{Name: "timezone", Value: "UTC", Usage: "IANA timezone for calendar boundaries and unzoned inputs"},
		&cli.IntFlag{Name: "max-parallel", Value: 1, Usage: "maximum active partitions (may be reduced by connection limits)"},
		&cli.IntFlag{Name: "workers", Value: 16, Usage: "maximum asset workers per child run"},
		&cli.IntFlag{Name: "retries", Usage: "additional attempts per selected partition in this invocation"},
		&cli.StringFlag{Name: "on-failure", Value: "stop", Usage: "continue, stop (drain active runs), or fail-fast (cancel active runs)"},
		&cli.StringFlag{Name: "continue", Usage: "resume a persisted backfill ID"},
		&cli.StringFlag{Name: "rerun", Usage: "select failed, missing (queued/interrupted), or all partitions; default: failed and missing"},
		&cli.BoolFlag{Name: "reverse", Usage: "process newest partitions first"},
		&cli.BoolFlag{Name: "dry-run", Usage: "print the plan and current partition state without executing or writing records"},
		&cli.StringFlag{Name: "output", Value: "text", Usage: "text or json (use --dry-run to emit just the plan)"},
		&cli.IntFlag{Name: "limit", Value: 1000, Usage: "maximum partitions to print (0 prints all); execution always covers the full plan"},
		&cli.IntFlag{Name: "offset", Usage: "number of partitions to skip in plan/state output"},
		&cli.StringFlag{Name: "state-dir", Usage: "backfill store directory (default: <repository>/logs/backfills)"},
	}
	for _, f := range Run(isDebug).Flags {
		if slices.Contains(backfillRunFlags, f.Names()[0]) {
			// Read environment-backed options only when creating a backfill.
			// On resume the persisted values take precedence over today's env.
			switch flag := f.(type) {
			case *cli.StringFlag:
				flag.Sources = cli.ValueSourceChain{}
			case *cli.StringSliceFlag:
				flag.Sources = cli.ValueSourceChain{}
			}
			flags = append(flags, f)
		}
	}
	return &cli.Command{
		Name: "backfill", Usage: "run resumable, partitioned local backfills", ArgsUsage: "[pipeline directory or asset file]", Flags: flags, DisableSliceFlagSeparator: true,
		Action: func(ctx context.Context, c *cli.Command) error { return runBackfill(ctx, c, *isDebug) },
	}
}

func backfillFlags(c *cli.Command) map[string][]string {
	values := make(map[string][]string)
	for _, f := range c.Flags {
		name := f.Names()[0]
		if !slices.Contains(backfillRunFlags, name) {
			continue
		}
		switch f.(type) {
		case *cli.BoolFlag:
			values[name] = []string{strconv.FormatBool(c.Bool(name))}
		case *cli.IntFlag:
			values[name] = []string{strconv.Itoa(c.Int(name))}
		case *cli.StringSliceFlag:
			values[name] = c.StringSlice(name)
		default:
			values[name] = []string{c.String(name)}
		}
	}
	return values
}

func runBackfill(ctx context.Context, c *cli.Command, debug bool) error {
	if c.Args().Len() > 1 {
		return fmt.Errorf("backfill accepts one pipeline or asset path; use --selector to select multiple assets")
	}
	if c.String("output") != "text" && c.String("output") != "json" {
		return fmt.Errorf("output must be text or json")
	}
	page := backfillPage{Offset: c.Int("offset"), Limit: c.Int("limit")}
	if page.Offset < 0 || page.Limit < 0 {
		return fmt.Errorf("offset and limit must not be negative")
	}
	options := backfill.Options{MaxParallel: c.Int("max-parallel"), Workers: c.Int("workers"), Retries: c.Int("retries"), OnFailure: c.String("on-failure"), Rerun: c.String("rerun"), Reverse: c.Bool("reverse")}
	if err := options.Validate(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	target := c.Args().First()
	if target == "" {
		target = "."
	}
	target, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	root := c.String("state-dir")
	if root == "" {
		repo, err := git.FindRepoFromPath(target)
		if err != nil {
			return err
		}
		root = filepath.Join(repo.Path, "logs", "backfills")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return err
	}
	id := c.String("continue")
	continuing := id != ""
	if !continuing {
		id = uuid.NewString()
	}
	store, err := backfill.Open(root, id)
	if err != nil {
		return err
	}
	var m backfill.Manifest
	if continuing {
		m, err = store.Manifest()
		if err != nil {
			return err
		}
		if c.Args().Len() > 0 && target != m.Plan.Target {
			return fmt.Errorf("target does not match saved backfill %s", id)
		}
		for _, name := range append([]string{"start-date", "end-date", "partition", "timezone"}, backfillRunFlags...) {
			if c.IsSet(name) {
				return fmt.Errorf("--%s cannot be changed with --continue; the saved backfill inputs are reused", name)
			}
		}
	} else {
		for name, env := range map[string]string{"config-file": "BRUIN_CONFIG_FILE", "var": "BRUIN_VARS", "query-annotations": "BRUIN_QUERY_ANNOTATIONS"} {
			if !c.IsSet(name) && os.Getenv(env) != "" {
				if err := c.Set(name, os.Getenv(env)); err != nil {
					return err
				}
			}
		}
		if _, err := os.Stat(target); err != nil {
			return err
		}
		if c.String("start-date") == "" || c.String("end-date") == "" {
			return fmt.Errorf("start-date and end-date are required for a new backfill")
		}
		start, end, err := backfill.ParseRange(c.String("start-date"), c.String("end-date"), c.String("timezone"))
		if err != nil {
			return err
		}
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		m = backfill.Manifest{Version: backfill.Version, ID: id, CreatedAt: time.Now().UTC(), Plan: backfill.Plan{Target: target, WorkingDirectory: cwd, RunFlags: backfillFlags(c), Start: start, End: end, Timezone: c.String("timezone"), Partition: c.String("partition")}}
		configPath := c.String("config-file")
		if configPath == "" {
			repo, err := git.FindRepoFromPath(target)
			if err != nil {
				return err
			}
			configPath = filepath.Join(repo.Path, ".bruin.yml")
		}
		configPath, err = filepath.Abs(configPath)
		if err != nil {
			return err
		}
		m.Plan.RunFlags["config-file"] = []string{configPath}
		if backend := secretsBackendFromContext(ctx); backend != "" {
			m.Plan.RunFlags[secretsBackendFlagName] = []string{backend}
		}
	}
	if err := m.Plan.Validate(); err != nil {
		return err
	}
	configPaths := m.Plan.RunFlags["config-file"]
	if len(configPaths) != 1 || configPaths[0] == "" {
		return fmt.Errorf("saved backfill is missing its config-file")
	}
	cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), configPaths[0])
	if err != nil {
		return err
	}
	environment := m.Plan.Environment
	if !continuing {
		environment = c.String("environment")
		if environment == "" {
			environment = cm.DefaultEnvironmentName
		}
		m.Plan.Environment = environment
		m.Plan.RunFlags["environment"] = []string{environment}
	}
	if err = cm.SelectEnvironment(environment); err != nil {
		return err
	}
	effective, err := backfillParallelism(options.MaxParallel, options.Workers, cm.SelectedEnvironment.Connections)
	if err != nil {
		return err
	}
	if effective < options.MaxParallel {
		fmt.Fprintf(c.ErrWriter, "Limiting active partitions to %d for the environment's connection limits / local DuckDB files.\n", effective)
	}
	options.MaxParallel = effective
	if c.Bool("dry-run") {
		var state *backfill.Store
		if continuing {
			state = store
		}
		summary, err := backfill.Summarize(ctx, m.Plan, state, options.Rerun)
		if err != nil {
			return err
		}
		return writeBackfillOutput(c.Writer, c.String("output"), m, state, options, summary, page)
	}
	if strings.Contains(strings.ToLower(environment), "prod") && !slices.Equal(m.Plan.RunFlags["force"], []string{"true"}) && !slices.Equal(m.Plan.RunFlags["only"], []string{"checks"}) {
		return fmt.Errorf("backfill in a production environment requires --force on the initial invocation")
	}
	lock, err := store.Lock()
	if err != nil {
		return err
	}
	defer lock.Close()
	if !continuing {
		if err = store.Create(m); err != nil {
			return err
		}
	}
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	output := &backfill.LockedWriter{WriteFunc: c.ErrWriter.Write}
	fmt.Fprintf(output, "Backfill %s; resume with: bruin backfill --state-dir %q --continue %s\n", id, root, id)
	total, err := m.Plan.Count(ctx)
	if err != nil {
		return err
	}
	summary, runErr := backfill.Execute(ctx, m, store, options, func(ctx context.Context, i backfill.Interval, runID string) error {
		return runBackfillChild(ctx, binary, m, store, i, runID, options.Workers, total, debug, output)
	})
	outputErr := writeBackfillOutput(c.Writer, c.String("output"), m, store, options, summary, page)
	return errors.Join(runErr, outputErr)
}

// Reserve each child's worst-case use of each configured connection. This is
// deliberately conservative: even dynamically selected assets cannot exceed an
// environment's limit across partitions. File-backed DuckDB has one writer process.
func backfillParallelism(requested, workers int, connections *config.Connections) (int, error) {
	limits, err := connections.ConnectionConcurrencyLimits()
	if err != nil {
		return 0, err
	}
	for _, limit := range limits {
		requested = min(requested, max(1, limit/min(workers, limit)))
	}
	if connections != nil {
		for _, conn := range connections.DuckDB {
			if !conn.ReadOnly {
				requested = 1
				break
			}
		}
	}
	return requested, nil
}

func backfillChildArgs(m backfill.Manifest, i backfill.Interval, workers, total int, debug bool) []string {
	args := []string{}
	if debug {
		args = append(args, "--debug")
	}
	if backend := m.Plan.RunFlags[secretsBackendFlagName]; len(backend) > 0 {
		args = append(args, "--"+secretsBackendFlagName, backend[0])
	}
	args = append(args, "run", "--workers", strconv.Itoa(workers), "--start-date", i.Start.Format("2006-01-02T15:04:05.000000Z07:00"), "--end-date", i.End.Add(-time.Microsecond).Format("2006-01-02T15:04:05.000000Z07:00"), "--backfill-id", m.ID, "--backfill-total", strconv.Itoa(total))
	names := make([]string, 0, len(m.Plan.RunFlags))
	for name := range m.Plan.RunFlags {
		if name != secretsBackendFlagName {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		for _, value := range m.Plan.RunFlags[name] {
			if value == "" {
				continue
			}
			args = append(args, "--"+name+"="+value)
		}
	}
	return append(args, m.Plan.Target)
}

func runBackfillChild(ctx context.Context, binary string, m backfill.Manifest, s *backfill.Store, i backfill.Interval, runID string, workers, total int, debug bool, output io.Writer) error {
	logDir := filepath.Join(s.Dir, "children")
	if err := os.MkdirAll(logDir, 0o700); err != nil {
		return err
	}
	logPath := filepath.Join(logDir, runID+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	child := exec.CommandContext(ctx, binary, backfillChildArgs(m, i, workers, total, debug)...)
	child.Dir = m.Plan.WorkingDirectory
	// Snapshot variable overrides in the plan; do not reapply a changed BRUIN_VARS
	// or an inherited full-refresh/run ID to children on resume.
	for _, env := range os.Environ() {
		name, _, _ := strings.Cut(env, "=")
		if name != "BRUIN_RUN_ID" && name != "BRUIN_VARS" && name != "BRUIN_FULL_REFRESH" && name != "BRUIN_SECRETS_BACKEND" && name != "BRUIN_QUERY_ANNOTATIONS" {
			child.Env = append(child.Env, env)
		}
	}
	child.Env = append(child.Env, "BRUIN_RUN_ID="+runID, "BRUIN_FULL_REFRESH=0")
	writer := &backfill.LockedWriter{WriteFunc: io.MultiWriter(logFile, output).Write}
	child.Stdout, child.Stderr = writer, writer
	configureBackfillChild(child)
	if err = child.Run(); err != nil {
		return fmt.Errorf("child run %s failed (%s): %w", runID, logPath, err)
	}
	return nil
}

func writeBackfillOutput(w io.Writer, format string, m backfill.Manifest, s *backfill.Store, o backfill.Options, summary backfill.Summary, page backfillPage) error {
	if format == "text" {
		if _, err := fmt.Fprintf(w, "Backfill %s: %d partitions, timezone %s, %d active partitions × %d workers\n", m.ID, summary.Total, m.Plan.Timezone, o.MaxParallel, o.Workers); err != nil {
			return err
		}
		for i := range page.intervals(m.Plan, o.Reverse, summary.Total) {
			r, err := s.Read(i)
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(w, "%s  [%s, %s)  %s  attempts=%d\n", i.ID[:12], i.Start.Format(time.RFC3339Nano), i.End.Format(time.RFC3339Nano), r.Status, len(r.Attempts)); err != nil {
				return err
			}
		}
		if page.Offset > 0 || (page.Limit > 0 && page.Limit < summary.Total-page.Offset) {
			if _, err := fmt.Fprintf(w, "Output page: offset %d, limit %d (use --offset / --limit to inspect other partitions).\n", page.Offset, page.Limit); err != nil {
				return err
			}
		}
		_, err := fmt.Fprintf(w, "Succeeded: %d, failed: %d, skipped: %d, queued: %d, running: %d, cancelled: %d\n", summary.Succeeded, summary.Failed, summary.Skipped, summary.Queued, summary.Running, summary.Cancelled)
		return err
	}
	// Stream the partition array instead of building a potentially enormous slice.
	header := struct {
		Manifest backfill.Manifest `json:"backfill"`
		Options  backfill.Options  `json:"options"`
		Summary  backfill.Summary  `json:"summary"`
		Page     backfillPage      `json:"page"`
		HasMore  bool              `json:"has_more"`
	}{m, o, summary, page, page.Limit > 0 && page.Offset < summary.Total && page.Limit < summary.Total-page.Offset}
	data, err := json.Marshal(header)
	if err != nil {
		return err
	}
	if _, err = w.Write(data[:len(data)-1]); err != nil {
		return err
	}
	if _, err = io.WriteString(w, ",\"partitions\":["); err != nil {
		return err
	}
	first := true
	for i := range page.intervals(m.Plan, o.Reverse, summary.Total) {
		r, err := s.Read(i)
		if err != nil {
			return err
		}
		if !first {
			if _, err = io.WriteString(w, ","); err != nil {
				return err
			}
		}
		first = false
		if err = json.NewEncoder(w).Encode(r); err != nil {
			return err
		}
	}
	_, err = io.WriteString(w, "]}\n")
	return err
}

type backfillPage struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
}

func (page backfillPage) intervals(p backfill.Plan, reverse bool, total int) iter.Seq[backfill.Interval] {
	return func(yield func(backfill.Interval) bool) {
		if page.Offset >= total {
			return
		}
		n, shown := 0, 0
		for i := range p.Intervals(reverse) {
			if n < page.Offset {
				n++
				continue
			}
			if page.Limit > 0 && shown >= page.Limit {
				return
			}
			if !yield(i) {
				return
			}
			shown++
		}
	}
}
