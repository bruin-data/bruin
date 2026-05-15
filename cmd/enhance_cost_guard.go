package cmd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// defaultBigQueryCostLimit is the per-query USD cap applied when the user skips
// the interactive prompt or runs ai-enhance in a non-interactive context.
const defaultBigQueryCostLimit = 5.0

// bqAssetTypes are the asset types that resolve to a google_cloud_platform connection
// and therefore participate in the BigQuery cost guard preflight.
var bqAssetTypes = map[pipeline.AssetType]bool{
	pipeline.AssetTypeBigqueryQuery:       true,
	pipeline.AssetTypeBigqueryQuerySensor: true,
	pipeline.AssetTypeBigquerySeed:        true,
	pipeline.AssetTypeBigquerySource:      true,
	pipeline.AssetTypeBigqueryTableSensor: true,
}

// costGuardPrompter abstracts the interactive prompt so tests can inject behavior.
type costGuardPrompter interface {
	Prompt(connName string) (limit float64, skipped bool, err error)
}

// stdinCostGuardPrompter reads a USD value (or blank for default) from a reader.
type stdinCostGuardPrompter struct {
	in  io.Reader
	out io.Writer
}

func (p *stdinCostGuardPrompter) Prompt(connName string) (float64, bool, error) {
	fmt.Fprintf(p.out, "\nNo BigQuery cost guard is configured on connection '%s'.\n", connName)
	fmt.Fprintf(p.out, "Running queries during AI enhancement could incur uncapped cost.\n")
	fmt.Fprintf(p.out, "Enter a per-query limit in USD, or press Enter to use the default ($%.2f): ", defaultBigQueryCostLimit)

	reader := bufio.NewReader(p.in)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, false, errors.Wrap(err, "failed to read cost limit input")
	}

	answer := strings.TrimSpace(line)
	if answer == "" || strings.EqualFold(answer, "skip") {
		return defaultBigQueryCostLimit, true, nil
	}

	answer = strings.TrimPrefix(answer, "$")
	val, parseErr := strconv.ParseFloat(answer, 64)
	if parseErr != nil {
		return 0, false, errors.Errorf("invalid USD amount %q: enter a number like 2.50 or press Enter to use the default", answer)
	}
	if val <= 0 {
		return 0, false, errors.Errorf("cost limit must be positive, got %g", val)
	}
	return val, false, nil
}

// runBigQueryCostGuard scans the assets implied by inputPath, finds the BigQuery
// connections they would touch, and ensures each connection has at least one
// cost-guard key set on it in .bruin.yml. If a connection has no limit, the user
// is prompted to choose a USD per-query cap (default $5).
//
// The function is a no-op when no BigQuery assets are in scope.
func runBigQueryCostGuard(ctx context.Context, inputPath, environment, output string, fs afero.Fs, prompter costGuardPrompter, isInteractive bool) error {
	connections, err := collectBigQueryConnections(ctx, inputPath)
	if err != nil {
		return err
	}
	if len(connections) == 0 {
		return nil
	}

	repoRoot, err := git.FindRepoFromPath(inputPath)
	if err != nil {
		return errors.Wrap(err, "failed to find repo root for cost guard check")
	}
	configPath := filepath.Join(repoRoot.Path, ".bruin.yml")

	cm, err := config.LoadOrCreate(fs, configPath)
	if err != nil {
		return errors.Wrapf(err, "failed to load %s", configPath)
	}
	if environment != "" {
		if err := cm.SelectEnvironment(environment); err != nil {
			return err
		}
	}

	env := cm.SelectedEnvironment
	if env == nil {
		return nil
	}

	missing := connectionsMissingCostGuards(env, connections)
	if len(missing) == 0 {
		return nil
	}

	for _, connName := range missing {
		var limit float64
		var skipped bool

		if isInteractive && prompter != nil {
			limit, skipped, err = prompter.Prompt(connName)
			if err != nil {
				return err
			}
		} else {
			limit = defaultBigQueryCostLimit
			skipped = true
		}

		if err := applyCostLimit(env, connName, limit); err != nil {
			return err
		}

		if output != "json" {
			if skipped {
				warningPrinter.Printf("  Applied default cost guard $%.2f/query to '%s'\n", limit, connName)
			} else {
				successPrinter.Printf("  Set max_query_cost=$%.2f on '%s'\n", limit, connName)
			}
		}
	}

	if err := cm.PersistToFs(fs); err != nil {
		return errors.Wrap(err, "failed to persist cost guard changes to .bruin.yml")
	}
	return nil
}

// collectBigQueryConnections returns the unique connection names used by any
// BigQuery asset in scope of inputPath (which can be either an asset file or a
// folder containing assets across one or more pipelines).
func collectBigQueryConnections(ctx context.Context, inputPath string) ([]string, error) {
	var assetPaths []string
	switch {
	case isPathReferencingAsset(inputPath):
		assetPaths = []string{inputPath}
	case isDir(inputPath):
		assetPaths = path.GetAllPossibleAssetPaths(inputPath, assetsDirectoryNames, pipeline.SupportedFileSuffixes)
	default:
		return nil, nil
	}
	if len(assetPaths) == 0 {
		return nil, nil
	}

	pipelineCache := map[string]*pipeline.Pipeline{}
	seen := map[string]struct{}{}
	var connections []string

	for _, ap := range assetPaths {
		pipelinePath, err := path.GetPipelineRootFromTask(ap, PipelineDefinitionFiles)
		if err != nil {
			continue
		}
		pl, ok := pipelineCache[pipelinePath]
		if !ok {
			pl, err = DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithOnlyPipeline())
			if err != nil {
				continue
			}
			pipelineCache[pipelinePath] = pl
		}
		asset, err := DefaultPipelineBuilder.CreateAssetFromFile(ap, pl)
		if err != nil || asset == nil {
			continue
		}
		if !bqAssetTypes[asset.Type] {
			continue
		}
		connName, err := pl.GetConnectionNameForAsset(asset)
		if err != nil || connName == "" {
			continue
		}
		if _, dup := seen[connName]; dup {
			continue
		}
		seen[connName] = struct{}{}
		connections = append(connections, connName)
	}

	sort.Strings(connections)
	return connections, nil
}

// connectionsMissingCostGuards returns the subset of names that exist on the
// google_cloud_platform list but lack any cost-guard key.
func connectionsMissingCostGuards(env *config.Environment, names []string) []string {
	var missing []string
	for _, name := range names {
		conn, found := findGCPConnection(env, name)
		if !found {
			// Connection isn't declared at all — skip; the existing flow will
			// surface a clearer "connection not found" error later.
			continue
		}
		if hasCostGuard(conn) {
			continue
		}
		missing = append(missing, name)
	}
	return missing
}

func findGCPConnection(env *config.Environment, name string) (*config.GoogleCloudPlatformConnection, bool) {
	if env == nil {
		return nil, false
	}
	for i := range env.Connections.GoogleCloudPlatform {
		if env.Connections.GoogleCloudPlatform[i].Name == name {
			return &env.Connections.GoogleCloudPlatform[i], true
		}
	}
	return nil, false
}

func hasCostGuard(conn *config.GoogleCloudPlatformConnection) bool {
	if conn == nil {
		return false
	}
	return conn.MaxQueryCost != nil ||
		conn.MaxQueryCostSoft != nil ||
		conn.MaxBillableBytes != nil ||
		conn.MaxBillableBytesSoft != nil
}

func applyCostLimit(env *config.Environment, connName string, usd float64) error {
	conn, found := findGCPConnection(env, connName)
	if !found {
		return errors.Errorf("google_cloud_platform connection %q not found while applying cost guard", connName)
	}
	v := usd
	conn.MaxQueryCost = &v
	return nil
}
