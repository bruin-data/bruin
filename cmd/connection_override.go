package cmd

import (
	"fmt"
	"slices"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// connectionTypeGetter returns the type of a connection by name, or an empty string when it's unknown.
type connectionTypeGetter interface {
	GetConnectionType(name string) string
}

// applyConnectionOverride runs the asset being run, and its checks, on the given connection instead
// of the one it resolves to. The secrets the asset reads from its own connection, like the one
// injected into Python assets, are read from the given connection instead, under the same names,
// so the asset's code doesn't change. A connection of another type is rejected when both types are
// known, since the asset's query or code is written for its own platform.
func applyConnectionOverride(p *pipeline.Pipeline, target *pipeline.Asset, connection string, connections connectionTypeGetter) error {
	if target == nil {
		return errors.New("--connection-override requires running a single asset")
	}
	asset := p.GetAssetByName(target.Name)
	if asset == nil {
		return fmt.Errorf("asset '%s' is not in the pipeline", target.Name)
	}

	current, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		return errors.Wrapf(err, "failed to resolve the connection of asset '%s'", asset.Name)
	}
	if connections != nil {
		overrideType := connections.GetConnectionType(connection)
		if overrideType == "" {
			return fmt.Errorf("connection '%s' does not exist", connection)
		}
		if currentType := connections.GetConnectionType(current); currentType != "" && currentType != overrideType {
			return fmt.Errorf("connection '%s' is a %s connection, but asset '%s' runs on a %s connection", connection, overrideType, asset.Name, currentType)
		}
	}

	for _, a := range []*pipeline.Asset{asset, target} {
		a.Connection = connection
		for i, secret := range a.Secrets {
			if secret.SecretKey == current {
				a.Secrets[i].SecretKey = connection
			}
		}
		// Python and R assets are told their connection's name in BRUIN_CONNECTION, and their
		// helpers read its credentials by that name, so it's injected under its own name too.
		if readsSecret(a, connection) && !injectsAs(a, connection) {
			a.Secrets = append(a.Secrets, pipeline.SecretMapping{SecretKey: connection, InjectedKey: connection})
		}
		// An ingestr asset may name the connection it writes to in its parameters, which some
		// checks read directly, e.g. the BigQuery credentials check before the run.
		if destination, ok := a.Parameters.GetString("destination_connection"); ok && destination == current {
			a.Parameters["destination_connection"] = connection
		}
	}
	return nil
}

// continuedConnectionOverride returns the connection to run on: the given one, or when continuing a
// run, the one it was started on, if any. Continuing on another connection than the run started on
// is rejected, since the assets that already ran wrote to that one.
func continuedConnectionOverride(given string, continuing bool, saved string) (string, error) {
	if !continuing {
		return given, nil
	}
	if given != "" && given != saved {
		if saved == "" {
			return "", fmt.Errorf("the run was started without --connection-override, so it can't continue on '%s'", given)
		}
		return "", fmt.Errorf("the run was started with --connection-override %s, so it can't continue on '%s'", saved, given)
	}
	return saved, nil
}

func readsSecret(a *pipeline.Asset, name string) bool {
	return slices.ContainsFunc(a.Secrets, func(s pipeline.SecretMapping) bool { return s.SecretKey == name })
}

func injectsAs(a *pipeline.Asset, name string) bool {
	return slices.ContainsFunc(a.Secrets, func(s pipeline.SecretMapping) bool { return s.InjectedKey == name })
}
