package config

import (
	"encoding/json"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
)

// InjectConnectionEnv auto-injects the asset's default connection into envVariables and connectionTypes
// if it is not already listed in the asset's secrets. This avoids requiring users to duplicate the
// connection name in both `connection:` and `secrets:`.
func InjectConnectionEnv(
	cfg ConnectionDetailsGetter,
	asset *pipeline.Asset,
	envVariables map[string]string,
	connectionTypes map[string]string,
) error {
	if asset.Connection == "" {
		return nil
	}

	for _, mapping := range asset.Secrets {
		if mapping.SecretKey == asset.Connection {
			return nil
		}
	}

	conn := cfg.GetConnectionDetails(asset.Connection)
	if conn == nil {
		return nil
	}

	connType := cfg.GetConnectionType(asset.Connection)
	if connType != "" {
		connectionTypes[asset.Connection] = connType
	}

	if val, ok := conn.(*GenericConnection); ok {
		envVariables[asset.Connection] = val.Value
		return nil
	}

	res, err := json.Marshal(conn)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal connection '%s'", asset.Connection)
	}
	envVariables[asset.Connection] = string(res)
	return nil
}
