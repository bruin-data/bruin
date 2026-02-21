package config

import (
	"encoding/json"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// InjectConnectionEnv auto-injects the asset's default connection into envVariables and connectionTypes
// if it is not already listed in the asset's secrets. This avoids requiring users to duplicate the
// connection name in both `connection:` and `secrets:`.
func InjectConnectionEnv(
	cfg ConnectionDetailsGetter,
	asset *pipeline.Asset,
	envVariables map[string]string,
	connectionTypes map[string]string,
) {
	if asset.Connection == "" {
		return
	}

	for _, mapping := range asset.Secrets {
		if mapping.SecretKey == asset.Connection {
			return
		}
	}

	conn := cfg.GetConnectionDetails(asset.Connection)
	if conn == nil {
		return
	}

	connType := cfg.GetConnectionType(asset.Connection)
	if connType != "" {
		connectionTypes[asset.Connection] = connType
	}

	if val, ok := conn.(*GenericConnection); ok {
		envVariables[asset.Connection] = val.Value
		return
	}

	res, err := json.Marshal(conn)
	if err != nil {
		return
	}
	envVariables[asset.Connection] = string(res)
}
