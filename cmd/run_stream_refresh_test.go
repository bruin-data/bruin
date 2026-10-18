package cmd

import (
	"context"
	"io"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestValidateStreamingRunRefresh(t *testing.T) { //nolint:paralleltest // Run uses shared CLI date flags.
	t.Setenv("BRUIN_FULL_REFRESH", "false")

	tests := []struct {
		name       string
		args       []string
		parameters pipeline.ParameterMap
		restricted bool
		wantErr    string
	}{
		{name: "full refresh", args: []string{"--full-refresh"}},
		{name: "CLI request ID", args: []string{"--full-refresh", "--refresh-id", "request / 1"}},
		{name: "asset refresh with CLI ID", args: []string{"--refresh-id", "request-1"}, parameters: pipeline.ParameterMap{"full_refresh": true}},
		{name: "asset refresh and ID", parameters: pipeline.ParameterMap{"full_refresh": true, "refresh_id": "request-1"}},
		{name: "CLI ID without refresh", args: []string{"--refresh-id", "request-1"}, wantErr: "requires full refresh"},
		{name: "asset ID without refresh", parameters: pipeline.ParameterMap{"refresh_id": "request-1"}, wantErr: "requires full refresh"},
		{name: "restricted refresh", args: []string{"--full-refresh", "--refresh-id", "request-1"}, restricted: true, wantErr: "requires full refresh"},
		{name: "downstream remains invalid", args: []string{"--full-refresh", "--downstream"}, wantErr: "cannot be combined with --downstream"},
		{name: "selector remains invalid", args: []string{"--full-refresh", "--selector", "tag:finance"}, wantErr: "cannot be combined with --selector"},
		{name: "sensor remains invalid", args: []string{"--full-refresh", "--sensor-mode", "wait"}, wantErr: "cannot be combined with --sensor-mode"},
	}
	for _, tt := range tests { //nolint:paralleltest // Run uses shared CLI date flags.
		t.Run(tt.name, func(t *testing.T) {
			parameters := pipeline.ParameterMap{"stream": true, "cdc": true}
			for key, value := range tt.parameters {
				parameters[key] = value
			}
			asset := &pipeline.Asset{
				Name:              "public.orders",
				Type:              pipeline.AssetTypeIngestr,
				Parameters:        parameters,
				RefreshRestricted: &tt.restricted,
			}
			debug := false
			command := Run(&debug)
			command.Before = nil
			command.After = nil
			command.Writer = io.Discard
			command.ErrWriter = io.Discard
			command.Action = func(_ context.Context, c *cli.Command) error {
				return validateStreamingRun(c, asset, true)
			}
			args := append([]string{"run", "--stream"}, tt.args...)
			err := command.Run(t.Context(), append(args, "assets/stream.asset.yml"))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}
