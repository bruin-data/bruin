package cmd

import (
	"io"
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestPrintErrorsMinimal_AddsAFailureReportLine(t *testing.T) { //nolint:paralleltest // captures os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = original })

	bigQueryErr := &googleapi.Error{
		Code:    400,
		Message: "The job encountered an error during execution. Retrying the job may solve the problem.",
		Errors:  []googleapi.ErrorItem{{Reason: "jobBackendError"}},
	}
	printErrorsMinimal([]*scheduler.TaskExecutionResult{
		{Instance: &scheduler.AssetInstance{Asset: &pipeline.Asset{Name: "analytics.daily"}}, Error: bigQueryErr},
		{
			Instance: &scheduler.AssetInstance{Asset: &pipeline.Asset{Name: "raw.orders"}},
			Error:    &config.MissingConnectionError{Name: "pg-prod", SecretsBackend: "vault"},
		},
	})
	require.NoError(t, writer.Close())
	os.Stdout = original
	out, err := io.ReadAll(reader)
	require.NoError(t, err)

	assert.Equal(t, "\n  analytics.daily: "+bigQueryErr.Error()+"\n"+
		`bruin-failure: {"asset":"analytics.daily","class":"transient","code":"bq_retryable","source":"google_api","reason":"jobBackendError","http_status":400}`+"\n"+
		"\n  raw.orders: connection 'pg-prod' not found in secrets backend 'vault'\n"+
		`bruin-failure: {"asset":"raw.orders","class":"missing_connection","code":"connection_missing","source":"bruin","connection":"pg-prod"}`+"\n",
		string(out))
}
