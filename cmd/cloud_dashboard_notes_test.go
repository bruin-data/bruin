package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

const dashboardNotesContext = `{"dashboard_model":"orders","model_contexts":{"payments":{"name":"payments","dimensions":[{"name":"method","type":"string"}]},"orders":{"name":"orders","dimensions":[{"name":"country","type":"string"},{"name":"created_at","type":"time"},{"name":"count","type":"number"},{"name":"active","type":"boolean"}]}},"filters":[{"name":"region","type":"select"}]}`

func runDashboardNotesCLI(t *testing.T, args ...string) (string, error) {
	t.Helper()
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	originalColorOutput := color.Output
	os.Stdout = writer
	color.Output = writer
	t.Cleanup(func() {
		os.Stdout = original
		color.Output = originalColorOutput
	})
	debug := false
	cmd := Cloud(&debug)
	cmd.ExitErrHandler = func(context.Context, *cli.Command, error) {}
	runErr := runCLI(t.Context(), cmd, append([]string{"cloud", "dashboards", "notes"}, args...))
	require.NoError(t, writer.Close())
	os.Stdout = original
	color.Output = originalColorOutput
	data, err := io.ReadAll(reader)
	require.NoError(t, reader.Close())
	require.NoError(t, err)
	return string(data), runErr
}

func TestCloudDashboardNotes(t *testing.T) {
	const note = `{"id":42,"dashboard_id":null,"semantic_model":"orders","dimensions":{"country":["US","CA"]},"content":"US campaign launched.","created_by":7,"created_at":"2026-10-20T12:00:00Z","updated_at":"2026-10-20T12:00:00Z","can_delete":true}`
	const paymentNote = `{"id":43,"dashboard_id":null,"semantic_model":"payments","dimensions":{"method":"card"},"content":"Card payment context.","created_by":7,"created_at":"2026-10-20T12:00:00Z"}`
	const plainNote = `{"id":44,"dashboard_id":123,"semantic_model":null,"dimensions":[],"content":"Revenue excludes refunds.","author":"Ada","created_by":null,"created_at":"2026-10-20T12:00:00Z"}`
	const list = `{"notes":[` + note + `,` + plainNote + `],"definitions":[{"name":"country"}],"dashboard_model":"orders","model_contexts":{"orders":{"dimensions":[{"name":"country","type":"string"},{"name":"created_at","type":"time"}]}},"filters":[{"name":"region"}]}`

	for _, tc := range []struct {
		name    string
		args    []string
		body    string
		note    string
		context string
	}{
		{name: "list"},
		{name: "content only", args: []string{"--content", "US campaign launched."}, body: `{"content":"US campaign launched."}`},
		{name: "explicit model", args: []string{"--content", "US campaign launched.", "--model", "orders"}, body: `{"content":"US campaign launched.","semantic_model":"orders"}`, context: dashboardNotesContext},
		{name: "explicit model and scalar dimension", args: []string{"--content", "Card payment context.", "--model", "payments", "--dimensions", `{"method":"card"}`}, body: `{"content":"Card payment context.","semantic_model":"payments","dimensions":{"method":"card"}}`, note: paymentNote, context: dashboardNotesContext},
		{name: "explicit model and multiple values", args: []string{"--content", "US campaign launched.", "--model", "orders", "--dimensions", `{"country":["US","CA"],"created_at":["2026-10-20"]}`}, body: `{"content":"US campaign launched.","semantic_model":"orders","dimensions":{"country":["US","CA"],"created_at":["2026-10-20"]}}`, context: dashboardNotesContext},
		{name: "explicit model and unvalidated dimension", args: []string{"--content", "Card payment context.", "--model", "payments", "--dimensions", `{"customers.country":"US"}`}, body: `{"content":"Card payment context.","semantic_model":"payments","dimensions":{"customers.country":"US"}}`, note: paymentNote, context: dashboardNotesContext},
		{name: "multiple dimension values", args: []string{"--content", "US campaign launched.", "--dimensions", `{"country":["US","CA"],"created_at":["2026-10-20"],"count":2}`}, body: `{"content":"US campaign launched.","dimensions":{"country":["US","CA"],"created_at":["2026-10-20"],"count":2}}`},
		{name: "dimensions without model", args: []string{"--content", "US campaign launched.", "--dimensions", `{"country":"US"}`}, body: `{"content":"US campaign launched.","dimensions":{"country":"US"}}`},
		{name: "empty dimensions", args: []string{"--content", "US campaign launched.", "--dimensions", `{}`}, body: `{"content":"US campaign launched.","dimensions":{}}`},
	} {
		for _, output := range []string{"json", "plain"} {
			t.Run(tc.name+"/"+output, func(t *testing.T) {
				expectedNote := note
				if tc.note != "" {
					expectedNote = tc.note
				}
				command, method, response := "list", http.MethodGet, list
				if tc.body != "" {
					command, method, response = "create", http.MethodPost, `{"note":`+expectedNote+`}`
				}
				var requests []string
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests = append(requests, r.Method)
					assert.Equal(t, "/dashboards/123/notes", r.URL.Path)
					assert.Equal(t, "Bearer user-key", r.Header.Get("Authorization"))
					assert.Equal(t, "analytics", r.Header.Get("X-Bruin-Team"))
					if command == "create" && r.Method == http.MethodGet {
						assert.NotEmpty(t, tc.context)
						_, _ = io.WriteString(w, tc.context)
						return
					}
					assert.Equal(t, method, r.Method)
					if tc.body != "" {
						assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
						body, err := io.ReadAll(r.Body)
						assert.NoError(t, err)
						assert.JSONEq(t, tc.body, string(body))
						w.WriteHeader(http.StatusCreated)
					}
					_, _ = io.WriteString(w, response)
				}))
				defer server.Close()
				t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
				args := append([]string{command, "--dashboard-id", "123", "--api-key", "user-key", "--team", "analytics", "--output", output}, tc.args...)
				data, err := runDashboardNotesCLI(t, args...)
				require.NoError(t, err)
				if tc.context != "" {
					assert.Equal(t, []string{http.MethodGet, http.MethodPost}, requests)
				} else {
					assert.Equal(t, []string{method}, requests)
				}
				switch {
				case output == "json":
					if command == "list" {
						assert.JSONEq(t, list, data)
					} else {
						assert.JSONEq(t, expectedNote, data)
					}
				case command == "list":
					assert.Contains(t, data, "US campaign launched.")
					assert.Contains(t, data, "Revenue excludes refunds.")
					assert.Contains(t, data, "Ada")
					assert.Contains(t, data, `{"country":["US","CA"]}`)
					assert.Contains(t, data, "Available dimensions for --model and --dimensions:")
					assert.Contains(t, data, "orders (default)")
					assert.Contains(t, data, "created_at")
				default:
					assert.Contains(t, data, "Created note")
				}
			})
		}
	}
}

func TestCloudDashboardNotesListEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"notes":[],"definitions":[],"dashboard_model":null,"model_contexts":[],"filters":[]}`)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	data, err := runDashboardNotesCLI(t, "list", "--dashboard-id", "123", "--api-key", "user-key", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, data, "No notes found for this dashboard.")
	assert.NotContains(t, data, "Created At")
}

func TestCloudDashboardNotesListFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"notes":[],"definitions":[],"dashboard_model":null,"model_contexts":[],"filters":[{"name":"region","type":"select","multiple":true},{"name":"active","type":"boolean"},{"name":"period","type":"date-range"},"legacy",{"name":7,"type":"text"},{"name":"segment","type":"text","multiple":"true"}]}`)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	data, err := runDashboardNotesCLI(t, "list", "--dashboard-id", "123", "--api-key", "user-key", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, data, "Available filters for --dimensions without --model:")
	assert.Contains(t, data, "segment")
	assert.NotContains(t, data, "text (multiple)")
	assert.Contains(t, data, "region")
	assert.Contains(t, data, "select (multiple)")
	assert.Contains(t, data, "boolean")
	assert.NotContains(t, data, "period")
	assert.NotContains(t, data, "Available dimensions")
}

func TestCloudDashboardNotesListWidgetModelsAndFilters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"notes":[],"definitions":[],"dashboard_model":null,"model_contexts":{"orders":{"name":"orders","dimensions":[{"name":"country","type":"string"}]}},"filters":[{"name":"region","type":"select"}]}`)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	data, err := runDashboardNotesCLI(t, "list", "--dashboard-id", "123", "--api-key", "user-key", "--output", "plain")
	require.NoError(t, err)
	assert.Contains(t, data, "Available dimensions for --model and --dimensions:")
	assert.Contains(t, data, "country")
	assert.NotContains(t, data, "(default)")
	assert.Contains(t, data, "Available filters for --dimensions without --model:")
	assert.Contains(t, data, "region")
}

func TestCloudDashboardNotesInvalidContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		context string
		message string
	}{
		{name: "unknown model", args: []string{"--model", "customers"}, context: dashboardNotesContext, message: `model "customers" is not used by this dashboard; available models: orders, payments`},
		{name: "no models", args: []string{"--model", "orders"}, context: `{"dashboard_model":null,"model_contexts":[],"filters":[]}`, message: `model "orders" is not used by this dashboard; available models: (none)`},
	} {
		for _, output := range []string{"plain", "json"} {
			t.Run(tc.name+"/"+output, func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					assert.Equal(t, http.MethodGet, r.Method, "invalid context must not be posted")
					assert.Equal(t, "/dashboards/123/notes", r.URL.Path)
					_, _ = io.WriteString(w, tc.context)
				}))
				defer server.Close()
				t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
				args := append([]string{"create", "--dashboard-id", "123", "--content", "Note", "--api-key", "key", "--output", output}, tc.args...)
				data, err := runDashboardNotesCLI(t, args...)
				require.Error(t, err)
				var exit cli.ExitCoder
				require.ErrorAs(t, err, &exit)
				assert.Equal(t, 1, exit.ExitCode())
				assert.Equal(t, 1, requests)
				if output == "json" {
					var result map[string]string
					require.NoError(t, json.Unmarshal([]byte(data), &result))
					assert.Equal(t, tc.message, result["error"])
				} else {
					assert.Contains(t, data, tc.message)
				}
			})
		}
	}
}

//nolint:paralleltest // CLI runs share process-wide stdout and environment.
func TestCloudDashboardNotesInvalidFlags(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	for _, dimensions := range []string{`{`, `[]`, `"US"`, `null`, `1`, `true`, `{"country":"US"} trailing`, ""} {
		t.Run("dimensions/"+dimensions, func(t *testing.T) {
			data, err := runDashboardNotesCLI(t, "create", "--dashboard-id", "123", "--content", "Note", "--dimensions", dimensions, "--api-key", "key", "--output", "json")
			require.Error(t, err)
			assert.JSONEq(t, `{"error":"dimensions must be a JSON object"}`, data)
		})
	}
	for _, args := range [][]string{
		{"list"},
		{"create", "--content", "Note"},
		{"create", "--dashboard-id", "123"},
		{"list", "--dashboard-id", "0"},
		{"create", "--dashboard-id", "-1", "--content", "Note"},
		{"create", "--dashboard-id", "123", "--content", " "},
		{"create", "--dashboard-id", "123", "--content", "Note", "--model", " "},
		{"create", "--dashboard-id", "123", "--content", "Note", "--widget-id", "payments-table"},
		{"create", "--dashboard-id", "123", "--content", "Note", "--note-type", "campaign"},
	} {
		t.Run(args[0]+"/"+strings.Join(args[1:], " "), func(t *testing.T) {
			_, err := runDashboardNotesCLI(t, args...)
			require.Error(t, err)
		})
	}
	assert.Zero(t, requests)
}

func TestCloudDashboardNotesBackendErrors(t *testing.T) {
	for _, tc := range []struct {
		status   int
		response string
		message  string
	}{
		{403, `{"message":"Creating notes requires a user-scoped token."}`, "Creating notes requires a user-scoped token."},
		{404, `{"message":"Dashboard not found."}`, "Dashboard not found."},
		{422, `{"message":"The given data was invalid.","errors":{"dimensions.country":["is not a valid dimension."]}}`, "The given data was invalid.: dimensions.country is not a valid dimension.;"},
	} {
		for _, command := range []struct {
			name string
			args []string
		}{
			{name: "list", args: []string{"list"}},
			{name: "create", args: []string{"create", "--content", "Note"}},
			{name: "create context", args: []string{"create", "--content", "Note", "--model", "orders"}},
		} {
			t.Run(http.StatusText(tc.status)+"/"+command.name, func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.response)
				}))
				defer server.Close()
				t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
				args := append([]string(nil), command.args...)
				args = append(args, "--dashboard-id", "123", "--api-key", "team-key", "--output", "json")
				data, err := runDashboardNotesCLI(t, args...)
				require.Error(t, err)
				var exit cli.ExitCoder
				require.ErrorAs(t, err, &exit)
				assert.Equal(t, 1, exit.ExitCode())
				expectedRequests := 1
				if command.name == "create context" && tc.status == http.StatusForbidden {
					// A 403 on the context pre-fetch falls through to the create request.
					expectedRequests = 2
				}
				assert.Equal(t, expectedRequests, requests)
				var result map[string]string
				require.NoError(t, json.Unmarshal([]byte(data), &result))
				assert.Equal(t, tc.message, result["error"])
			})
		}
	}
}

func TestCloudDashboardNotesBackendValidationAfterContext(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method)
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, dashboardNotesContext)
			return
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, `{"message":"The given data was invalid.","errors":{"dimensions.country":["Each selected value must be non-empty."]}}`)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	data, err := runDashboardNotesCLI(t, "create", "--dashboard-id", "123", "--api-key", "key", "--model", "orders", "--dimensions", `{"country":""}`, "--content", "Note", "--output", "json")
	require.Error(t, err)
	assert.Equal(t, []string{http.MethodGet, http.MethodPost}, requests)
	assert.JSONEq(t, `{"error":"The given data was invalid.: dimensions.country Each selected value must be non-empty.;"}`, data)
}

func TestCloudDashboardNotesCreateWithoutListAbility(t *testing.T) {
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method)
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"message":"Invalid ability provided."}`)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"note":{"id":7,"semantic_model":"orders","dimensions":{},"content":"Note"}}`)
	}))
	defer server.Close()
	t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
	data, err := runDashboardNotesCLI(t, "create", "--dashboard-id", "123", "--api-key", "key", "--model", "orders", "--content", "Note")
	require.NoError(t, err)
	assert.Equal(t, []string{http.MethodGet, http.MethodPost}, requests)
	assert.Contains(t, data, "Created note 7.")
}
