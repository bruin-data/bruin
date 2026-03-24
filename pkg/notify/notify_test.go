package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatSuccessMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload Payload
		want    string
	}{
		{
			name:    "custom message",
			payload: Payload{Message: "custom msg", Pipeline: "test"},
			want:    "custom msg",
		},
		{
			name:    "with pipeline",
			payload: Payload{Pipeline: "my-pipeline"},
			want:    "Pipeline `my-pipeline` has finished successfully.",
		},
		{
			name:    "no pipeline",
			payload: Payload{},
			want:    "Pipeline has finished successfully.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, FormatSuccessMessage(tt.payload))
		})
	}
}

func TestSlackSender_Send(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json; charset=utf-8", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))
		assert.Equal(t, "#test-channel", payload["channel"])
		assert.Equal(t, "Bruin", payload["username"])

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	defer server.Close()

	sender := &SlackSender{
		APIKey:  "test-api-key",
		Channel: "#test-channel",
		client:  server.Client(),
	}
	// Override the URL by using a custom transport
	sender.client = &http.Client{
		Transport: &rewriteTransport{
			base:   http.DefaultTransport,
			target: server.URL,
		},
	}

	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Status:   "success",
	})
	require.NoError(t, err)
}

func TestDiscordSender_Send(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))
		assert.Equal(t, "Bruin", payload["username"])
		assert.Contains(t, payload["content"], "test-pipeline")

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender := NewDiscordSender(server.URL)
	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Status:   "success",
	})
	require.NoError(t, err)
}

func TestMSTeamsSender_Send(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))
		assert.Equal(t, "MessageCard", payload["@type"])

		sections := payload["sections"].([]any)
		section := sections[0].(map[string]any)
		assert.Contains(t, section["activityTitle"], "successfully")

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewMSTeamsSender(server.URL)
	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Status:   "success",
	})
	require.NoError(t, err)
}

func TestWebhookSender_Send(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))
		assert.Equal(t, "test-pipeline", payload["pipeline"])
		assert.Equal(t, "success", payload["status"])
		assert.Nil(t, payload["asset"])

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewWebhookSender(server.URL, "", "")
	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Status:   "success",
	})
	require.NoError(t, err)
}

func TestWebhookSender_SendWithBasicAuth(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "myuser", user)
		assert.Equal(t, "mypass", pass)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := NewWebhookSender(server.URL, "myuser", "mypass")
	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Status:   "failure",
	})
	require.NoError(t, err)
}

func TestDiscordSender_SendFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var payload map[string]any
		require.NoError(t, json.Unmarshal(body, &payload))
		assert.Contains(t, payload["content"], "failed")

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender := NewDiscordSender(server.URL)
	err := sender.Send(context.Background(), Payload{
		Pipeline: "test-pipeline",
		Asset:    "my-asset",
		Status:   "failure",
	})
	require.NoError(t, err)
}

func TestSenderErrorHandling(t *testing.T) {
	t.Parallel()

	newErrorServer := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("server error"))
		}))
	}

	t.Run("discord returns error on non-2xx", func(t *testing.T) {
		t.Parallel()
		server := newErrorServer()
		defer server.Close()
		sender := NewDiscordSender(server.URL)
		err := sender.Send(context.Background(), Payload{Status: "success"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("ms_teams returns error on non-2xx", func(t *testing.T) {
		t.Parallel()
		server := newErrorServer()
		defer server.Close()
		sender := NewMSTeamsSender(server.URL)
		err := sender.Send(context.Background(), Payload{Status: "success"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("webhook returns error on non-2xx", func(t *testing.T) {
		t.Parallel()
		server := newErrorServer()
		defer server.Close()
		sender := NewWebhookSender(server.URL, "", "")
		err := sender.Send(context.Background(), Payload{Status: "success"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "500")
	})
}

// rewriteTransport rewrites the request URL to point to a test server.
type rewriteTransport struct {
	base   http.RoundTripper
	target string
}

func (t *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = t.target[len("http://"):]
	return t.base.RoundTrip(req)
}
