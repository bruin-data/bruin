package cloudauth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthLogin(t *testing.T) {
	t.Parallel()
	var challenge, redirect string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/cli/oauth/token", r.URL.Path)
		require.NoError(t, r.ParseForm())
		assert.Equal(t, ClientID, r.Form.Get("client_id"))
		assert.Equal(t, redirect, r.Form.Get("redirect_uri"))
		assert.Equal(t, "test-code", r.Form.Get("code"))
		digest := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		assert.Equal(t, challenge, base64.RawURLEncoding.EncodeToString(digest[:]))
		require.NoError(t, json.NewEncoder(w).Encode(Credential{Token: "secret", TokenType: "Bearer", TokenID: "123", Account: "person@example.com", ExpiresAt: time.Now().Add(time.Hour)}))
	}))
	defer server.Close()
	var output bytes.Buffer
	flow := OAuth{APIURL: server.URL + "/api/v1", Writer: &output, OpenBrowser: func(ctx context.Context, link string) error {
		authorize, err := url.Parse(link)
		require.NoError(t, err)
		assert.Equal(t, "/cli/oauth/authorize", authorize.Path)
		query := authorize.Query()
		assert.Equal(t, "S256", query.Get("code_challenge_method"))
		assert.Equal(t, "repo", query.Get("storage"))
		challenge = query.Get("code_challenge")
		redirect = query.Get("redirect_uri")
		response, err := http.Get(redirect + "?state=wrong&code=attacker")
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, response.StatusCode)
		body, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "Authorization could not be verified")
		assert.NotContains(t, string(body), "attacker")
		require.NoError(t, response.Body.Close())
		callback := redirect + "?" + url.Values{"state": {query.Get("state")}, "code": {"test-code"}}.Encode()
		response, err = http.Get(callback)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode)
		assert.Equal(t, "text/html; charset=utf-8", response.Header.Get("Content-Type"))
		assert.Equal(t, "no-store", response.Header.Get("Cache-Control"))
		assert.Equal(t, "no-referrer", response.Header.Get("Referrer-Policy"))
		body, err = io.ReadAll(response.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "<h1>Authorization received</h1>")
		assert.Contains(t, string(body), "Return to your terminal to finish signing in.")
		assert.Contains(t, string(body), "You can close this tab.")
		assert.NotContains(t, string(body), "test-code")
		assert.NotContains(t, string(body), query.Get("state"))
		require.NoError(t, response.Body.Close())
		return nil
	}}
	credential, err := flow.Login(t.Context(), "repo")
	require.NoError(t, err)
	assert.Equal(t, "secret", credential.Token)
	assert.Equal(t, server.URL+"/api/v1", credential.APIURL)
	assert.NotContains(t, output.String(), "secret")
}

func TestOAuthDeniedAndCancelled(t *testing.T) {
	t.Parallel()
	for _, denied := range []bool{true, false} {
		t.Run(map[bool]string{true: "denied", false: "cancelled"}[denied], func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			flow := OAuth{APIURL: DefaultAPIURL, Writer: io.Discard, OpenBrowser: func(_ context.Context, link string) error {
				if !denied {
					cancel()
					return nil
				}
				parsed, err := url.Parse(link)
				require.NoError(t, err)
				query := parsed.Query()
				response, err := http.Get(query.Get("redirect_uri") + "?" + url.Values{"state": {query.Get("state")}, "error": {"access_denied"}, "error_description": {"sensitive-detail"}}.Encode())
				require.NoError(t, err)
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), "<h1>Authorization not completed</h1>")
				assert.NotContains(t, string(body), "Authorization received")
				assert.NotContains(t, string(body), "sensitive-detail")
				return response.Body.Close()
			}}
			credential, err := flow.Login(ctx, "global")
			require.Error(t, err)
			assert.Nil(t, credential)
			assert.NotContains(t, err.Error(), "sensitive-detail")
		})
	}
}

func TestExchangeRejectsRedirectAndRedactsResponse(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusFound, http.StatusBadRequest, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://example.com/stolen")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "secret-code-verifier")
			}))
			defer server.Close()
			_, err := exchangeCode(t.Context(), server.URL, "http://127.0.0.1/callback", "verifier", "code")
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret-code-verifier")
		})
	}
}

func TestOAuthOriginValidation(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"http://example.com/api/v1", "https://user:password@example.com/api/v1", "https://example.com/api/v1?secret=1", "not-a-url"} {
		_, err := oauthOrigin(input)
		require.Error(t, err)
	}
	origin, err := oauthOrigin(DefaultAPIURL)
	require.NoError(t, err)
	assert.Equal(t, strings.TrimSuffix(DefaultAPIURL, "/api/v1"), origin)
}
