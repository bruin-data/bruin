package cloudauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const ClientID = "bruin-cli"

var callbackPage = template.Must(template.New("oauth-callback").Parse(`<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{.Title}} · Bruin CLI</title>
    <style>
        * { box-sizing: border-box; }
        body { margin: 0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; padding-top: 1.5rem; background: #f3f4f6; color: #111827; font-family: "IBM Plex Sans", ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; -webkit-font-smoothing: antialiased; }
        .logo { display: block; width: 4rem; height: 4rem; }
        main { width: 100%; margin-top: 1.5rem; padding: 1rem 1.5rem; overflow: hidden; background: #fff; box-shadow: 0 4px 6px -1px rgb(0 0 0 / .1), 0 2px 4px -2px rgb(0 0 0 / .1); }
        h1 { margin: 0; font-size: 1.25rem; line-height: 1.75rem; font-weight: 600; color: #111827; }
        p { margin: 0.5rem 0 0; font-size: 0.875rem; line-height: 1.25rem; color: #4b5563; }
        .hint { margin-top: 1rem; color: #6b7280; }
        @media (min-width: 640px) {
            body { justify-content: center; padding-top: 0; }
            main { max-width: 28rem; border-radius: 0.5rem; }
        }
    </style>
</head>
<body>
    <svg class="logo" viewBox="0 0 23 18" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
        <path d="M20.0177 2.70367C19.9697 2.5717 20.0037 2.42371 20.1077 2.32971L21.9654 0.603928C22.1974 0.387955 22.0454 0 21.7294 0H0.348107C0.0321458 0 -0.119837 0.387955 0.112134 0.603928L1.96791 2.32772C2.0719 2.42371 2.10588 2.5717 2.0559 2.70367L0.0341441 6.90117C-0.0318478 7.03716 0.000149277 7.19914 0.110136 7.30112L10.7988 17.2319C10.9268 17.3499 11.1228 17.3499 11.2508 17.2319L21.9554 7.28112C22.0715 7.17314 22.1054 6.99915 22.0354 6.85717L20.0197 2.70568L20.0177 2.70367ZM6.55134 4.25548L5.52146 3.05563C5.32949 2.83166 5.48947 2.4877 5.78344 2.4877H7.83118C8.12514 2.4877 8.28313 2.83166 8.09315 3.05563L7.06327 4.25548C6.92929 4.41147 6.68732 4.41147 6.55134 4.25548ZM14.8863 10.7547H11.8967C11.5847 10.7547 11.4347 10.3727 11.6627 10.1608L12.5566 9.33088H12.5526L12.7246 9.1709C12.9546 8.95692 12.8026 8.57297 12.4906 8.57297H9.55297C9.23901 8.57297 9.08902 8.95692 9.319 9.1709L9.49298 9.33288H9.48897L10.3789 10.1568C10.6088 10.3708 10.4589 10.7567 10.1429 10.7567H7.15927C6.89529 10.7567 6.72531 10.4747 6.8513 10.2408L10.7148 3.05963C10.8468 2.81367 11.1988 2.81367 11.3307 3.05963L15.1962 10.2408C15.3223 10.4747 15.1523 10.7567 14.8883 10.7567L14.8863 10.7547ZM16.5221 3.05563L15.4923 4.25548C15.3583 4.41147 15.1162 4.41147 14.9803 4.25548L13.9504 3.05563C13.7585 2.83166 13.9184 2.4877 14.2123 2.4877H16.2601C16.5541 2.4877 16.712 2.83166 16.5221 3.05563Z" fill="#FF5569"/>
    </svg>
    <main>
        <h1>{{.Title}}</h1>
        <p>{{.Message}}</p>
        <p class="hint">You can close this tab.</p>
    </main>
</body>
</html>`))

func writeCallbackPage(w http.ResponseWriter, status int, title, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'")
	w.WriteHeader(status)
	_ = callbackPage.Execute(w, struct{ Title, Message string }{Title: title, Message: message})
}

type OAuth struct {
	APIURL      string
	OpenBrowser func(context.Context, string) error
	Writer      io.Writer
	NoBrowser   bool
}

func (o OAuth) Login(ctx context.Context, target string) (*Credential, error) {
	issuer, err := oauthOrigin(o.APIURL)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("cannot start local OAuth callback listener")
	}
	defer func() { _ = listener.Close() }()
	redirect := "http://" + listener.Addr().String() + "/callback"
	state, err := randomSecret()
	if err != nil {
		return nil, err
	}
	verifier, err := randomSecret()
	if err != nil {
		return nil, err
	}
	challenge := sha256.Sum256([]byte(verifier))
	query := url.Values{
		"client_id": {ClientID}, "response_type": {"code"}, "redirect_uri": {redirect},
		"state": {state}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])},
		"code_challenge_method": {"S256"}, "storage": {target},
	}
	authorizeURL := issuer + "/cli/oauth/authorize?" + query.Encode()
	type result struct {
		code string
		err  error
	}
	callback := make(chan result, 1)
	var once sync.Once
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if r.Method != http.MethodGet || r.Host != listener.Addr().String() {
			writeCallbackPage(w, http.StatusBadRequest, "Invalid authorization request", "Return to your terminal and try signing in again.")
			return
		}
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil || len(values["state"]) != 1 || subtle.ConstantTimeCompare([]byte(values.Get("state")), []byte(state)) != 1 {
			writeCallbackPage(w, http.StatusBadRequest, "Authorization could not be verified", "Return to your terminal and try signing in again.")
			return
		}
		response := result{}
		if values.Get("error") != "" {
			response.err = errors.New("OAuth authorization was denied or failed")
		} else if len(values["code"]) != 1 || values.Get("code") == "" {
			writeCallbackPage(w, http.StatusBadRequest, "Authorization incomplete", "Return to your terminal and try signing in again.")
			return
		} else {
			response.code = values.Get("code")
		}
		accepted := false
		once.Do(func() { accepted = true })
		if !accepted {
			writeCallbackPage(w, http.StatusConflict, "Authorization already received", "Return to your terminal to check your sign-in.")
			return
		}
		if response.err != nil {
			writeCallbackPage(w, http.StatusOK, "Authorization not completed", "Return to your terminal to try again.")
		} else {
			writeCallbackPage(w, http.StatusOK, "Authorization received", "Return to your terminal to finish signing in.")
		}
		_ = http.NewResponseController(w).Flush()
		callback <- response
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer func() { _ = server.Close() }()
	go func() { _ = server.Serve(listener) }()
	_, _ = fmt.Fprintf(o.Writer, "Open this URL to authorize Bruin Cloud:\n%s\n", authorizeURL)
	if !o.NoBrowser && o.OpenBrowser != nil {
		if err := o.OpenBrowser(ctx, authorizeURL); err != nil {
			_, _ = fmt.Fprintln(o.Writer, "Could not open the browser automatically. Open the URL above on this computer.")
		}
	}
	select {
	case <-ctx.Done():
		return nil, errors.New("OAuth login cancelled or timed out; existing credentials were preserved")
	case response := <-callback:
		if response.err != nil {
			return nil, response.err
		}
		credential, err := exchangeCode(ctx, issuer, redirect, verifier, response.code)
		if err != nil {
			return nil, err
		}
		credential.APIURL = o.APIURL
		return credential, nil
	}
}

func oauthOrigin(apiURL string) (string, error) {
	endpoint, err := url.Parse(apiURL)
	if err != nil || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", errors.New("invalid Bruin Cloud API URL")
	}
	loopback := endpoint.Hostname() == "127.0.0.1" || endpoint.Hostname() == "localhost" || endpoint.Hostname() == "::1"
	if endpoint.Scheme != "https" && !(endpoint.Scheme == "http" && loopback) {
		return "", errors.New("OAuth requires HTTPS except for a local development server")
	}
	return endpoint.Scheme + "://" + endpoint.Host, nil
}

func randomSecret() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func exchangeCode(ctx context.Context, issuer, redirect, verifier, code string) (*Credential, error) {
	form := url.Values{"grant_type": {"authorization_code"}, "client_id": {ClientID}, "redirect_uri": {redirect}, "code_verifier": {verifier}, "code": {code}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, issuer+"/cli/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, errors.New("could not create token exchange request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("OAuth token exchange failed; retry login")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OAuth token exchange failed (HTTP %d); Cloud must support the CLI personal-token flow", resp.StatusCode)
	}
	var credential Credential
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&credential); err != nil {
		return nil, errors.New("invalid OAuth token response")
	}
	if credential.Token == "" || credential.TokenID == "" || !strings.EqualFold(credential.TokenType, "Bearer") {
		return nil, errors.New("incomplete OAuth personal-token response")
	}
	if !credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt) {
		return nil, errors.New("OAuth returned an expired token")
	}
	return &credential, nil
}

func Revoke(ctx context.Context, credential *Credential) error {
	issuer, err := oauthOrigin(credential.APIURL)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, issuer+"/cli/oauth/revoke", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+credential.Token)
	client := &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("could not revoke the new personal token")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token revocation failed (HTTP %d)", resp.StatusCode)
	}
	return nil
}
