package ingestr

import (
	"net/url"
	"os"
	"strings"
)

// sensitiveQueryParams is the set of URI query-parameter names whose values
// must be redacted from ingestr debug output during cloud runs. Names are
// matched case-insensitively, after normalising underscores and hyphens.
var sensitiveQueryParams = map[string]struct{}{
	"apikey":               {},
	"accesskey":            {},
	"secretkey":            {},
	"clientsecret":         {},
	"password":             {},
	"passwd":               {},
	"pwd":                  {},
	"secret":               {},
	"token":                {},
	"accesstoken":          {},
	"refreshtoken":         {},
	"idtoken":              {},
	"bearer":               {},
	"auth":                 {},
	"authorization":        {},
	"signature":            {},
	"sig":                  {},
	"xamzsignature":        {},
	"xgoogsignature":       {},
	"privatekey":           {},
	"credentialsbase64":    {},
	"credentialsjson":      {},
	"awsaccesskeyid":       {},
	"awssecretaccesskey":   {},
}

// isCloudRun returns true when bruin is running under the orchestrator
// (BRUIN_RUN_ID is injected per cloud invocation). Local users never set
// this, so local --debug runs stay unredacted.
func isCloudRun() bool {
	return os.Getenv("BRUIN_RUN_ID") != ""
}

// normaliseParamName strips characters that vary between equivalent param
// names ("api_key", "api-key", "ApiKey" → "apikey").
func normaliseParamName(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, "-", "")
	return s
}

// collectURISecrets extracts the values that should be redacted in debug
// output produced by a process given this URI. Currently: the password
// component of userinfo, plus the values of known sensitive query
// parameters. Unparseable URIs return nil.
func collectURISecrets(uri string) []string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil
	}

	var secrets []string
	if parsed.User != nil {
		if pwd, ok := parsed.User.Password(); ok && pwd != "" {
			secrets = append(secrets, pwd)
		}
	}
	for name, values := range parsed.Query() {
		if _, sensitive := sensitiveQueryParams[normaliseParamName(name)]; !sensitive {
			continue
		}
		for _, v := range values {
			if v != "" {
				secrets = append(secrets, v)
			}
		}
	}
	return secrets
}

// appendDebugMaskFlags appends one "--debug-mask <value>" pair per secret
// extracted from the provided URIs. Caller is responsible for gating the
// call on cloud-mode + debug-mode.
func appendDebugMaskFlags(args []string, uris ...string) []string {
	seen := make(map[string]struct{})
	for _, uri := range uris {
		for _, secret := range collectURISecrets(uri) {
			if _, dup := seen[secret]; dup {
				continue
			}
			seen[secret] = struct{}{}
			args = append(args, "--debug-mask", secret)
		}
	}
	return args
}
