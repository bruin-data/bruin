package config

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// secretNamePattern flags credential-looking field names. A match MUST carry
// `sensitive:"true"` or be listed in safeNonSecretKeys, else the test fails.
var secretNamePattern = regexp.MustCompile(`(?i)(password|secret|token|api[_]?key|access[_]?key|private[_]?key|account[_]?key|credential|passphrase|auth|grant_key|key_base64|service_account_json|^key$)`)

// safeNonSecretKeys are fields whose names look secret-ish but hold
// identifiers, file paths, or booleans — not credential values.
var safeNonSecretKeys = map[string]bool{
	"service_account_file":                true,
	"private_key_path":                    true,
	"key_path":                            true,
	"key_id":                              true,
	"ssl_cert_path":                       true,
	"verify_certs":                        true,
	"use_application_default_credentials": true,
	"use_azure_default_credential":        true,
	"client_id":                           true,
	"token_id":                            true, // Polymarket market id, not auth
	"clob_token_ids":                      true, // Polymarket market ids, not auth
	"personal_access_token_name":          true, // identifier, paired with *_secret
}

func TestConnectionSensitiveTagsComplete(t *testing.T) {
	t.Parallel()
	visited := map[reflect.Type]bool{}
	connsType := reflect.TypeOf(Connections{})
	for i := range connsType.NumField() {
		checkSensitiveTags(t, unwrapToStruct(connsType.Field(i).Type), visited)
	}
}

// unwrapToStruct peels slice/array/pointer/map layers to reach the element type.
func unwrapToStruct(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Pointer || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	return t
}

// checkSensitiveTags recurses through a connection struct (like mask.SensitiveValues),
// failing on any credential-looking string field not tagged `sensitive:"true"`.
func checkSensitiveTags(t *testing.T, st reflect.Type, visited map[reflect.Type]bool) {
	t.Helper()
	if st.Kind() != reflect.Struct || visited[st] {
		return
	}
	visited[st] = true

	for j := range st.NumField() {
		f := st.Field(j)
		if !f.IsExported() {
			continue
		}
		if inner := unwrapToStruct(f.Type); inner.Kind() == reflect.Struct {
			checkSensitiveTags(t, inner, visited)
			continue
		}
		if f.Type.Kind() != reflect.String {
			continue
		}
		key := f.Tag.Get("mapstructure")
		if key == "" {
			key = strings.ToLower(f.Name)
		}
		if safeNonSecretKeys[key] {
			continue
		}
		if !secretNamePattern.MatchString(key) && !secretNamePattern.MatchString(f.Name) {
			continue
		}
		if f.Tag.Get("sensitive") != "true" {
			t.Errorf("%s.%s (key=%q) looks like a credential but is not tagged `sensitive:\"true\"`.\n"+
				"Add the tag, or if it is NOT a secret add %q to safeNonSecretKeys.",
				st.Name(), f.Name, key, key)
		}
	}
}
