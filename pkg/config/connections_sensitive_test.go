package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/mask"
	"github.com/stretchr/testify/assert"
)

// TestIcebergConnectionSecretsAreMasked guards that catalog credentials carried
// on dedicated fields are collected by the masker. The free-form Properties map
// is deliberately NOT a place for secrets: map values bypass the masker.
func TestIcebergConnectionSecretsAreMasked(t *testing.T) {
	t.Parallel()
	conn := &IcebergConnection{
		Catalog: IcebergCatalog{
			Type:       IcebergCatalogREST,
			Host:       "catalog.internal",
			Credential: "rest-client:rest-secret",
			Token:      "bearer-token",
			URI:        "postgresql://u:catalog-pass@h:5432/db",
			Auth:       IcebergAuth{AccessKey: "AKID", SecretKey: "catalog-secret-key"},
		},
		Storage: IcebergStorage{
			Type: IcebergStorageS3,
			Path: "s3://lake/wh",
			Auth: IcebergAuth{SecretKey: "storage-secret-key"},
		},
	}

	values := mask.InlineSensitiveValues(conn)
	for _, want := range []string{
		"rest-client:rest-secret", "bearer-token",
		"postgresql://u:catalog-pass@h:5432/db",
		"catalog-secret-key", "storage-secret-key",
	} {
		assert.Contains(t, values, want)
	}
}

// secretNamePattern flags credential-looking field names. A match MUST carry
// `sensitive:"true"` or be listed in safeNonSecretKeys, else the test fails.
var secretNamePattern = regexp.MustCompile(`(?i)(password|secret|token|api[_]?key|access[_]?key|private[_]?key|account[_]?key|credential|passphrase|auth|grant_key|key_base64|service_account_json|^dsn$|^key$|^uri$|^value$)`)

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
	connsType := reflect.TypeOf(Connections{})
	for i := range connsType.NumField() {
		field := connsType.Field(i)
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		t.Run(connectionFieldName(field), func(t *testing.T) {
			t.Parallel()
			checkSensitiveTags(t, unwrapToStruct(field.Type), map[reflect.Type]bool{})
		})
	}
}

// TestConnectionSensitiveValuesAreCollected exercises every registered connection
// independently. Each tagged inline value and credential-file content must reach
// the masker; this catches collector regressions in nested connection structs.
func TestConnectionSensitiveValuesAreCollected(t *testing.T) {
	t.Parallel()
	connsType := reflect.TypeOf(Connections{})
	for i := range connsType.NumField() {
		field := connsType.Field(i)
		if field.Type.Kind() != reflect.Slice {
			continue
		}
		t.Run(connectionFieldName(field), func(t *testing.T) {
			t.Parallel()
			conn := reflect.New(unwrapToStruct(field.Type))
			expected := make([]string, 0)
			setSensitiveTestValues(t, conn.Elem(), conn.Elem().Type().Name(), &expected)

			actual, unreadable := mask.SensitiveValues(conn.Interface())
			assert.Empty(t, unreadable)
			assert.ElementsMatch(t, expected, actual)
		})
	}
}

func connectionFieldName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
	if name != "" {
		return name
	}
	return field.Name
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
		for _, tag := range []string{"sensitive", "sensitive_file"} {
			if value := f.Tag.Get(tag); value != "" && value != "true" {
				t.Errorf("%s.%s has invalid `%s:%q`; the only supported value is true", st.Name(), f.Name, tag, value)
			}
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

func setSensitiveTestValues(t *testing.T, value reflect.Value, path string, expected *[]string) {
	t.Helper()
	typeOfValue := value.Type()
	for i := range typeOfValue.NumField() {
		field := typeOfValue.Field(i)
		if !field.IsExported() {
			continue
		}
		fieldValue := value.Field(i)
		fieldPath := path + "." + field.Name
		if fieldValue.Kind() == reflect.String {
			isInline := field.Tag.Get("sensitive") == "true"
			isFile := field.Tag.Get("sensitive_file") == "true"
			if isInline && isFile {
				t.Errorf("%s cannot be both sensitive and sensitive_file", fieldPath)
				continue
			}
			switch {
			case isInline:
				secret := "inline-secret::" + fieldPath
				fieldValue.SetString(secret)
				*expected = append(*expected, secret)
			case isFile:
				secret := "file-secret::" + fieldPath
				filePath := filepath.Join(t.TempDir(), fmt.Sprintf("credential-%d", i))
				if err := os.WriteFile(filePath, []byte(secret), 0o600); err != nil {
					t.Fatalf("write credential fixture for %s: %v", fieldPath, err)
				}
				fieldValue.SetString(filePath)
				*expected = append(*expected, secret)
			}
			continue
		}

		switch fieldValue.Kind() { //nolint:exhaustive // only nested config structs can contain sensitive tags
		case reflect.Struct:
			if fieldValue.Type().PkgPath() == typeOfValue.PkgPath() {
				setSensitiveTestValues(t, fieldValue, fieldPath, expected)
			}
		case reflect.Pointer:
			if fieldValue.Type().Elem().Kind() == reflect.Struct && fieldValue.Type().Elem().PkgPath() == typeOfValue.PkgPath() {
				fieldValue.Set(reflect.New(fieldValue.Type().Elem()))
				setSensitiveTestValues(t, fieldValue.Elem(), fieldPath, expected)
			}
		}
	}
}
