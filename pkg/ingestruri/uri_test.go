package ingestruri

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConn implements Connection.
type fakeConn struct {
	uri string
	err error
}

func (f fakeConn) GetIngestrURI() (string, error) { return f.uri, f.err }

// unsupportedConn deliberately does not implement Connection, mirroring
// config.GenericConnection.
type unsupportedConn struct{}

// getter implements config.ConnectionGetter only.
type getter struct {
	conns map[string]any
}

func (g getter) GetConnection(name string) any { return g.conns[name] }

// resolver implements config.ConnectionResolver as the secrets backends do.
type resolver struct {
	getter
	err error
}

func (r resolver) ResolveConnection(name string) (any, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.conns[name], nil
}

func TestForConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		conn     config.ConnectionGetter
		role     string
		connName string
		want     string
		wantErr  string
	}{
		{
			name:     "returns the uri",
			conn:     getter{conns: map[string]any{"pg": fakeConn{uri: "postgresql://u:p@h:5432/db"}}},
			role:     "source",
			connName: "pg",
			want:     "postgresql://u:p@h:5432/db",
		},
		{
			name:     "missing connection reports the role",
			conn:     getter{conns: map[string]any{}},
			role:     "destination",
			connName: "nope",
			wantErr:  "destination connection 'nope' not found",
		},
		{
			name:     "missing connection without a role",
			conn:     getter{conns: map[string]any{}},
			connName: "nope",
			wantErr:  "connection 'nope' not found",
		},
		{
			name:     "connection that cannot speak ingestr errors rather than panicking",
			conn:     getter{conns: map[string]any{"gen": unsupportedConn{}}},
			role:     "source",
			connName: "gen",
			wantErr:  "source connection 'gen' is not supported by ingestr",
		},
		{
			name:     "empty uri is rejected",
			conn:     getter{conns: map[string]any{"pg": fakeConn{uri: ""}}},
			role:     "source",
			connName: "pg",
			wantErr:  "source uri is empty",
		},
		{
			name:     "GetIngestrURI failure is wrapped",
			conn:     getter{conns: map[string]any{"pg": fakeConn{err: errors.New("boom")}}},
			role:     "source",
			connName: "pg",
			wantErr:  "could not get the source uri: boom",
		},
		{
			name: "resolver surfaces the backend error instead of not-found",
			conn: resolver{
				getter: getter{conns: map[string]any{}},
				err:    errors.New("vault: permission denied"),
			},
			role:     "source",
			connName: "pg",
			wantErr:  "failed to resolve source connection 'pg': vault: permission denied",
		},
		{
			name: "resolver returns the uri when it succeeds",
			conn: resolver{
				getter: getter{conns: map[string]any{"pg": fakeConn{uri: "postgresql://x"}}},
			},
			role:     "source",
			connName: "pg",
			want:     "postgresql://x",
		},
		{
			name:     "resolver with no error and no connection is a not-found",
			conn:     resolver{getter: getter{conns: map[string]any{}}},
			role:     "source",
			connName: "pg",
			wantErr:  "source connection 'pg' not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ForConnection(context.Background(), tt.conn, tt.role, tt.connName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestForConnection_NotFoundMentionsSecretsBackend(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), config.SecretsBackendContextKey, "vault")

	_, err := ForConnection(ctx, getter{conns: map[string]any{}}, "source", "pg")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in secrets backend 'vault'")
}

func TestCDCScheme(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scheme string
		want   string
		wantOK bool
	}{
		// postgres is a rename, not a suffix: the ingestr URI scheme is
		// "postgresql" but its CDC scheme is "postgres+cdc".
		{scheme: "postgresql", want: "postgres+cdc", wantOK: true},
		{scheme: "mysql", want: "mysql+cdc", wantOK: true},
		{scheme: "mysql+pymysql", want: "mysql+pymysql+cdc", wantOK: true},
		{scheme: "mariadb", want: "mariadb+cdc", wantOK: true},
		{scheme: "vitess", want: "vitess+cdc", wantOK: true},
		{scheme: "ps_mysql", want: "ps_mysql+cdc", wantOK: true},

		// already a cdc scheme
		{scheme: "postgres+cdc", want: "postgres+cdc", wantOK: true},
		{scheme: "mysql+cdc", want: "mysql+cdc", wantOK: true},

		// no cdc counterpart
		{scheme: "bigquery", want: "bigquery", wantOK: false},
		{scheme: "duckdb", want: "duckdb", wantOK: false},
		{scheme: "snowflake", want: "snowflake", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			t.Parallel()

			got, ok := CDCScheme(tt.scheme)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestCDCScheme_Idempotent(t *testing.T) {
	t.Parallel()

	for _, scheme := range []string{"postgresql", "mysql", "mariadb", "vitess", "ps_mysql"} {
		once, ok := CDCScheme(scheme)
		require.True(t, ok)

		twice, ok := CDCScheme(once)
		require.True(t, ok)
		assert.Equal(t, once, twice, "applying the cdc scheme twice must not double the suffix")
	}
}

func TestCDC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		uri     string
		want    string
		wantErr string
	}{
		{
			name: "postgres",
			uri:  "postgresql://u:p@h:5432/db",
			want: "postgres+cdc://u:p@h:5432/db",
		},
		{
			name: "preserves the query string",
			uri:  "mysql://u:p@h:3306/db?ssl=true",
			want: "mysql+cdc://u:p@h:3306/db?ssl=true",
		},
		{
			// url.Parse rejects an underscore in the scheme, hence the manual split.
			name: "planetscale scheme with an underscore",
			uri:  "ps_mysql://u:p@aws.connect.psdb.cloud:3306/db",
			want: "ps_mysql+cdc://u:p@aws.connect.psdb.cloud:3306/db",
		},
		{
			name: "vitess keeps its grpc_port",
			uri:  "vitess://u:p@vtgate.internal:15306/commerce?grpc_port=15991",
			want: "vitess+cdc://u:p@vtgate.internal:15306/commerce?grpc_port=15991",
		},
		{
			name:    "unsupported scheme is an error rather than a silent passthrough",
			uri:     "bigquery://project/dataset",
			wantErr: "scheme 'bigquery' does not support change data capture",
		},
		{
			name:    "missing scheme",
			uri:     "not-a-uri",
			wantErr: "has no scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CDC(tt.uri)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "adds the authority separator", uri: "gsheets:?key=value", want: "gsheets://?key=value"},
		{name: "leaves a well formed uri alone", uri: "postgresql://u:p@h:5432/db", want: "postgresql://u:p@h:5432/db"},
		{name: "leaves extra slashes alone", uri: "duckdb:////some/path", want: "duckdb:////some/path"},
		{name: "leaves a schemeless string alone", uri: "not-a-uri", want: "not-a-uri"},
		{name: "leaves an empty string alone", uri: "", want: ""},
		{name: "is idempotent", uri: "gsheets://?key=value", want: "gsheets://?key=value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, Normalize(tt.uri))
			assert.Equal(t, tt.want, Normalize(Normalize(tt.uri)))
		})
	}
}
