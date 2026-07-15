// Package ingestruri resolves connection names into ingestr URIs.
//
// It lives outside pkg/ingestr because pkg/ingestr imports pkg/python, and
// pkg/python needs this resolution too.
package ingestruri

import (
	"context"
	"net/url"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
)

// Connection is implemented by every connection type that ingestr can talk to.
type Connection interface {
	GetIngestrURI() (string, error)
}

// ForConnection resolves name through conn and returns its ingestr URI.
//
// role describes the connection's part in the transfer ("source",
// "destination") and is only used to build error messages. It may be empty.
//
// The returned URI is not normalized; call Normalize when the URI is handed to
// ingestr as a source.
func ForConnection(ctx context.Context, conn config.ConnectionGetter, role, name string) (string, error) {
	prefix := ""
	if role = strings.TrimSpace(role); role != "" {
		prefix = role + " "
	}

	resolved, err := resolve(conn, name)
	if err != nil {
		return "", errors.Wrapf(err, "failed to resolve %sconnection '%s'", prefix, name)
	}

	if resolved == nil {
		return "", config.NewConnectionNotFoundError(ctx, role, name)
	}

	ingestrConn, ok := resolved.(Connection)
	if !ok {
		return "", errors.Errorf("%sconnection '%s' is not supported by ingestr", prefix, name)
	}

	uri, err := ingestrConn.GetIngestrURI()
	if err != nil {
		return "", errors.Wrapf(err, "could not get the %suri", prefix)
	}

	if uri == "" {
		return "", errors.Errorf("%suri is empty, which means the %sconnection '%s' is not configured correctly", prefix, prefix, name)
	}

	return uri, nil
}

// resolve prefers ConnectionResolver so that a secrets backend can report why a
// lookup failed. Plain ConnectionGetter implementations collapse every failure
// into a nil connection.
func resolve(conn config.ConnectionGetter, name string) (any, error) {
	if resolver, ok := conn.(config.ConnectionResolver); ok {
		return resolver.ResolveConnection(name)
	}

	return conn.GetConnection(name), nil
}

// Normalize ensures the URI carries the "//" authority separator after its
// scheme. Some source configs build URIs without a Host, which makes Go's
// url.URL.String() emit "scheme:?params" rather than "scheme://?params".
func Normalize(uri string) string {
	parts := strings.SplitN(uri, ":", 2)
	if len(parts) != 2 || strings.HasPrefix(parts[1], "//") {
		return uri
	}

	return parts[0] + "://" + parts[1]
}

// Parse parses an ingestr URI.
//
// url.Parse rejects schemes containing an underscore (PlanetScale's ps_mysql),
// so the scheme is split off by hand, the remainder parsed under a placeholder,
// and the real scheme restored. This mirrors how ingestr parses its own
// MySQL-family URIs.
func Parse(uri string) (*url.URL, error) {
	scheme, rest, found := strings.Cut(uri, "://")
	if !found {
		return nil, errors.Errorf("uri %q has no scheme", uri)
	}

	parsed, err := url.Parse("placeholder://" + rest)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse uri %q", uri)
	}
	parsed.Scheme = scheme

	return parsed, nil
}

// CDCScheme maps scheme onto ingestr's change-data-capture scheme and reports
// whether the scheme supports CDC at all.
//
// This is not a plain "+cdc" suffix: PostgreSQL's ingestr URI uses the
// "postgresql" scheme but its CDC scheme is "postgres+cdc".
//
// Supported today: PostgreSQL, the MySQL family (mysql, mariadb), Vitess,
// PlanetScale (ps_mysql), MongoDB (mongodb, mongodb+srv), and SQL Server (mssql).
//
// MongoDB and SQL Server both follow the plain "+cdc" suffix rule, which yields
// exactly what ingestr expects: "mongodb+cdc", "mongodb+srv+cdc", "mssql+cdc".
// SQL Server Change Tracking ("mssql+ct") is not selected here because it depends
// on an asset parameter rather than the scheme alone; the ingestr operator swaps
// the suffix for that mode.
func CDCScheme(scheme string) (string, bool) {
	switch {
	case strings.HasSuffix(scheme, "+cdc"):
		return scheme, true
	case strings.Contains(scheme, "postgresql"):
		return strings.ReplaceAll(scheme, "postgresql", "postgres+cdc"), true
	case strings.HasPrefix(scheme, "mysql"), strings.HasPrefix(scheme, "mariadb"),
		strings.HasPrefix(scheme, "vitess"), strings.HasPrefix(scheme, "ps_mysql"),
		strings.HasPrefix(scheme, "mongodb"),
		strings.HasPrefix(scheme, "mssql"), strings.HasPrefix(scheme, "sqlserver"):
		return scheme + "+cdc", true
	}

	return scheme, false
}

// CDC rewrites uri onto its change-data-capture scheme, and fails when the
// scheme has no CDC counterpart.
func CDC(uri string) (string, error) {
	parsed, err := Parse(uri)
	if err != nil {
		return "", errors.Wrap(err, "cannot enable cdc")
	}

	scheme, ok := CDCScheme(parsed.Scheme)
	if !ok {
		return "", errors.Errorf("scheme '%s' does not support change data capture", parsed.Scheme)
	}
	parsed.Scheme = scheme

	return parsed.String(), nil
}
