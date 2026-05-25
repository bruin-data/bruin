package duck

import (
	"net/url"
	"strconv"

	"github.com/bruin-data/bruin/pkg/config"
)

// Serialises a LakehouseConfig into a URI that
// ingestr's matching destination parses on the other side.
func BuildIngestrLakehouseURI(lh *config.LakehouseConfig) string {
	if lh.IsZero() {
		return ""
	}
	if lh.Format != config.LakehouseFormatDuckLake {
		return ""
	}

	q := url.Values{}

	cat := lh.Catalog
	switch cat.Type {
	case config.CatalogTypeDuckDB, config.CatalogTypeSQLite:
		if cat.Path != "" {
			q.Set("catalog_path", cat.Path)
		}
	case config.CatalogTypePostgres:
		if cat.Host != "" {
			q.Set("catalog_host", cat.Host)
		}
		if cat.Port != 0 {
			q.Set("catalog_port", strconv.Itoa(cat.Port))
		}
		if cat.Database != "" {
			q.Set("catalog_database", cat.Database)
		}
		if cat.Auth.Username != "" {
			q.Set("catalog_username", cat.Auth.Username)
		}
		if cat.Auth.Password != "" {
			q.Set("catalog_password", cat.Auth.Password)
		}
	default:
		return ""
	}
	q.Set("catalog_type", string(cat.Type))

	st := lh.Storage
	q.Set("storage_type", string(st.Type))
	if st.Path != "" {
		q.Set("storage_path", st.Path)
	}
	if st.Region != "" {
		q.Set("storage_region", st.Region)
	}
	if st.Endpoint != "" {
		q.Set("storage_endpoint", st.Endpoint)
	}
	if st.URLStyle != "" {
		q.Set("storage_url_style", st.URLStyle)
	}
	if st.UseSSL != nil {
		q.Set("storage_use_ssl", strconv.FormatBool(*st.UseSSL))
	}
	if st.Auth.AccessKey != "" {
		q.Set("storage_access_key", st.Auth.AccessKey)
	}
	if st.Auth.SecretKey != "" {
		q.Set("storage_secret_key", st.Auth.SecretKey)
	}
	if st.Auth.SessionToken != "" {
		q.Set("storage_session_token", st.Auth.SessionToken)
	}

	return "ducklake://?" + q.Encode()
}
