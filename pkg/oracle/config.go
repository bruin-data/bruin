package oracle

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	go_ora "github.com/sijms/go-ora/v2"
)

type Config struct {
	Username     string
	Password     string
	Host         string
	Port         string
	ServiceName  string
	SID          string
	Role         string
	SSL          bool
	SSLVerify    bool
	PrefetchRows int
	TraceFile    string
	Wallet       string
}

func (c *Config) DSN() (string, error) {
	port := c.Port
	if port == "" {
		port = "1521"
	}

	portInt := 1521
	if p, err := strconv.Atoi(port); err == nil {
		portInt = p
	}

	options := make(map[string]string)

	if c.SSL {
		options["SSL"] = "enable"
		if !c.SSLVerify {
			options["SSL Verify"] = "false"
		}
		if c.Wallet != "" {
			options["WALLET"] = c.Wallet
		}
	}

	if c.Role != "" {
		options["DBA Privilege"] = c.Role
	}

	if c.PrefetchRows > 0 {
		options["PREFETCH_ROWS"] = strconv.Itoa(c.PrefetchRows)
	}

	if c.TraceFile != "" {
		options["TRACE FILE"] = c.TraceFile
	}
	var dsn string
	switch {
	case c.ServiceName != "":
		dsn = go_ora.BuildUrl(c.Host, portInt, c.ServiceName, c.Username, c.Password, options)
	case c.SID != "":
		sidOptions := make(map[string]string)
		for k, v := range options {
			sidOptions[k] = v
		}
		sidOptions["SID"] = c.SID
		dsn = go_ora.BuildUrl(c.Host, portInt, "", c.Username, c.Password, sidOptions)
	default:
		return "", errors.New("either ServiceName or SID must be specified")
	}

	return dsn, nil
}

func (c *Config) GetIngestrURI() (string, error) {
	port := c.Port
	if port == "" {
		port = "1521"
	}

	// For Ingestr URI, we'll use the service name if available, otherwise SID
	serviceOrSID := c.ServiceName
	if serviceOrSID == "" {
		serviceOrSID = c.SID
	}

	if serviceOrSID == "" {
		return "", errors.New("either ServiceName or SID must be specified")
	}

	// Ingestr expects oracle+cx_oracle scheme
	uri := fmt.Sprintf("oracle+cx_oracle://%s:%s@%s:%s/%s",
		url.QueryEscape(c.Username),
		url.QueryEscape(c.Password),
		c.Host,
		port,
		serviceOrSID,
	)
	return uri, nil
}
