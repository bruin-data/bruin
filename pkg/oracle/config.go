package oracle

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"

	go_ora "github.com/sijms/go-ora/v2"
)

type Config struct {
	Username    string
	Password    string
	Host        string
	Port        string
	ServiceName string 
	SID         string 
	Role        string 
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

	// Build DSN based on whether we're using Service Name or SID
	var dsn string
	if c.ServiceName != "" {
		// Use service name connection
		dsn = go_ora.BuildUrl(c.Host, portInt, c.ServiceName, c.Username, c.Password, options)
	} else if c.SID != "" {
		// Use SID-based connection - pass SID in options with empty service name
		sidOptions := make(map[string]string)
		for k, v := range options {
			sidOptions[k] = v
		}
		sidOptions["SID"] = c.SID
		dsn = go_ora.BuildUrl(c.Host, portInt, "", c.Username, c.Password, sidOptions)
	} else {
		return "", errors.New("either ServiceName or SID must be specified")
	}

	return dsn, nil
}

func (c *Config) GetIngestrURI() string {
	port := c.Port
	if port == "" {
		port = "1521"
	}

	// For Ingestr URI, we'll use the service name if available, otherwise SID
	serviceOrSID := c.ServiceName
	if serviceOrSID == "" {
		serviceOrSID = c.SID
	}

	url := url.URL{
		Scheme: "oracle",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   fmt.Sprintf("%s:%s", c.Host, port),
		Path:   serviceOrSID,
	}
	return url.String()
}
