package chess

import "strings"

type Config struct {
	Players []string
}

func (c *Config) GetIngestrURI() string {
	return "chess://?players=" + strings.Join(c.Players, ",")
}
