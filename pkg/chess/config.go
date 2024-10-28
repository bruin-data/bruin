package chess

import "strings"

type Config struct {
	Players []string
}

func (c *Config) GetIngestrURI() string {
	print("chess://?players_username=" + strings.Join(c.Players, ","))
	return "chess://?players_username=" + strings.Join(c.Players, ",")
}
