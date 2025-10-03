package plusvibeai

type Config struct {
	APIKey      string
	WorkspaceID string
}

func (c *Config) GetIngestrURI() string {
	return "plusvibeai://?api_key=" + c.APIKey + "&workspace_id=" + c.WorkspaceID
}
